/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.oap.index

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BaseOrdering, GenerateOrdering}
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.index.impl.IndexFileReaderImpl
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.{StatisticsManager, StatsAnalysisResult}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.CompletionIterator

// TODO: Pick out the scanner related code: e.g. findRowIdRange ...
private[index] abstract class BTreeIndexRecordReader(
    configuration: Configuration,
    schema: StructType,
    fileReader: IndexFileReader
) extends Iterator[Int] {

  // abstract members to be implemented
  protected var meta: BTreeMeta
  protected var footer: BTreeFooter

  type CompareFunction = (InternalRow, InternalRow, Boolean) => Int

  // Global variables
  protected var internalIterator: Iterator[Int] = _
  private var initialized = false

  // Currently used FiberCache, will release it finally.
  private val MAX_IN_USE_FIBER_CACHE_NUM = 3
  private val inUseFiberCache = new Array[FiberCache](MAX_IN_USE_FIBER_CACHE_NUM)

  protected def release(sectionId: Int): Unit = synchronized {
    Option(inUseFiberCache(sectionId)).foreach { fiberCache =>
      fiberCache.release()
      inUseFiberCache.update(sectionId, null)
    }
  }

  protected def update(sectionId: Int, fiberCache: FiberCache): Unit = {
    release(sectionId)
    inUseFiberCache.update(sectionId, fiberCache)
  }

  // Constant variables
  protected val FOOTER_LENGTH_SIZE: Int = IndexUtils.INT_SIZE
  protected val ROW_ID_LIST_LENGTH_SIZE: Int = IndexUtils.LONG_SIZE
  protected val footerSectionId: Int = 0
  protected val rowIdListSectionId: Int = 1
  protected val nodeSectionId: Int = 2

  // Ordering for key searching
  protected lazy val ordering: BaseOrdering = GenerateOrdering.create(schema)
  protected lazy val partialOrdering: BaseOrdering =
    GenerateOrdering.create(StructType(schema.dropRight(1)))

  // Util functions
  protected def getBTreeFiberCache(
      offset: Long, length: Int, sectionId: Int, idx: Int): FiberCache = {

    val readFunc = () => MemoryManager.toIndexFiberCache(readData(offset, length))
    val fiber = BTreeFiber(readFunc, fileReader.getName, sectionId, idx)
    FiberCacheManager.get(fiber, configuration)
  }

  protected def getLongFromBuffer(buffer: Array[Byte], offset: Int): Long =
    Platform.getLong(buffer, Platform.BYTE_ARRAY_OFFSET + offset)

  protected def getIntFromBuffer(buffer: Array[Byte], offset: Int): Int =
    Platform.getInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset)

  def totalRows(): Long =
    (0 until footer.getNodesCount).map(footer.getRowCountOfNode).sum

  // Iterator related functions
  def close(): Unit = {
    fileReader.close()
    inUseFiberCache.indices.foreach(release)
  }

  override def hasNext: Boolean = if (internalIterator.hasNext) {
    true
  } else {
    close()
    false
  }

  override def next(): Int = internalIterator.next()

  // Interfaces need to be implemented
  // Visible to test suite
  protected[index] def rowIdListSizePerSection: Int

  protected[index] def initializeReader(): Unit

  protected def readData(position: Long, length: Int): Array[Byte]

  protected def readBTreeFooter(): BTreeFooter

  protected def readBTreeRowIdList(footer: BTreeFooter, partIdx: Int): BTreeRowIdList

  protected def readBTreeNodeData(footer: BTreeFooter, nodeIdx: Int): BTreeNode

  // Public function
  def analyzeStatistics(
      keySchema: StructType,
      intervalArray: ArrayBuffer[RangeInterval]): StatsAnalysisResult = {

    if (!initialized) {
      initialized = true
      initializeReader()
    }

    val offset = footer.getStatsOffset
    val stats = StatisticsManager.read(footer.fiberCache, offset, keySchema)
    StatisticsManager.analyse(stats, intervalArray, configuration)
  }

  def initialize(path: Path, intervalArray: ArrayBuffer[RangeInterval]): Unit = {

    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => close()))

    if (!initialized) {
      initialized = true
      initializeReader()
    }

    internalIterator = intervalArray.toIterator.flatMap { interval =>
      val (start, end) = findRowIdRange(interval)
      val groupedPos = (start until end).groupBy(i => i / rowIdListSizePerSection)
      groupedPos.toIterator.flatMap {
        case (partIdx, subPosList) =>
          val rowIdList = readBTreeRowIdList(footer, partIdx)
          val iterator =
            subPosList.toIterator.map(i => rowIdList.getRowId(i % rowIdListSizePerSection))
          CompletionIterator[Int, Iterator[Int]](iterator, release(rowIdListSectionId))
      }
    } // get the row ids
  }

  // Visible to Test Suite
  protected[index] def getFileMeta: BTreeMeta = meta

  // Internal scan functions to transfer interval to row id range.
  // find the row id list start pos, end pos of the range interval
  protected[index] def findRowIdRange(interval: RangeInterval): (Int, Int) = {
    val compareFunc: CompareFunction =
      if (interval.isPrefixMatch) rowOrderingPattern else rowOrdering
    val recordCount = footer.getNonNullKeyRecordCount
    if (interval.isNullPredicate) { // process "isNull" predicate
      return (recordCount, recordCount + footer.getNullKeyRecordCount)
    }
    val nodeIdxForStart = findNodeIdx(interval.start, isStart = true, compareFunc)
    val nodeIdxForEnd = findNodeIdx(interval.end, isStart = false, compareFunc)

    if (nodeIdxForStart.isEmpty || nodeIdxForEnd.isEmpty ||
        nodeIdxForEnd.get < nodeIdxForStart.get) {
      (0, 0) // not found in B+ tree
    } else {
      val start =
        if (interval.start == IndexScanner.DUMMY_KEY_START) {
          0
        } else {
          nodeIdxForStart.map { idx =>
            findRowIdPos(idx, interval.start, isStart = true, !interval.startInclude, compareFunc)
          }.getOrElse(recordCount)
        }
      val end =
        if (interval.end == IndexScanner.DUMMY_KEY_END) {
          recordCount
        } else {
          nodeIdxForEnd.map { idx =>
            findRowIdPos(idx, interval.end, isStart = false, interval.endInclude, compareFunc)
          }.getOrElse(recordCount)
        }
      (start, end)
    }
  }

  protected def findRowIdPos(
      nodeIdx: Int,
      candidate: InternalRow,
      isStart: Boolean,
      findNext: Boolean,
      compareFunc: CompareFunction = rowOrdering): Int = {

    val node = readBTreeNodeData(footer, nodeIdx)

    val keyCount = node.getKeyCount

    val (pos, found) = if (isStart) {
      IndexUtils.binarySearchForStart(
        0, keyCount, node.getKey(_, schema), candidate, compareFunc(_, _, isStart))
    } else {
      IndexUtils.binarySearchForEnd(
        0, keyCount, node.getKey(_, schema), candidate, compareFunc(_, _, isStart))
    }

    val keyPos = if (found && findNext) pos + 1 else pos

    val rowPos =
      if (keyPos == keyCount) {
        if (nodeIdx + 1 == footer.getNodesCount) {
          footer.getNonNullKeyRecordCount
        } else {
          val nextNode = readBTreeNodeData(footer, nodeIdx + 1)
          nextNode.getRowIdPos(0)
        }
      } else {
        node.getRowIdPos(keyPos)
      }
    release(nodeSectionId)
    rowPos
  }

  /**
   * Find the Node index contains the candidate. If no, return the first which node.max >= candidate
   * If candidate > all node.max, return None
   *
   * @param isStart to indicate if the candidate is interval.start or interval.end
   * @return Option of Node index and if candidate falls in node (means min <= candidate < max)
   */
  private def findNodeIdx(
      candidate: InternalRow,
      isStart: Boolean,
      compareFunc: CompareFunction = rowOrdering): Option[Int] = {
    if (isStart) {
      (0 until footer.getNodesCount).find { idx =>
        footer.getRowCountOfNode(idx) > 0 && // ensure this node is not an empty node
            compareFunc(candidate, footer.getMaxValue(idx, schema), true) <= 0
      }
    } else {
      (0 until footer.getNodesCount).reverse.find { idx =>
        footer.getRowCountOfNode(idx) > 0 && // ensure this node is not an empty node
            compareFunc(candidate, footer.getMinValue(idx, schema), false) >= 0
      }
    }
  }

  /**
   * Compare auxiliary function.
   * Visible to test suite
   *
   * @param x       , y are the key to be compared.
   *                One comes from interval.start/end. One comes from index records
   * @param isStart indicates to compare interval.start or interval end
   */
  protected[index] def rowOrdering(x: InternalRow, y: InternalRow, isStart: Boolean): Int = {
    if (x.numFields == y.numFields) {
      ordering.compare(x, y)
    } else if (x.numFields < y.numFields) {
      val cmp = partialOrdering.compare(x, y)
      if (cmp == 0) {
        if (isStart) -1 else 1
      } else {
        cmp
      }
    } else {
      -rowOrdering(y, x, isStart) // Keep x.numFields <= y.numFields to simplify
    }
  }

  /**
   * like [[rowOrdering]], but x should always from interval.start or interval.end for pattern,
   * while y should be the other one from index records.
   */
  protected def rowOrderingPattern(x: InternalRow, y: InternalRow, isStart: Boolean): Int = {
    // Note min/max == null has been handled elsewhere
    // For pattern match queries, there is no dummy end and dummy start
    assert(x.numFields == y.numFields)
    if (x.numFields > 1) {
      val partialRes = partialOrdering.compare(x, y)
      if (partialRes != 0) {
        return partialRes
      }
    }
    val xStr = x.getString(schema.length - 1)
    val yStr = y.getString(schema.length - 1)
    if (strMatching(xStr, yStr)) 0 else xStr.compare(yStr)
  }

  protected def strMatching(xStr: String, yStr: String): Boolean = yStr.startsWith(xStr)

  // Base abstract class to represent data format
  protected trait BTreeMeta {
    def fileLength: Long

    def footerOffset: Long

    def footerLength: Int

    def rowIdListOffset: Long

    def rowIdListLength: Long

    def nodeOffset: Long
  }

  protected trait BTreeFooter {
    def fiberCache: FiberCache

    def getStatsOffset: Int

    def getNodesCount: Int

    def getNullKeyRecordCount: Int

    def getNonNullKeyRecordCount: Int

    def getRowCountOfNode(idx: Int): Int

    def getMaxValue(idx: Int, schema: StructType): InternalRow

    def getMinValue(idx: Int, schema: StructType): InternalRow

    def getNodeOffset(idx: Int): Int

    def getNodeSize(idx: Int): Int

    def getRowIdListPartOffset(idx: Int): Int

    def getRowIdListPartSize(idx: Int): Int

    def getRowIdListPartSizePerSection: Int
  }

  protected trait BTreeRowIdList {
    def fiberCache: FiberCache

    def getRowId(idx: Int): Int
  }

  protected trait BTreeNode {
    def fiberCache: FiberCache

    def getKeyCount: Int

    def getKey(idx: Int, schema: StructType): InternalRow

    def getRowIdPos(idx: Int): Int
  }

}

private[index] object BTreeIndexRecordReader {
  def apply(
      configuration: Configuration,
      schema: StructType,
      indexPath: Path): BTreeIndexRecordReader = {

    val fileReader = IndexFileReaderImpl(configuration, indexPath)

    readVersion(fileReader) match {
      case Some(version) =>
        IndexVersion(version) match {
          case IndexVersion.OAP_INDEX_V1 =>
            BTreeIndexRecordReaderV1(configuration, schema, fileReader)
          case IndexVersion.OAP_INDEX_V2 =>
            BTreeIndexRecordReaderV2(configuration, schema, fileReader)
        }
      case None =>
        throw new OapException("not a valid index file")
    }
  }

  private def readVersion(fileReader: IndexFileReader): Option[Int] = {
    val magicBytes = fileReader.read(0, IndexFile.VERSION_LENGTH)
    IndexUtils.deserializeVersion(magicBytes)
  }
}

