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
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.{StatisticsManager, StatsAnalysisResult}
import org.apache.spark.sql.execution.datasources.oap.utils.NonNullKeyReader
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.CompletionIterator

private[index] case class BTreeIndexRecordReaderV1(
    configuration: Configuration,
    schema: StructType,
    fileReader: IndexFileReader) extends BTreeIndexRecordReader {
  import BTreeIndexRecordReaderV1.{BTreeFooter, BTreeRowIdList, BTreeNodeData}

  private var internalIterator: Iterator[Int] = _
  private var initialized = false
  private var footer: BTreeFooter = _
  // Currently used FiberCache, will release it finally.
  private val MAX_IN_USE_FIBER_CACHE_NUM = 3
  private val inUseFiberCache = new Array[FiberCache](MAX_IN_USE_FIBER_CACHE_NUM)

  private def release(sectionId: Int): Unit = synchronized {
    Option(inUseFiberCache(sectionId)).foreach { fiberCache =>
      fiberCache.release()
      inUseFiberCache.update(sectionId, null)
    }
  }

  private def update(sectionId: Int, fiberCache: FiberCache): Unit = {
    release(sectionId)
    inUseFiberCache.update(sectionId, fiberCache)
  }

  private lazy val ordering = GenerateOrdering.create(schema)
  private lazy val partialOrdering = GenerateOrdering.create(StructType(schema.dropRight(1)))

  type CompareFunction = (InternalRow, InternalRow, Boolean) => Int

  private val FOOTER_LENGTH_SIZE = IndexUtils.INT_SIZE
  private val ROW_ID_LIST_LENGTH_SIZE = IndexUtils.LONG_SIZE

  val footerSectionId: Int = 0
  val rowIdListSectionId: Int = 1
  val nodeSectionId: Int = 2
  val rowIdListSizePerSection: Int =
    configuration.getInt(OapConf.OAP_BTREE_ROW_LIST_PART_SIZE.key, 1024 * 1024)

  private[index] case class BTreeFileMeta(
      fileLength: Long,
      footerLength: Int,
      rowIdListLength: Long) {
    def footerOffset: Long =
      fileLength - FOOTER_LENGTH_SIZE - ROW_ID_LIST_LENGTH_SIZE - footerLength
    def rowIdListOffset: Long = footerOffset - rowIdListLength
    def nodeOffset: Long = IndexFile.VERSION_LENGTH
  }

  private var meta: BTreeFileMeta = _
  private[index] def initFileMeta(): Unit = {
    initialized = true
    val sectionLengthIndex = fileReader.getLen - FOOTER_LENGTH_SIZE - ROW_ID_LIST_LENGTH_SIZE
    val sectionLengthBuffer = new Array[Byte](FOOTER_LENGTH_SIZE + ROW_ID_LIST_LENGTH_SIZE)
    fileReader.readFully(sectionLengthIndex, sectionLengthBuffer)
    val rowIdListSize = getLongFromBuffer(sectionLengthBuffer, 0)
    val footerSize = getIntFromBuffer(sectionLengthBuffer, ROW_ID_LIST_LENGTH_SIZE)
    meta = BTreeFileMeta(fileReader.getLen, footerSize, rowIdListSize)
  }
  private[index] def getFileMeta: BTreeFileMeta = meta

  private def getLongFromBuffer(buffer: Array[Byte], offset: Int) =
    Platform.getLong(buffer, Platform.BYTE_ARRAY_OFFSET + offset)

  private def getIntFromBuffer(buffer: Array[Byte], offset: Int) =
    Platform.getInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset)

  private[index] def readFooter() =
    fileReader.readFiberCache(meta.footerOffset, meta.footerLength)

  private[index] def readRowIdList(partIdx: Int) = {
    val partSize = rowIdListSizePerSection.toLong * IndexUtils.INT_SIZE
    val readLength = if (partIdx * partSize + partSize > meta.rowIdListLength) {
      meta.rowIdListLength % partSize
    } else {
      partSize
    }
    assert(readLength <= Int.MaxValue, "Size of each row id list partition is too large!")
    fileReader.readFiberCache(meta.rowIdListOffset + partIdx * partSize, readLength.toInt)
  }

  private[index] def readNode(offset: Int, size: Int) =
    fileReader.readFiberCache(meta.nodeOffset + offset, size)

  def analyzeStatistics(
      keySchema: StructType,
      intervalArray: ArrayBuffer[RangeInterval]): StatsAnalysisResult = {
    if (!initialized) {
      initFileMeta()
      val footerFiber = BTreeFiber(
        () => readFooter(), fileReader.getName, footerSectionId, 0)
      val footerCache = FiberCacheManager.get(footerFiber, configuration)
      update(footerSectionId, footerCache)
      footer = BTreeFooter(footerCache, schema)
    }
    val offset = footer.getStatsOffset
    val stats = StatisticsManager.read(footer.fiberCache, offset, keySchema)
    StatisticsManager.analyse(stats, intervalArray, configuration)
  }

  def initialize(path: Path, intervalArray: ArrayBuffer[RangeInterval]): Unit = {
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => close()))
    if (!initialized) {
      initFileMeta()
      val footerFiber = BTreeFiber(
        () => readFooter(), fileReader.getName, footerSectionId, 0)
      val footerCache = FiberCacheManager.get(footerFiber, configuration)
      update(footerSectionId, footerCache)
      footer = BTreeFooter(footerCache, schema)
    }

    internalIterator = intervalArray.toIterator.flatMap { interval =>
      val (start, end) = findRowIdRange(interval)
      val groupedPos = (start until end).groupBy(i => i / rowIdListSizePerSection)
      groupedPos.toIterator.flatMap {
        case (partIdx, subPosList) =>
          val rowIdListFiber = BTreeFiber(
            () => readRowIdList(partIdx),
            fileReader.getName,
            rowIdListSectionId, partIdx)

          val rowIdListCache = FiberCacheManager.get(rowIdListFiber, configuration)
          update(rowIdListSectionId, rowIdListCache)
          val rowIdList = BTreeRowIdList(rowIdListCache)
          val iterator =
            subPosList.toIterator.map(i => rowIdList.getRowId(i % rowIdListSizePerSection))
          CompletionIterator[Int, Iterator[Int]](iterator, release(rowIdListSectionId))
      }
    } // get the row ids
  }

  // find the row id list start pos, end pos of the range interval
  private[index] def findRowIdRange(interval: RangeInterval): (Int, Int) = {
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

  private def findRowIdPos(
      nodeIdx: Int,
      candidate: InternalRow,
      isStart: Boolean,
      findNext: Boolean,
      compareFunc: CompareFunction = rowOrdering): Int = {

    val nodeFiber = BTreeFiber(
      () => readNode(footer.getNodeOffset(nodeIdx), footer.getNodeSize(nodeIdx)),
      fileReader.getName,
      nodeSectionId,
      nodeIdx
    )
    val nodeCache = FiberCacheManager.get(nodeFiber, configuration)
    update(nodeSectionId, nodeCache)
    val node = BTreeNodeData(nodeCache, schema)

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
          val offset = footer.getNodeOffset(nodeIdx + 1)
          val size = footer.getNodeSize(nodeIdx + 1)
          val nextNodeFiber = BTreeFiber(
            () => readNode(offset, size),
            fileReader.getName,
            nodeSectionId,
            nodeIdx + 1)
          val nextNodeCache = FiberCacheManager.get(nextNodeFiber, configuration)
          val nextNode = BTreeNodeData(nextNodeCache, schema)
          update(nodeSectionId, nextNodeCache)
          val rowPos = nextNode.getRowIdPos(0)
          rowPos
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
   * @param x, y are the key to be compared.
   *         One comes from interval.start/end. One comes from index records
   * @param isStart indicates to compare interval.start or interval end
   */
  private[index] def rowOrdering(x: InternalRow, y: InternalRow, isStart: Boolean): Int = {
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
  private[index] def rowOrderingPattern(x: InternalRow, y: InternalRow, isStart: Boolean): Int = {
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

  private[index] def strMatching(xStr: String, yStr: String): Boolean = yStr.startsWith(xStr)

  def totalRows(): Long =
    (0 until footer.getNodesCount).map(footer.getRowCountOfNode).sum

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
}

private[index] object BTreeIndexRecordReaderV1 {

  private[index] case class BTreeFooter(fiberCache: FiberCache, schema: StructType) {
    // TODO move to companion object
    private val nodePosOffset = IndexUtils.INT_SIZE
    private val nodeSizeOffset = IndexUtils.INT_SIZE * 2
    private val minPosOffset = IndexUtils.INT_SIZE * 3
    private val maxPosOffset = IndexUtils.INT_SIZE * 4
    private val nodeMetaStart = IndexUtils.INT_SIZE * 4
    private val nodeMetaByteSize = IndexUtils.INT_SIZE * 5
    private val statsLengthSize = IndexUtils.INT_SIZE

    @transient protected lazy val nnkr: NonNullKeyReader = new NonNullKeyReader(schema)

    def getVersionNum: Int = fiberCache.getInt(0)

    def getNonNullKeyRecordCount: Int = fiberCache.getInt(IndexUtils.INT_SIZE)

    def getNullKeyRecordCount: Int = fiberCache.getInt(IndexUtils.INT_SIZE * 2)

    def getNodesCount: Int = fiberCache.getInt(IndexUtils.INT_SIZE * 3)

    // get idx Node's max value
    def getMaxValue(idx: Int, schema: StructType): InternalRow =
      nnkr.readKey(fiberCache, getMaxValueOffset(idx))._1

    def getMinValue(idx: Int, schema: StructType): InternalRow =
      nnkr.readKey(fiberCache, getMinValueOffset(idx))._1

    def getMinValueOffset(idx: Int): Int = {
      fiberCache.getInt(nodeMetaStart + nodeMetaByteSize * idx + minPosOffset) +
          nodeMetaStart + nodeMetaByteSize * getNodesCount + statsLengthSize + getStatsLength
    }

    def getMaxValueOffset(idx: Int): Int = {
      fiberCache.getInt(nodeMetaStart + nodeMetaByteSize * idx + maxPosOffset) +
          nodeMetaStart + nodeMetaByteSize * getNodesCount + statsLengthSize + getStatsLength
    }

    def getRowCountOfNode(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + idx * nodeMetaByteSize)

    def getNodeOffset(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + idx * nodeMetaByteSize + nodePosOffset)

    def getNodeSize(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + idx * nodeMetaByteSize + nodeSizeOffset)

    def getStatsOffset: Int = nodeMetaStart + nodeMetaByteSize * getNodesCount + statsLengthSize

    private def getStatsLength: Int =
      fiberCache.getInt(nodeMetaStart + nodeMetaByteSize * getNodesCount)
  }

  private[index] case class BTreeRowIdList(fiberCache: FiberCache) {
    def getRowId(idx: Int): Int = fiberCache.getInt(idx * IndexUtils.INT_SIZE)
  }

  private[index] case class BTreeNodeData(fiberCache: FiberCache, schema: StructType) {
    private val posSectionStart = IndexUtils.INT_SIZE
    private val posEntrySize = IndexUtils.INT_SIZE * 2
    private def valueSectionStart = posSectionStart + getKeyCount * posEntrySize

    @transient
    protected lazy val nnkr: NonNullKeyReader = new NonNullKeyReader(schema)

    def getKeyCount: Int = fiberCache.getInt(0)

    def getKey(idx: Int, schema: StructType): InternalRow = {
      val offset = valueSectionStart +
          fiberCache.getInt(posSectionStart + idx * posEntrySize)
      nnkr.readKey(fiberCache, offset)._1
    }

    def getRowIdPos(idx: Int): Int =
      fiberCache.getInt(posSectionStart + idx * posEntrySize + IndexUtils.INT_SIZE)
  }
}
