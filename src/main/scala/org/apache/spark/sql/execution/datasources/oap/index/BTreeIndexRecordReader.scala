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
import org.apache.spark.sql.execution.datasources.oap.filecache.{BTreeFiber, FiberCache, FiberCacheManager, WrappedFiberCache}
import org.apache.spark.sql.execution.datasources.oap.utils.NonNullKeyReader
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator


private[index] case class BTreeIndexRecordReader(
    configuration: Configuration,
    schema: StructType) extends Iterator[Int] {

  private var internalIterator: Iterator[Int] = _

  import BTreeIndexRecordReader.{BTreeFooter, BTreeRowIdList, BTreeNodeData}
  private var footer: BTreeFooter = _
  private var footerFiber: BTreeFiber = _
  private var footerCache: WrappedFiberCache = _
  private val indexCaches: ArrayBuffer[WrappedFiberCache] = new ArrayBuffer[WrappedFiberCache]()

  private var reader: BTreeIndexFileReader = _

  private lazy val ordering = GenerateOrdering.create(schema)
  private lazy val partialOrdering = GenerateOrdering.create(StructType(schema.dropRight(1)))

  def initialize(path: Path, intervalArray: ArrayBuffer[RangeInterval]): Unit = {
    reader = BTreeIndexFileReader(configuration, path)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => close()))

    footerFiber = BTreeFiber(
      () => reader.readFooter(), reader.file.toString, reader.footerSectionId, 0)
    footerCache = WrappedFiberCache(FiberCacheManager.get(footerFiber, configuration))
    indexCaches += footerCache
    footer = BTreeFooter(footerCache.fc, schema)

    reader.checkVersionNum(footer.getVersionNum)

    internalIterator = intervalArray.toIterator.flatMap { interval =>
      val (start, end) = findRowIdRange(interval)
      val groupedPos = (start until end).groupBy(i => i / reader.rowIdListSizePerSection)
      groupedPos.toIterator.flatMap {
        case (partIdx, subPosList) =>
          val rowIdListFiber = BTreeFiber(
            () => reader.readRowIdList(partIdx),
            reader.file.toString,
            reader.rowIdListSectionId, partIdx)

          val rowIdListCache =
            WrappedFiberCache(FiberCacheManager.get(rowIdListFiber, configuration))
          indexCaches += rowIdListCache
          val rowIdList = BTreeRowIdList(rowIdListCache.fc)
          val iterator =
            subPosList.toIterator.map(i => rowIdList.getRowId(i % reader.rowIdListSizePerSection))
          CompletionIterator[Int, Iterator[Int]](iterator, rowIdListCache.release())
      }
    } // get the row ids
  }
  // find the row id list start pos, end pos of the range interval
  private[index] def findRowIdRange(interval: RangeInterval): (Int, Int) = {
    val recordCount = footer.getNonNullKeyRecordCount
    if (interval.isNullPredicate) { // process "isNull" predicate
      return (recordCount, recordCount + footer.getNullKeyRecordCount)
    }
    val (nodeIdxForStart, isStartFound) = findNodeIdx(interval.start, isStart = true)
    val (nodeIdxForEnd, isEndFound) = findNodeIdx(interval.end, isStart = false)

    if (nodeIdxForStart == nodeIdxForEnd && !isStartFound && !isEndFound) {
      (0, 0) // not found in B+ tree
    } else {
      val start = if (interval.start == IndexScanner.DUMMY_KEY_START) 0
      else {
        nodeIdxForStart.map { idx =>
          findRowIdPos(idx, interval.start, isStart = true, !interval.startInclude)
        }.getOrElse(recordCount)
      }
      val end = if (interval.end == IndexScanner.DUMMY_KEY_END) recordCount
      else {
        nodeIdxForEnd.map { idx =>
          findRowIdPos(idx, interval.end, isStart = false, interval.endInclude)
        }.getOrElse(recordCount)
      }
      (start, end)
    }
  }

  private def findRowIdPos(
      nodeIdx: Int,
      candidate: InternalRow,
      isStart: Boolean,
      findNext: Boolean): Int = {

    val nodeFiber = BTreeFiber(
      () => reader.readNode(footer.getNodeOffset(nodeIdx), footer.getNodeSize(nodeIdx)),
      reader.file.toString,
      reader.nodeSectionId,
      nodeIdx
    )
    val nodeCache = WrappedFiberCache(FiberCacheManager.get(nodeFiber, configuration))
    indexCaches += nodeCache
    val node = BTreeNodeData(nodeCache.fc, schema)

    val keyCount = node.getKeyCount

    val (pos, found) =
      IndexUtils.binarySearch(0, keyCount, node.getKey(_, schema), candidate,
        rowOrdering(_, _, isStart))

    val keyPos = if (found && findNext) pos + 1 else pos

    val rowPos =
      if (keyPos == keyCount) {
        if (nodeIdx + 1 == footer.getNodesCount) footer.getNonNullKeyRecordCount
        else {
          val offset = footer.getNodeOffset(nodeIdx + 1)
          val size = footer.getNodeSize(nodeIdx + 1)
          val nextNodeFiber = BTreeFiber(
            () => reader.readNode(offset, size),
            reader.file.toString,
            reader.nodeSectionId,
            nodeIdx + 1)
          val nextNodeCache = WrappedFiberCache(FiberCacheManager.get(nextNodeFiber, configuration))
          indexCaches += nextNodeCache
          val nextNode = BTreeNodeData(nextNodeCache.fc, schema)
          val rowPos = nextNode.getRowIdPos(0)
          nextNodeCache.release()
          rowPos
        }
      } else node.getRowIdPos(keyPos)
    nodeCache.release()
    rowPos
  }

  /**
   * Find the Node index contains the candidate. If no, return the first which node.max >= candidate
   * If candidate > all node.max, return None
   * @param isStart to indicate if the candidate is interval.start or interval.end
   * @return Option of Node index and if candidate falls in node (means min <= candidate < max)
   */
  private def findNodeIdx(candidate: InternalRow, isStart: Boolean): (Option[Int], Boolean) = {
    val idxOption = (0 until footer.getNodesCount).find { idx =>
      footer.getRowCountOfNode(idx) > 0 && // ensure this node is not an empty node
        rowOrdering(candidate, footer.getMaxValue(idx, schema), isStart) <= 0
    }

    (idxOption, idxOption.exists { idx =>
      footer.getRowCountOfNode(idx) > 0 && // ensure this node is not an empty node
        rowOrdering(candidate, footer.getMinValue(idx, schema), isStart) >= 0
    })
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
      } else cmp
    } else {
      -rowOrdering(y, x, isStart) // Keep x.numFields <= y.numFields to simplify
    }
  }

  def close(): Unit = {
    if (reader != null) {
      reader.close()
      reader = null
    }
    indexCaches.foreach(_.release())
    indexCaches.clear()
  }

  override def hasNext: Boolean = if (internalIterator.hasNext) {
    true
  } else {
    close()
    false
  }

  override def next(): Int = internalIterator.next()
}

private[index] object BTreeIndexRecordReader {

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
    def getMinValueOffset(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + nodeMetaByteSize * idx + minPosOffset) +
          nodeMetaStart + nodeMetaByteSize * getNodesCount + statsLengthSize + getStatsLength
    def getMaxValueOffset(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + nodeMetaByteSize * idx + maxPosOffset) +
        nodeMetaStart + nodeMetaByteSize * getNodesCount + statsLengthSize + getStatsLength
    def getRowCountOfNode(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + idx * nodeMetaByteSize)
    def getNodeOffset(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + idx * nodeMetaByteSize + nodePosOffset)
    def getNodeSize(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + idx * nodeMetaByteSize + nodeSizeOffset)
    def getStatsOffset: Int = nodeMetaStart + nodeMetaByteSize * getNodesCount + statsLengthSize
    private def getStatsLength: Int = fiberCache.getInt(
      nodeMetaStart + nodeMetaByteSize * getNodesCount)
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
