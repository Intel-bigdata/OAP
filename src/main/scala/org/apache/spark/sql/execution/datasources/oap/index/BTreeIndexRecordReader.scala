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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

private[index] case class BTreeIndexRecordReader(
    configuration: Configuration,
    schema: StructType) extends Iterator[Int] {

  private var internalIterator: Iterator[Int] = _

  import BTreeIndexRecordReader._
  private var footer: BTreeFooter = _
  private var rowIdList: BTreeRowIdList = _
  private var reader: BTreeIndexFileReader = _

  private lazy val ordering = GenerateOrdering.create(schema)
  private lazy val partialOrdering = GenerateOrdering.create(StructType(schema.dropRight(1)))

  def initialize(path: Path, intervalArray: ArrayBuffer[RangeInterval]): Unit = {
    reader = BTreeIndexFileReader(configuration, path)
    footer = BTreeFooter(reader.readFooter())
    rowIdList = BTreeRowIdList(reader.readRowIdList())
    internalIterator = intervalArray.toIterator.flatMap { interval =>
      val (start, end) = findRowIdRange(interval)
      (start until end).toIterator.map(rowIdList.getRowId)
    }
  }

  private[index] def findRowIdRange(interval: RangeInterval): (Int, Int) = {
    val (nodeIdxForStart, isStartFound) = findNodeIdx(interval.start, isStart = true)
    val (nodeIdxForEnd, isEndFound) = findNodeIdx(interval.end, isStart = false)

    val recordCount = footer.getRecordCount
    if (nodeIdxForStart == nodeIdxForEnd && !isStartFound && !isEndFound) {
      (0, 0)
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
    val node =
      BTreeNodeData(reader.readNode(footer.getNodeOffset(nodeIdx), footer.getNodeSize(nodeIdx)))
    val keyCount = node.getKeyCount
    val (pos, found) =
      binarySearch(0, keyCount, node.getKey(_, schema), candidate, rowOrdering(_, _, isStart))
    val keyPos = if (found && findNext) pos + 1 else pos
    if (keyPos == keyCount) {
      if (nodeIdx + 1 == footer.getNodesCount) footer.getRecordCount
      else {
        val nextNode =
          BTreeNodeData(
            reader.readNode(
              footer.getNodeOffset(nodeIdx + 1), footer.getNodeSize(nodeIdx + 1)))
        nextNode.getRowIdPos(0)
      }
    } else node.getRowIdPos(keyPos)
  }

  /**
   * Find the Node index contains the candidate. If no, return the first which node.max >= candidate
   * If candidate > all node.max, return None
   * @param isStart to indicate if the candidate is interval.start or interval.end
   * @return Option of Node index and if candidate falls in node (means min <= candidate < max)
   */
  private def findNodeIdx(candidate: InternalRow, isStart: Boolean): (Option[Int], Boolean) = {
    val idxOption = (0 until footer.getNodesCount).find { idx =>
      rowOrdering(candidate, footer.getMaxValue(idx, schema), isStart) <= 0
    }

    (idxOption, idxOption.exists { idx =>
      rowOrdering(candidate, footer.getMinValue(idx, schema), isStart) >= 0
    })
  }

  /**
   * Constrain: keys.last >= candidate must be true. This is guaranteed by [[findNodeIdx]]
   * @return the first key >= candidate. (keys.last >= candidate makes this always possible)
   */
  private[index] def binarySearch(
      start: Int, length: Int,
      keys: Int => InternalRow, candidate: InternalRow,
      compare: (InternalRow, InternalRow) => Int): (Int, Boolean) = {
    var s = 0
    var e = length - 1
    var found = false
    var m = s
    while (s <= e & !found) {
      assert(s + e >= 0, "too large array size caused overflow")
      m = (s + e) / 2
      val cmp = compare(keys(m), candidate)
      if (cmp == 0) found = true
      else if (cmp < 0) s = m + 1
      else e = m - 1
    }
    if (!found) m = s
    (m, found)
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
    reader.close()
  }

  override def hasNext: Boolean = {
    if (internalIterator.hasNext) true
    else {
      close()
      false
    }
  }

  override def next(): Int = internalIterator.next()
}

private[index] object BTreeIndexRecordReader {
  private[index] def readBasedOnSchema(
      buffer: Array[Byte], offset: Int, schema: StructType): InternalRow = {
    var pos = offset
    val values = schema.map(_.dataType).map { dataType =>
      val (value, length) = readBasedOnDataType(buffer, pos, dataType)
      pos += length
      value
    }
    InternalRow.fromSeq(values)
  }

  private[index] def readBasedOnDataType(buffer: Array[Byte], offset: Int, dataType: DataType) = {
    dataType match {
      case BooleanType =>
        (Platform.getBoolean(buffer, Platform.BYTE_ARRAY_OFFSET + offset), BooleanType.defaultSize)
      case ByteType =>
        (Platform.getByte(buffer, Platform.BYTE_ARRAY_OFFSET + offset), ByteType.defaultSize)
      case ShortType =>
        (Platform.getShort(buffer, Platform.BYTE_ARRAY_OFFSET + offset), ShortType.defaultSize)
      case IntegerType =>
        (Platform.getInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset), IntegerType.defaultSize)
      case LongType =>
        (Platform.getLong(buffer, Platform.BYTE_ARRAY_OFFSET + offset), LongType.defaultSize)
      case FloatType =>
        (Platform.getFloat(buffer, Platform.BYTE_ARRAY_OFFSET + offset), FloatType.defaultSize)
      case DoubleType =>
        (Platform.getDouble(buffer, Platform.BYTE_ARRAY_OFFSET + offset), DoubleType.defaultSize)
      case DateType =>
        (Platform.getInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset), DateType.defaultSize)
      case StringType =>
        val bytes = new Array[Byte](Platform.getInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset))
        Platform.copyMemory(
          buffer, Platform.BYTE_ARRAY_OFFSET + offset + Integer.BYTES,
          bytes, Platform.BYTE_ARRAY_OFFSET,
          bytes.length)
        (UTF8String.fromBytes(bytes), Integer.BYTES + bytes.length)
      case BinaryType =>
        val bytes = new Array[Byte](Platform.getInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset))
        Platform.copyMemory(
          buffer, Platform.BYTE_ARRAY_OFFSET + offset + Integer.BYTES,
          bytes, Platform.BYTE_ARRAY_OFFSET,
          bytes.length)
        (bytes, Integer.BYTES + bytes.length)
      case _ => throw new OapException("Not supported data type")
    }
  }

  private[index] case class BTreeFooter(buf: Array[Byte]) {
    private val nodePosOffset = Integer.BYTES
    private val nodeSizeOffset = Integer.BYTES * 2
    private val minPosOffset = Integer.BYTES * 3
    private val maxPosOffset = Integer.BYTES * 4
    private val nodeMetaStart = Integer.BYTES * 2
    private val nodeMetaByteSize = Integer.BYTES * 5

    def getRecordCount: Int = Platform.getInt(buf, Platform.BYTE_ARRAY_OFFSET)
    def getNodesCount: Int = Platform.getInt(buf, Platform.BYTE_ARRAY_OFFSET + Integer.BYTES)
    def getMaxValue(idx: Int, schema: StructType): InternalRow =
      BTreeIndexRecordReader.readBasedOnSchema(
        buf, getMaxValueOffset(idx), schema)
    def getMinValue(idx: Int, schema: StructType): InternalRow =
      BTreeIndexRecordReader.readBasedOnSchema(
        buf, getMinValueOffset(idx), schema)
    def getMinValueOffset(idx: Int): Int =
      nodeMetaStart + nodeMetaByteSize * getNodesCount +
          Platform.getInt(
            buf, Platform.BYTE_ARRAY_OFFSET + nodeMetaStart + nodeMetaByteSize * idx + minPosOffset)
    def getMaxValueOffset(idx: Int): Int =
      nodeMetaStart + nodeMetaByteSize * getNodesCount +
          Platform.getInt(
            buf, Platform.BYTE_ARRAY_OFFSET + nodeMetaStart + nodeMetaByteSize * idx + maxPosOffset)
    def getNodeOffset(idx: Int): Int =
      Platform.getInt(
        buf, Platform.BYTE_ARRAY_OFFSET + nodeMetaStart + idx * nodeMetaByteSize + nodePosOffset)
    def getNodeSize(idx: Int): Int =
      Platform.getInt(
        buf, Platform.BYTE_ARRAY_OFFSET + nodeMetaStart + idx * nodeMetaByteSize + nodeSizeOffset)
  }

  private[index] case class BTreeRowIdList(buf: Array[Byte]) {
    def getRowId(idx: Int): Int =
      Platform.getInt(buf, Platform.BYTE_ARRAY_OFFSET + idx * Integer.BYTES)
  }

  private[index] case class BTreeNodeData(buf: Array[Byte]) {
    private val posSectionStart = Integer.BYTES
    private val posEntrySize = Integer.BYTES * 2
    private def valueSectionStart = posSectionStart + getKeyCount * posEntrySize

    def getKeyCount: Int = Platform.getInt(buf, Platform.BYTE_ARRAY_OFFSET)
    def getKey(idx: Int, schema: StructType): InternalRow = {
      val offset = valueSectionStart +
          Platform.getInt(buf, Platform.BYTE_ARRAY_OFFSET + posSectionStart + idx * posEntrySize)
      readBasedOnSchema(buf, offset, schema)
    }
    def getRowIdPos(idx: Int): Int =
      Platform.getInt(
        buf, Platform.BYTE_ARRAY_OFFSET + posSectionStart + idx * posEntrySize + Integer.BYTES)
  }
}
