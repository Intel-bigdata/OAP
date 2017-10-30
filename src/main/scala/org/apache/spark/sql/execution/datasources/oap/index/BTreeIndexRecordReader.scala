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
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

private case class BTreeIndexRecordReader(
    configuration: Configuration,
    keySchema: StructType) extends Iterator[Int] {

  private var internalIterator: Iterator[Int] = _

  import BTreeIndexRecordReader._
  private var footer: BTreeFooter = _
  private var rowIdList: BTreeRowIdList = _
  private var reader: BTreeIndexFileReader = _

  def initialize(path: Path, intervalArray: ArrayBuffer[RangeInterval]): Unit = {

    reader = BTreeIndexFileReader(configuration, path)
    footer = BTreeFooter(reader.readFooter())
    rowIdList = BTreeRowIdList(reader.readRowIdList())

    internalIterator = intervalArray.toIterator.flatMap { interval =>
      val (start, end) = findRowIdRange(interval)
      (start until end).toIterator.map(rowIdList.getRowId)
    }
  }

  private def findRowIdRange(interval: RangeInterval): (Int, Int) = {
    // TODO: Need implementation
    (0, footer.getRecordCount)
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
      nodeMetaStart + nodeMetaByteSize * getNodesCount + Platform.getInt(
        buf,
        Platform.BYTE_ARRAY_OFFSET + nodeMetaStart + nodeMetaByteSize * idx + minPosOffset)
    def getMaxValueOffset(idx: Int): Int =
      nodeMetaStart + nodeMetaByteSize * getNodesCount + Platform.getInt(
        buf,
        Platform.BYTE_ARRAY_OFFSET + nodeMetaStart + nodeMetaByteSize * idx + maxPosOffset)
    def getNodeOffset(idx: Int): Int =
      Platform.getInt(
        buf,
        Platform.BYTE_ARRAY_OFFSET + nodeMetaStart + idx * nodeMetaByteSize + nodePosOffset)
    def getNodeSize(idx: Int): Int =
      Platform.getInt(
        buf,
        Platform.BYTE_ARRAY_OFFSET + nodeMetaStart + idx * nodeMetaByteSize + nodeSizeOffset)
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
    def getRowIdPos(idx: Int): Int = Platform.getInt(
      buf, Platform.BYTE_ARRAY_OFFSET + posSectionStart + idx * posEntrySize + Integer.BYTES)
  }
}
