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

package org.apache.spark.sql.execution.datasources.spinach

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.collection.BitSet

class ColumnValues(defaultSize: Int, dataType: DataType, val raw: FiberByteData) {
  require(dataType.isInstanceOf[AtomicType], "Only atomic type accepted for now.")

  // for any FiberData, the first defaultSize / 8 will be the bitmask
  // TODO what if defaultSize / 8 is not an integer?

  // TODO get the bitset from the FiberByteData
  val bitset: BitSet = {
    val bs = new BitSet(defaultSize)
    val longs = bs.toLongArray()
    Platform.copyMemory(raw.buf, Platform.BYTE_ARRAY_OFFSET,
      longs, Platform.LONG_ARRAY_OFFSET, longs.length * 8)
    bs
  }

  // TODO should be in FiberByteData
  private val baseOffset = Platform.BYTE_ARRAY_OFFSET + defaultSize / 8

  def isNullAt(idx: Int): Boolean = !bitset.get(idx)

  def get(idx: Int): AnyRef = dataType match {
    case IntegerType => new Integer(getInt(idx))
    case StringType => getString(idx)
    case other => throw new NotImplementedError(s"other")
  }

  def getBooleanValue(idx: Int): Boolean = {
    Platform.getBoolean(raw.buf, baseOffset + idx * 4)
  }
  def getByteValue(idx: Int): Byte = {
    Platform.getByte(raw.buf, baseOffset + idx * 1)
  }
  def getShortValue(idx: Int): Short = {
    Platform.getShort(raw.buf, baseOffset + idx * 2)
  }
  def getFloatValue(idx: Int): Float = {
    Platform.getFloat(raw.buf, baseOffset + idx * 4)
  }
  def getInt(idx: Int): Int = {
    Platform.getInt(raw.buf, baseOffset + idx * 4)
  }
  def getDouble(idx: Int): Double = {
    Platform.getDouble(raw.buf, baseOffset + idx * 4)
  }
  def getLong(idx: Int): Long = {
    Platform.getLong(raw.buf, baseOffset + idx * 8)
  }
  def getString(idx: Int): UTF8String = {
    //  The byte data format like:
    //    value #1 length (int)
    //    value #1 offset, (0 - based to the start of this Fiber Group)
    //    value #2 length
    //    value #2 offset, (0 - based to the start of this Fiber Group)
    //    …
    //    …
    //    value #N length
    //    value #N offset, (0 - based to the start of this Fiber Group)
    //    value #1
    //    value #2
    //    …
    //    value #N
    val length = getInt(idx * 2)
    val offset = getInt(idx * 2 + 1)
    UTF8String.fromAddress(raw.buf, Platform.BYTE_ARRAY_OFFSET + offset, length)
  }
  def getBinary(idx: Int): Array[Byte] = {
    //  The byte data format like:
    //    value #1 length (int)
    //    value #1 offset, (0 - based to the start of this Fiber Group)
    //    value #2 length
    //    value #2 offset, (0 - based to the start of this Fiber Group)
    //    …
    //    …
    //    value #N length
    //    value #N offset, (0 - based to the start of this Fiber Group)
    //    value #1
    //    value #2
    //    …
    //    value #N
    val length = getInt(idx * 2)
    val offset = getInt(idx * 2 + 1)
    val result = new Array[Byte](length)
    Platform.copyMemory(raw.buf, Platform.BYTE_ARRAY_OFFSET + offset, result,
      Platform.BYTE_ARRAY_OFFSET, length)

    result
  }
}

class BatchColumn {
  private var currentIndex: Int = 0
  private var rowCount: Int = 0
  private var values: Array[ColumnValues] = _

  def reset(rowCount: Int, values: Array[ColumnValues]): BatchColumn = {
    this.rowCount = rowCount
    this.values = values
    currentIndex = -1
    this
  }

  def toIterator: Iterator[InternalRow] = new Iterator[InternalRow]() {
    override def hasNext: Boolean = currentIndex < rowCount - 1

    override def next(): InternalRow = {
      currentIndex += 1
      internalRow
    }
  }

  object internalRow extends InternalRow {
    override def numFields: Int = values.length

    override def copy(): InternalRow = {
      val row = new Array[Any](values.length)
      var i = 0
      while (i < row.length) {
        row(i) = values(i).get(currentIndex)
        i += 1
      }
      new GenericInternalRow(row)
    }

    override def anyNull: Boolean = {
      var i = 0
      while (i < values.length) {
        if (values(i).isNullAt(currentIndex)) return true
        i += 1
      }
      return false
    }

    override def getUTF8String(ordinal: Int): UTF8String = values(ordinal).getString(currentIndex)

    override def get(ordinal: Int, dataType: DataType): AnyRef = values(ordinal).get(currentIndex)

    override def getBinary(ordinal: Int): Array[Byte] =
      throw new NotImplementedError("")

    override def getDouble(ordinal: Int): Double =
      throw new NotImplementedError("")

    override def getArray(ordinal: Int): ArrayData =
      throw new NotImplementedError("")

    override def getInterval(ordinal: Int): CalendarInterval =
      throw new NotImplementedError("")

    override def getFloat(ordinal: Int): Float =
      throw new NotImplementedError("")

    override def getLong(ordinal: Int): Long =
      throw new NotImplementedError("")

    override def getMap(ordinal: Int): MapData =
      throw new NotImplementedError("")

    override def getByte(ordinal: Int): Byte =
      throw new NotImplementedError("")

    override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
      throw new NotImplementedError("")

    override def getBoolean(ordinal: Int): Boolean =
      throw new NotImplementedError("")

    override def getShort(ordinal: Int): Short =
      throw new NotImplementedError("")

    override def getStruct(ordinal: Int, numFields: Int): InternalRow =
      throw new NotImplementedError("")

    override def getInt(ordinal: Int): Int = values(ordinal).getInt(currentIndex)

    override def isNullAt(ordinal: Int): Boolean = values(ordinal).isNullAt(currentIndex)
  }
}
