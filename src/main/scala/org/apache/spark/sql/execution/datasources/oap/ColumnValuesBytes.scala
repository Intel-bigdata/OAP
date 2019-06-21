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

package org.apache.spark.sql.execution.datasources.oap

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class ColumnValuesBytes(defaultSize: Int, dataType: DataType, bytesData: Array[Byte])
  extends ColumnValues(defaultSize, dataType, null) {
  require(dataType.isInstanceOf[AtomicType], s"Only atomic type accepted for now, got $dataType.")

  // for any FiberData, the first `(defaultSize - 1) >> 6 + 1` longs will be the bitmask
  // num of bytes needed to hold defaultSize elements is then `((defaultSize - 1) >> 6 << 3) + 8`
  private val dataOffset = ((defaultSize - 1) >> 6 << 3) + 8

  var byteBufferWraper = ByteBuffer.wrap(bytesData).order(ByteOrder.LITTLE_ENDIAN)

  override def isNullAt(idx: Int): Boolean = {
    val bitmask = 1L << (idx & 0x3f)   // mod 64 and shift
    (byteBufferWraper.getLong(idx >> 6 << 3) & bitmask) == 0  // div by 64 and mask
  }

  private def genericGet(idx: Int): Any = dataType match {
    case BinaryType => getBinaryValue(idx)
    case BooleanType => getBooleanValue(idx)
    case ByteType => getByteValue(idx)
    case DateType => getDateValue(idx)
    case DoubleType => getDoubleValue(idx)
    case FloatType => getFloatValue(idx)
    case IntegerType => getIntValue(idx)
    case LongType => getLongValue(idx)
    case ShortType => getShortValue(idx)
    case StringType => getStringValue(idx)
    case _: ArrayType => throw new NotImplementedError(s"Array")
    case CalendarIntervalType => throw new NotImplementedError(s"CalendarInterval")
    case _: DecimalType => throw new NotImplementedError(s"Decimal")
    case _: MapType => throw new NotImplementedError(s"Map")
    case _: StructType => throw new NotImplementedError(s"Struct")
    case TimestampType => throw new NotImplementedError(s"Timestamp")
    case other => throw new NotImplementedError(s"$other")
  }

  private def getAs[T](idx: Int): T = genericGet(idx).asInstanceOf[T]
  override def get(idx: Int): AnyRef = getAs(idx)

  override def getBooleanValue(idx: Int): Boolean = {
    bytesData(dataOffset + idx * BooleanType.defaultSize) == 1
  }
  override def getByteValue(idx: Int): Byte = {
    byteBufferWraper.get(dataOffset + idx * ByteType.defaultSize)
  }
  override def getDateValue(idx: Int): Int = {
    byteBufferWraper.getInt(dataOffset + idx * IntegerType.defaultSize)
  }
  override def getDoubleValue(idx: Int): Double = {
    byteBufferWraper.getDouble(dataOffset + idx * DoubleType.defaultSize)
  }
  override def getIntValue(idx: Int): Int = {
    byteBufferWraper.getInt(dataOffset + idx * IntegerType.defaultSize)
  }
  override def getLongValue(idx: Int): Long = {
    byteBufferWraper.getLong(dataOffset + idx * LongType.defaultSize)
  }
  override def getShortValue(idx: Int): Short = {
    byteBufferWraper.getShort(dataOffset + idx * ShortType.defaultSize)
  }
  override def getFloatValue(idx: Int): Float = {
    byteBufferWraper.getFloat(dataOffset + idx * FloatType.defaultSize)
  }

  override def getStringValue(idx: Int): UTF8String = {
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
    val length = getIntValue(idx * 2)
    val offset = getIntValue(idx * 2 + 1)

    UTF8String.fromBytes(bytesData, offset, length)
  }

  override def getBinaryValue(idx: Int): Array[Byte] = {
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
    val length = getIntValue(idx * 2)
    val offset = getIntValue(idx * 2 + 1)

    val array = new Array[Byte](length)
    System.arraycopy(bytesData, offset, array, 0, length)
    array
  }
}


