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

package org.apache.spark.sql.execution.datasources.spinach.filecache

import scala.collection.mutable.ArrayBuffer

import org.apache.parquet.format.Encoding

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.spinach.io.{DeltaByteArrayFiberBuilder, PlainBinaryDictionaryFiberBuilder, PlainIntegerDictionaryFiberBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.BitSet


/**
 * Used to build fiber on-heap
 */
case class FiberByteData(fiberData: Array[Byte])

private[spinach] trait DataFiberBuilder {
  def defaultRowGroupRowCount: Int
  def ordinal: Int

  protected val bitStream: BitSet = new BitSet(defaultRowGroupRowCount)
  protected var currentRowId: Int = 0

  def append(row: InternalRow): Unit = {
    require(currentRowId < defaultRowGroupRowCount, "fiber data overflow")
    if (!row.isNullAt(ordinal)) {
      bitStream.set(currentRowId)
      appendInternal(row)
    } else {
      appendNull()
    }

    currentRowId += 1
  }

  protected def appendInternal(row: InternalRow)
  protected def appendNull(): Unit = { }
  protected def fillBitStream(bytes: Array[Byte]): Unit = {
    val bitmasks = bitStream.toLongArray()
    Platform.copyMemory(bitmasks, Platform.LONG_ARRAY_OFFSET,
      bytes, Platform.BYTE_ARRAY_OFFSET, bitmasks.length * 8)
  }

  def build(): FiberByteData
  def count(): Int = currentRowId
  def clear(): Unit = {
    currentRowId = 0
    bitStream.clear()
  }

  def getEncoding: Encoding = Encoding.PLAIN

  def buildDictionary: Array[Byte] = Array.empty[Byte]
  def getDictionarySize: Int = 0
  def resetDictionary(): Unit = {}
}

private[spinach] case class FixedSizeTypeFiberBuilder(
    defaultRowGroupRowCount: Int,
    ordinal: Int,
    dataType: DataType) extends DataFiberBuilder {
  private val typeDefaultSize = dataType match {
    case BooleanType => BooleanType.defaultSize
    case ByteType => ByteType.defaultSize
    case DateType => IntegerType.defaultSize  // use IntegerType instead of using DateType
    case DoubleType => DoubleType.defaultSize
    case FloatType => FloatType.defaultSize
    case IntegerType => IntegerType.defaultSize
    case LongType => LongType.defaultSize
    case ShortType => ShortType.defaultSize
    case _ => throw new NotImplementedError("unknown data type default size")
  }
  private val baseOffset = bitStream.toLongArray().length * 8 + Platform.BYTE_ARRAY_OFFSET
  // TODO use the memory pool?
  private val bytes =
    new Array[Byte](bitStream.toLongArray().length * 8 +
      defaultRowGroupRowCount * typeDefaultSize)

  override protected def appendInternal(row: InternalRow): Unit = {
    dataType match {
      case BooleanType =>
        Platform.putBoolean(bytes, baseOffset + currentRowId * typeDefaultSize,
          row.getBoolean(ordinal))
      case ByteType =>
        Platform.putByte(bytes, baseOffset + currentRowId * typeDefaultSize,
          row.getByte(ordinal))
      case DateType =>
        Platform.putInt(bytes, baseOffset + currentRowId * typeDefaultSize,
          row.get(ordinal, DateType).asInstanceOf[Int])
      case DoubleType =>
        Platform.putDouble(bytes, baseOffset + currentRowId * typeDefaultSize,
          row.getDouble(ordinal))
      case FloatType =>
        Platform.putFloat(bytes, baseOffset + currentRowId * typeDefaultSize,
          row.getFloat(ordinal))
      case IntegerType =>
        Platform.putInt(bytes, baseOffset + currentRowId * typeDefaultSize,
          row.getInt(ordinal))
      case LongType =>
        Platform.putLong(bytes, baseOffset + currentRowId * typeDefaultSize,
          row.getLong(ordinal))
      case ShortType =>
        Platform.putShort(bytes, baseOffset + currentRowId * typeDefaultSize,
          row.getShort(ordinal))
      case _ => throw new NotImplementedError("not implemented Data Type")
    }
  }

  //  Field       Length in Byte            Description
  //  BitStream   defaultRowGroupRowCount / 8   To represent if the value is null
  //  value #1    typeDefaultSize
  //  value #2    typeDefaultSize
  //  ...
  //  value #N    typeDefaultSize
  def build(): FiberByteData = {
    fillBitStream(bytes)
    if (currentRowId == defaultRowGroupRowCount) {
      FiberByteData(bytes)
    } else {
      // shrink memory
      val newBytes = new Array[Byte](bitStream.toLongArray().length * 8 +
        currentRowId * typeDefaultSize)
      Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET,
        newBytes, Platform.BYTE_ARRAY_OFFSET, newBytes.length)
      FiberByteData(newBytes)
    }
  }
}

private[spinach] case class BinaryFiberBuilder(
    defaultRowGroupRowCount: Int,
    ordinal: Int) extends DataFiberBuilder {
  private val binaryArrs: ArrayBuffer[Array[Byte]] =
    new ArrayBuffer[Array[Byte]](defaultRowGroupRowCount)
  private var totalLengthInByte: Int = 0

  override protected def appendInternal(row: InternalRow): Unit = {
    val binaryData = row.getBinary(ordinal)
    val copiedBinaryData = new Array[Byte](binaryData.length)
    binaryData.copyToArray(copiedBinaryData)  // TODO to eliminate the copy
    totalLengthInByte += copiedBinaryData.length
    binaryArrs.append(copiedBinaryData)
  }

  override protected def appendNull(): Unit = {
    // fill the dummy value
    binaryArrs.append(null)
  }

  //  Field                 Size In Byte      Description
  //  BitStream             (defaultRowGroupRowCount / 8)  TODO to improve the memory usage
  //  value #1 length       4                 number of bytes for this binary
  //  value #1 offset       4                 (0 - based to the start of this Fiber Group)
  //  value #2 length       4
  //  value #2 offset       4                 (0 - based to the start of this Fiber Group)
  //  ...
  //  value #N length       4
  //  value #N offset       4                 (0 - based to the start of this Fiber Group)
  //  value #1              value #1 length
  //  value #2              value #2 length
  //  ...
  //  value #N              value #N length
  override def build(): FiberByteData = {
    val fiberDataLength =
      bitStream.toLongArray().length * 8 +  // bit mask length
        currentRowId * 8 +                  // bit
        totalLengthInByte
    val bytes = new Array[Byte](fiberDataLength)
    fillBitStream(bytes)
    val basePointerOffset = bitStream.toLongArray().length * 8 + Platform.BYTE_ARRAY_OFFSET
    var startValueOffset = bitStream.toLongArray().length * 8 + currentRowId * 8
    var i = 0
    while (i < binaryArrs.length) {
      val b = binaryArrs(i)
      if (b != null) {
        val valueLengthInByte = b.length
        // length of value #i
        Platform.putInt(bytes, basePointerOffset + i * 8, valueLengthInByte)
        // offset of value #i
        Platform.putInt(bytes, basePointerOffset + i * 8 + IntegerType.defaultSize,
          startValueOffset)
        // copy the string bytes
        Platform.copyMemory(b, Platform.BYTE_ARRAY_OFFSET, bytes,
          Platform.BYTE_ARRAY_OFFSET + startValueOffset, valueLengthInByte)

        startValueOffset += valueLengthInByte
      }
      i += 1
    }

    FiberByteData(bytes)
  }

  override def clear(): Unit = {
    super.clear()
    this.binaryArrs.clear()
    this.totalLengthInByte = 0
  }
}


private[spinach] case class StringFiberBuilder(
    defaultRowGroupRowCount: Int,
    ordinal: Int) extends DataFiberBuilder {
  private val strings: ArrayBuffer[UTF8String] =
    new ArrayBuffer[UTF8String](defaultRowGroupRowCount)
  private var totalStringDataLengthInByte: Int = 0

  override protected def appendInternal(row: InternalRow): Unit = {
    val s = row.getUTF8String(ordinal).clone()  // TODO to eliminate the copy
    totalStringDataLengthInByte += s.numBytes()
    strings.append(s)
  }

  override protected def appendNull(): Unit = {
    // fill the dummy value
    strings.append(null)
  }

  //  Field                 Size In Byte      Description
  //  BitStream             (defaultRowGroupRowCount / 8)  TODO to improve the memory usage
  //  value #1 length       4                 number of bytes for this string
  //  value #1 offset       4                 (0 - based to the start of this Fiber Group)
  //  value #2 length       4
  //  value #2 offset       4                 (0 - based to the start of this Fiber Group)
  //  ...
  //  value #N length       4
  //  value #N offset       4                 (0 - based to the start of this Fiber Group)
  //  value #1              value #1 length
  //  value #2              value #2 length
  //  ...
  //  value #N              value #N length
  override def build(): FiberByteData = {
    val fiberDataLength =
      bitStream.toLongArray().length * 8 +  // bit mask length
        currentRowId * 8 +                  // bit
        totalStringDataLengthInByte
    val bytes = new Array[Byte](fiberDataLength)
    fillBitStream(bytes)
    val basePointerOffset = bitStream.toLongArray().length * 8 + Platform.BYTE_ARRAY_OFFSET
    var startValueOffset = bitStream.toLongArray().length * 8 + currentRowId * 8
    var i = 0
    while (i < strings.length) {
      val s = strings(i)
      if (s != null) {
        val valueLengthInByte = s.numBytes()
        // length of value #i
        Platform.putInt(bytes, basePointerOffset + i * 8, valueLengthInByte)
        // offset of value #i
        Platform.putInt(bytes, basePointerOffset + i * 8 + IntegerType.defaultSize,
          startValueOffset)
        // copy the string bytes
        Platform.copyMemory(s.getBaseObject, s.getBaseOffset, bytes,
          Platform.BYTE_ARRAY_OFFSET + startValueOffset, valueLengthInByte)

        startValueOffset += valueLengthInByte
      }
      i += 1
    }

    FiberByteData(bytes)
  }

  override def clear(): Unit = {
    super.clear()
    this.strings.clear()
    this.totalStringDataLengthInByte = 0
  }
}

object DataFiberBuilder {
  def apply(dataType: DataType, ordinal: Int, defaultRowGroupRowCount: Int): DataFiberBuilder = {

    // TODO: [linhong] Plan to determine dictionaryEnable by statistics
    val dictionaryEnabled =
      System.getProperty("spinach.encoding.dictionaryEnabled", "false").toBoolean

    dataType match {
      case BinaryType | StringType =>
        if (dictionaryEnabled) {
          PlainBinaryDictionaryFiberBuilder(defaultRowGroupRowCount, ordinal, dataType)
        } else {
          DeltaByteArrayFiberBuilder(defaultRowGroupRowCount, ordinal, dataType)
        }
      case BooleanType =>
        FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, BooleanType)
      case ByteType =>
        FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, ByteType)
      case DateType =>
        FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, DateType)
      case DoubleType =>
        FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, DoubleType)
      case FloatType =>
        FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, FloatType)
      case IntegerType =>
        if (dictionaryEnabled) {
          PlainIntegerDictionaryFiberBuilder(defaultRowGroupRowCount, ordinal, dataType)
        } else {
          FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, IntegerType)
        }
      case LongType =>
        FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, LongType)
      case ShortType =>
        FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, ShortType)
      case _ => throw new NotImplementedError("not implemented type for fiber builder")
    }
  }

  def initializeFromSchema(
      schema: StructType,
      defaultRowGroupRowCount: Int): Array[DataFiberBuilder] = {
    schema.fields.zipWithIndex.map {
      case (field, oridinal) => DataFiberBuilder(field.dataType, oridinal, defaultRowGroupRowCount)
    }
  }
}
