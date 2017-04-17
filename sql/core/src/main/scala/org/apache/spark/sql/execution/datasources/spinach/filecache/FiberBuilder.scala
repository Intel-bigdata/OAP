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

import java.io.ByteArrayOutputStream

import org.apache.parquet.Ints
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.{PlainBinaryDictionaryValuesWriter, PlainFixedLenArrayDictionaryValuesWriter}
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder
import org.apache.parquet.format.Encoding
import org.apache.parquet.io.api.Binary

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.InternalRow
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

  def getEncoding(): Encoding = Encoding.PLAIN

  def buildDictionary(): Array[Byte] = Array.empty[Byte]
  def getDictionarySize: Int = 0
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

// TODO: Need Implement other FiberBuilder: DICT, DELTA, etc
// Refer: https://github.com/Parquet/parquet-format/blob/master/Encodings.md
private[spinach] case class RunLengthBitPackingHybridFiberBuilder(
  bitWidth: Int,
  defaultRowGroupRowCount: Int,
  ordinal: Int,
  dataType: DataType) extends DataFiberBuilder {

  private val encoder = new RunLengthBitPackingHybridEncoder(bitWidth, 32, 1048576)

  override def appendInternal(row: InternalRow): Unit = {
    dataType match {
      case BooleanType => encoder.writeInt(if (row.getBoolean(ordinal)) 1 else 0)
      case IntegerType => encoder.writeInt(row.getInt(ordinal))
    }
  }

  override def appendNull(): Unit = {}

  override def build(): FiberByteData = {
    val rle = encoder.toBytes

    val bits = new Array[Byte](bitStream.toLongArray().length * 8)

    Platform.copyMemory(bitStream.toLongArray(), Platform.LONG_ARRAY_OFFSET,
      bits, Platform.BYTE_ARRAY_OFFSET, bitStream.toLongArray().length * 8)

    // TODO: is (Ints.checkedCast(rle.size)) really needed?
    val bytes = BytesInput.concat(BytesInput.from(bits),
      BytesInput.fromInt(Ints.checkedCast(rle.size)),
      rle).toByteArray

    FiberByteData(bytes)
  }

  override def getEncoding(): Encoding = Encoding.RLE

  override def clear(): Unit = {
    super.clear()
    encoder.reset()
  }
}

private[spinach] case class DeltaByteArrayFiberBuilder(
   defaultRowGroupRowCount: Int,
   ordinal: Int) extends DataFiberBuilder {

  private val suffixWriter = new DeltaLengthByteArrayValuesWriter(32, 1048576)
  private val prefixLengthWriter = new DeltaBinaryPackingValuesWriter(128, 4, 32, 1048576)
  private var previous = new Array[Byte](0)
  private var totalStringDataLengthInByte: Int = 0

  override def getEncoding(): Encoding = Encoding.DELTA_BYTE_ARRAY
  override def appendNull(): Unit = super.appendNull()

  override def appendInternal(row: InternalRow): Unit = {

    val v = Binary.fromConstantByteArray(row.getUTF8String(ordinal).getBytes)
    val vb = v.getBytes
    val length = if (previous.length < vb.length) previous.length else vb.length
    var i = 0
    while ( i < length && previous(i) == vb(i) ) { i = i + 1 }
    prefixLengthWriter.writeInteger(i)
    suffixWriter.writeBytes(v.slice(i, vb.length - i))
    previous = vb

    totalStringDataLengthInByte += vb.length
  }

  override def build(): FiberByteData = {

    val bits = new Array[Byte](bitStream.toLongArray().length * 8)

    Platform.copyMemory(bitStream.toLongArray(), Platform.LONG_ARRAY_OFFSET,
      bits, Platform.BYTE_ARRAY_OFFSET, bitStream.toLongArray().length * 8)

    val bytes = BytesInput.concat(BytesInput.from(bits),
      BytesInput.fromInt(totalStringDataLengthInByte),
      prefixLengthWriter.getBytes, suffixWriter.getBytes).toByteArray

    FiberByteData(bytes)
  }

  override def clear(): Unit = {
    prefixLengthWriter.reset()
    suffixWriter.reset()
    previous = new Array[Byte](0)
    totalStringDataLengthInByte = 0
  }
}

private[spinach] case class PlainBinaryDictionaryFiberBuilder(
  defaultRowGroupRowCount: Int,
  ordinal: Int) extends DataFiberBuilder {

  private var totalStringDataLengthInByte: Int = 0
  private val valueWriter = new PlainBinaryDictionaryValuesWriter(1048576,
    org.apache.parquet.column.Encoding.PLAIN_DICTIONARY,
    org.apache.parquet.column.Encoding.PLAIN_DICTIONARY)

  override protected def appendInternal(row: InternalRow): Unit = {
    val v = Binary.fromConstantByteArray(row.getUTF8String(ordinal).getBytes)
    val vb = v.getBytes
    valueWriter.writeBytes(v)

    totalStringDataLengthInByte += vb.length
  }

  override def getEncoding(): Encoding = Encoding.PLAIN_DICTIONARY

  override def appendNull(): Unit = super.appendNull()

  override def build(): FiberByteData = {
    val bits = new Array[Byte](bitStream.toLongArray().length * 8)

    Platform.copyMemory(bitStream.toLongArray(), Platform.LONG_ARRAY_OFFSET,
      bits, Platform.BYTE_ARRAY_OFFSET, bitStream.toLongArray().length * 8)

    val bytes = BytesInput.concat(BytesInput.from(bits),
      BytesInput.fromInt(totalStringDataLengthInByte),
      valueWriter.getBytes).toByteArray

    FiberByteData(bytes)
  }

  override def buildDictionary(): Array[Byte] = {
    valueWriter.createDictionaryPage().getBytes.toByteArray
  }

  override def getDictionarySize: Int = {
    valueWriter.getDictionarySize
  }
  override def clear(): Unit = {
    valueWriter.reset()
  }
}

object DataFiberBuilder {
  def apply(dataType: DataType, ordinal: Int, defaultRowGroupRowCount: Int): DataFiberBuilder = {
    dataType match {
      case BinaryType =>
        new BinaryFiberBuilder(defaultRowGroupRowCount, ordinal)
      case BooleanType =>
        new RunLengthBitPackingHybridFiberBuilder(1, defaultRowGroupRowCount, ordinal, BooleanType)
      case ByteType =>
        new FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, ByteType)
      case DateType =>
        new FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, DateType)
      case DoubleType =>
        new FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, DoubleType)
      case FloatType =>
        new FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, FloatType)
      case IntegerType =>
        new FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, IntegerType)
      case LongType =>
        new FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, LongType)
      case ShortType =>
        new FixedSizeTypeFiberBuilder(defaultRowGroupRowCount, ordinal, ShortType)
      case StringType =>
        if (true) { // TODO: add statistics condition
          new PlainBinaryDictionaryFiberBuilder(defaultRowGroupRowCount, ordinal)
        } else {
          new DeltaByteArrayFiberBuilder(defaultRowGroupRowCount, ordinal)
        }
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
