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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable.ArrayBuffer

private[spinach] trait FiberBuilder {
  def defaultRowGroupSize: Int
  def ordinal: Int

  protected val bitStream: BitSet = new BitSet(defaultRowGroupSize)
  protected var currentRowId: Int = 0

  def append(row: InternalRow): Unit = {
    require(currentRowId < defaultRowGroupSize, "fiber data overflow")
    if (!row.isNullAt(ordinal)) {
      bitStream.set(currentRowId)
    }
    appendInternal(row)
    currentRowId += 1
  }

  protected def appendInternal(row: InternalRow)
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
}

private[spinach] case class IntFiberBuilder(defaultRowGroupSize: Int, ordinal: Int)
    extends FiberBuilder {
  private val baseOffset = bitStream.toLongArray().length * 8 + Platform.BYTE_ARRAY_OFFSET
  // TODO use the memory pool?
  private val bytes = new Array[Byte](bitStream.toLongArray().length * 8 + defaultRowGroupSize * 4)

  protected def appendInternal(row: InternalRow): Unit = {
    if (!row.isNullAt(ordinal)) {
      Platform.putInt(bytes, baseOffset + 4 * currentRowId, row.getInt(ordinal))
    }
  }

  //  Field       Length in Byte            Description
  //  BitStream   defaultRowGroupSize / 8   To represent if the value is null
  //  value #1    4
  //  value #2    4
  //  …
  //  value #N    4
  def build(): FiberByteData = {
    fillBitStream(bytes)
    if (currentRowId == defaultRowGroupSize) {
      FiberByteData(bytes)
    } else {
      // shrink memory
      val newBytes = new Array[Byte](bitStream.toLongArray().length * 8 + currentRowId * 4)
      Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET,
        newBytes, Platform.BYTE_ARRAY_OFFSET, newBytes.length)
      FiberByteData(newBytes)
    }
  }
}

case class StringFiberBuilder(defaultRowGroupSize: Int, ordinal: Int) extends FiberBuilder {
  private val strings: ArrayBuffer[UTF8String] = new ArrayBuffer[UTF8String](defaultRowGroupSize)
  private var totalStringDataLengthInByte: Int = 0

  override def appendInternal(row: InternalRow): Unit = {
    val s = row.getUTF8String(ordinal)
    if (null != s) {
      totalStringDataLengthInByte += s.numBytes()
    }
    strings.append(row.getUTF8String(ordinal))
  }

  //  Field                 Size In Byte      Description
  //  BitStream             (defaultRowGroupSize / 8)  TODO to improve the memory usage
  //  value #1 length       4                 number of bytes for this string
  //  value #1 offset       4                 (0 - based to the start of this Fiber Group)
  //  value #2 length       4
  //  value #2 offset       4                 (0 - based to the start of this Fiber Group)
  //  …
  //  …
  //  value #N length       4
  //  value #N offset       4                 (0 - based to the start of this Fiber Group)
  //  value #1              value #1 length
  //  value #2              value #2 length
  //  …
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
        Platform.putInt(bytes, basePointerOffset + i * 8 + 4, startValueOffset)
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

object FiberBuilder {
  def apply(dataType: DataType, ordinal: Int, defaultRowGroupSize: Int): FiberBuilder = {
    dataType match {
      case IntegerType => new IntFiberBuilder(defaultRowGroupSize, ordinal)
      case StringType => new StringFiberBuilder(defaultRowGroupSize, ordinal)
    }
  }

  def initializeFromSchema(schema: StructType, defaultRowGroupSize: Int): Array[FiberBuilder] = {
    schema.fields.zipWithIndex.map {
      case (field, oridinal) => FiberBuilder(field.dataType, oridinal, defaultRowGroupSize)
    }
  }
}
