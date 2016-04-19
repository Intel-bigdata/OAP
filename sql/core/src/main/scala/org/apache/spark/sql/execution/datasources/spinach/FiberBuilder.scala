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
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable.ArrayBuffer

/**
  * Created by shimingfei on 4/12/16.
  */

abstract class FiberBuilder(final val FIBER_SIZE: Int) {
  var records: Int = 0
  val bitStream: BitSet = new BitSet(FIBER_SIZE)
  def append(row: InternalRow, ordinal: Int): Unit
  def build(): Array[(Array[Byte], Int)]
  def clear(): Unit = {
    records = 0
    bitStream.clear()
  }
}

class IntFiberBuilder(fiberSize: Int) extends FiberBuilder(fiberSize) {
  val fiberData = new Array[Byte](FIBER_SIZE * 4)

  def append(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      bitStream.set(records)
      Platform.copyMemory(row.getInt(ordinal), 0, fiberData,
        Platform.BYTE_ARRAY_OFFSET + records * 4, 4)
    } else {
      Platform.copyMemory(Int.MinValue, 0, fiberData,
        Platform.BYTE_ARRAY_OFFSET + records * 4, 4)
    }
    records += 1
  }

  def build(): Array[(Array[Byte], Int)] = {
    return Array((fiberData, records * 4))
  }
}

class StringFiberBuilder(fiberSize: Int) extends FiberBuilder(fiberSize) {
  val fiberDataMeta = new Array[Byte](FIBER_SIZE * 8)
  val fiberDataBuf = new ArrayBuffer[(Array[Byte], Int)]()
  var size: Int = 0
  var fiberData = new Array[Byte](FIBER_SIZE)
  var offset: Int = 0

  def append(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      bitStream.set(records)
      val value = row.getUTF8String(ordinal)
      val length = value.numBytes()
      if (offset + length > fiberData.size) {
        fiberDataBuf += ((fiberData, offset))
        fiberData = new Array[Byte](FIBER_SIZE)
        offset = 0
      }
      value.writeToMemory(fiberData, Platform.BYTE_ARRAY_OFFSET + offset)
      Platform.copyMemory(size, 0, fiberData, Platform.BYTE_ARRAY_OFFSET + records * 8, 4)
      Platform.copyMemory(length, 0, fiberData, Platform.BYTE_ARRAY_OFFSET + records * 8 + 4, 4)
      size += length
      offset += length
    } else {
      Platform.copyMemory(0, 0, fiberData, Platform.BYTE_ARRAY_OFFSET + records * 8, 4)
      Platform.copyMemory(0, 0, fiberData, Platform.BYTE_ARRAY_OFFSET + records * 8 + 4, 4)
    }
    records += 1
  }

  def build(): Array[(Array[Byte], Int)] = {
    fiberDataBuf += ((fiberData, offset))
    fiberDataBuf.toArray
  }

  override def clear(): Unit = {
    super.clear()
    size = 0
    fiberDataBuf.clear()
    offset = 0
  }
}

object FiberBuilder {
  def apply(dataType: DataType, size: Int): FiberBuilder = {
    dataType match {
      case IntegerType => new IntFiberBuilder(size)
      case StringType => new StringFiberBuilder(size)
    }
  }

  def initializeFromSchema(schema: StructType, defaultRowGroupSize: Int): Array[FiberBuilder] = {
    schema.fields.map(field => FiberBuilder(field.dataType, defaultRowGroupSize))
  }
}
