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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.column.values.plain.{BooleanPlainValuesWriter, PlainValuesWriter}
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter
import org.apache.parquet.io.api.Binary

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.execution.vectorized.ColumnVector
import org.apache.spark.sql.types.BinaryType

class CombinedValuesReaderSuite extends SparkFunSuite with Logging{
  test("read and skip Integers") {

    // prepare data: [null, null, 1, null, null, null, 2, null, 3, 4]
    // def: [0, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    // value: [1, 2, 3, 4]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (1 until 5).foreach(valueWriter.writeInteger)
    val valueReader = new OapVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    defReader.skipIntegers(9, 1, valueReader)

    assert(valueReader.readInteger() == 4)
  }

  test("read and skip Booleans") {

    // prepare data: [null, null, true, null, null, null, false, null, false, true]
    // def: [0, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    // value: [true, false, false, true]
    val valueWriter = new BooleanPlainValuesWriter()
    valueWriter.writeBoolean(true)
    valueWriter.writeBoolean(false)
    valueWriter.writeBoolean(false)
    valueWriter.writeBoolean(true)
    val valueReader = new OapVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    defReader.skipBooleans(6, 1, valueReader)

    assert(!valueReader.readBoolean())
  }

  test("read and skip skipBytes") {

    // prepare data: [null, null, A, null, null, null, B, null, C, D]
    // def: [0, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    // value: value: [A, B, C, D]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    "ABCD".getBytes.foreach { v =>
      valueWriter.writeByte(v)
      valueWriter.writeByte(0)
      valueWriter.writeByte(0)
      valueWriter.writeByte(0)
    }


    val valueReader = new OapVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    defReader.skipBytes(8, 1, valueReader)

    assert(valueReader.readByte() == 'C'.toInt)
  }

  test("read and skip Shorts") {

    // actually short store as int
    // prepare data: [null, null, 1, null, null, null, 2, null, 3, 4]
    // def: [0, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    // value: [1, 2, 3, 4]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (1 until 5).foreach(valueWriter.writeInteger)
    val valueReader = new OapVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    defReader.skipShorts(9, 1, valueReader)

    assert(valueReader.readInteger().toShort == 4.toShort)
  }

  test("read and skip Longs") {

    // prepare data: [null, null, 1L, null, null, null, 2L, null, 3L, 4L]
    // def: [0, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    // value: [1L, 2L, 3L, 4L]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (1 until 5).foreach(v => valueWriter.writeLong(v.toLong))
    val valueReader = new OapVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    defReader.skipLongs(2, 1, valueReader)

    assert(valueReader.readLong() == 1L)
  }

  test("read and skip Floats") {

    // prepare data: [null, null, 1.0F, null, null, null, 2.0F, null, 3.0F, 4.0F]
    // def: [0, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    // value: [1.0F, 2.0F, 3.0F, 4.0F]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (1 until 5).foreach(v => valueWriter.writeFloat(v.toFloat))
    val valueReader = new OapVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    defReader.skipFloats(2, 1, valueReader)

    assert(valueReader.readFloat() == 1.0F)
  }

  test("read and skip Doubles") {

    // prepare data: [null, null, 1.0D, null, null, null, 2.0D, null, 3.0D, 4.0D]
    // def: [0, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    // value: [1.0D, 2.0D, 3.0D, 4.0D]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (1 until 5).foreach(v => valueWriter.writeDouble(v.toDouble))
    val valueReader = new OapVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    defReader.skipDoubles(9, 1, valueReader)

    assert(valueReader.readDouble() == 4.0D)
  }

  test("read and skip Binarys") {

    // prepare data: [null, null, AB, null, null, null, CDE, null, F, GHI]
    // def: [0, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    // value: [AB, CDE, F, GHI]

    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    valueWriter.writeBytes(Binary.fromString("AB"))
    valueWriter.writeBytes(Binary.fromString("CDE"))
    valueWriter.writeBytes(Binary.fromString("F"))
    valueWriter.writeBytes(Binary.fromString("GHI"))

    val valueReader = new OapVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    defReader.skipBinarys(9, 1, valueReader)

    val vector = ColumnVector.allocate(10, BinaryType, MemoryMode.ON_HEAP)
    valueReader.readBinary(1, vector, 0)
    assert(vector.getBinary(0).sameElements("GHI".getBytes))
  }

  private def defReader: OapVectorizedRleValuesReader = {
    val defWriter = new RunLengthBitPackingHybridValuesWriter(3, 5, 10)
    Array(0, 0, 1, 0, 0, 0, 1, 0, 1, 1).foreach(defWriter.writeInteger)
    val defData = defWriter.getBytes.toByteArray
    val defReader = new OapVectorizedRleValuesReader(3)
    defReader.initFromPage(10, defData, 0)
    defReader
  }
}
