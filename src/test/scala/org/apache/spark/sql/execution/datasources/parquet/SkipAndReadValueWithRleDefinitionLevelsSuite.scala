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

class SkipAndReadValueWithRleDefinitionLevelsSuite extends SparkFunSuite with Logging {

  test("read and skip Integers") {

    // prepare data:
    // [null, null, null, null, null, null, null, null, null, null, 1, 2, 3, 4]
    // value: [1, 2, 3, 4]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (1 until 5).foreach(valueWriter.writeInteger)

    // init value reader
    val valueReader = new SkippableVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    // skip data assisted by defReader
    val reader = defReader
    reader.skipIntegers(7, 1, valueReader)
    reader.skipIntegers(5, 1, valueReader)
    reader.skipIntegers(0, 1, valueReader)

    // assert read value
    assert(valueReader.readInteger() == 3)
  }

  test("read and skip Booleans") {

    // prepare data:
    // [null, null, null, null, null, null, null, null, null, null, true, false, false, true]
    // value: [true, false, false, true]
    val valueWriter = new BooleanPlainValuesWriter()
    valueWriter.writeBoolean(true)
    valueWriter.writeBoolean(false)
    valueWriter.writeBoolean(false)
    valueWriter.writeBoolean(true)

    // init value reader
    val valueReader = new SkippableVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    // skip data assisted by defReader
    val reader = defReader
    reader.skipBooleans(7, 1, valueReader)
    reader.skipBooleans(5, 1, valueReader)
    reader.skipBooleans(0, 1, valueReader)

    // assert read value
    assert(!valueReader.readBoolean())
  }

  test("read and skip skipBytes") {

    // prepare data:
    // [null, null, null, null, null, null, null, null, null, null, A, B, C, D]
    // value: value: [A, B, C, D]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    "ABCD".getBytes.foreach { v =>
      valueWriter.writeByte(v)
      valueWriter.writeByte(0)
      valueWriter.writeByte(0)
      valueWriter.writeByte(0)
    }
    // init value reader
    val valueReader = new SkippableVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    // skip data assisted by defReader
    val reader = defReader
    reader.skipBytes(7, 1, valueReader)
    reader.skipBytes(5, 1, valueReader)
    reader.skipBytes(0, 1, valueReader)

    // assert read value
    assert(valueReader.readByte() == 'C'.toInt)
  }

  test("read and skip Shorts") {

    // actually short store as int
    // prepare data:
    // [null, null, null, null, null, null, null, null, null, null, 1, 2, 3, 4]
    // value: [1, 2, 3, 4]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (1 until 5).foreach(valueWriter.writeInteger)
    // init value reader
    val valueReader = new SkippableVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    // skip data assisted by defReader
    val reader = defReader
    reader.skipShorts(7, 1, valueReader)
    reader.skipShorts(5, 1, valueReader)
    reader.skipShorts(0, 1, valueReader)

    // assert read value
    assert(valueReader.readInteger().toShort == 3.toShort)
  }

  test("read and skip Longs") {

    // prepare data: [null, null, null, null, null, null, null, null, null, null, 1L, 2L, 3L, 4L]
    // value: [1L, 2L, 3L, 4L]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (1 until 5).foreach(v => valueWriter.writeLong(v.toLong))

    // init value reader
    val valueReader = new SkippableVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    // skip data assisted by defReader
    val reader = defReader
    reader.skipLongs(7, 1, valueReader)
    reader.skipLongs(5, 1, valueReader)
    reader.skipLongs(0, 1, valueReader)

    // assert read value
    assert(valueReader.readLong() == 3L)
  }

  test("read and skip Floats") {

    // prepare data:
    // [null, null, null, null, null, null, null, null, null, null, 1.0F, 2.0F, 3.0F, 4.0F]
    // value: [1.0F, 2.0F, 3.0F, 4.0F]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (1 until 5).foreach(v => valueWriter.writeFloat(v.toFloat))

    // init value reader
    val valueReader = new SkippableVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    // skip data assisted by defReader
    val reader = defReader
    reader.skipFloats(7, 1, valueReader)
    reader.skipFloats(5, 1, valueReader)
    reader.skipFloats(0, 1, valueReader)

    // assert read value
    assert(valueReader.readFloat() == 3.0F)
  }

  test("read and skip Doubles") {

    // prepare data:
    // [null, null, null, null, null, null, null, null, null, null, 1.0D, 2.0D, 3.0D, 4.0D]
    // value: [1.0D, 2.0D, 3.0D, 4.0D]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (1 until 5).foreach(v => valueWriter.writeDouble(v.toDouble))

    // init value reader
    val valueReader = new SkippableVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    // skip data assisted by defReader
    val reader = defReader
    reader.skipDoubles(7, 1, valueReader)
    reader.skipDoubles(5, 1, valueReader)
    reader.skipDoubles(0, 1, valueReader)

    // assert read value
    assert(valueReader.readDouble() == 3.0D)
  }

  test("read and skip Binarys") {

    // prepare data:
    // [null, null, null, null, null, null, null, null, null, null, AB, CDE, F, GHI]
    // value: [AB, CDE, F, GHI]
    val valueWriter = new PlainValuesWriter(64 * 1024, 64 * 1024)
    valueWriter.writeBytes(Binary.fromString("AB"))
    valueWriter.writeBytes(Binary.fromString("CDE"))
    valueWriter.writeBytes(Binary.fromString("F"))
    valueWriter.writeBytes(Binary.fromString("GHI"))

    // init value reader
    val valueReader = new SkippableVectorizedPlainValuesReader()
    val valueData = valueWriter.getBytes.toByteArray
    valueReader.initFromPage(4, valueData, 0)

    // skip data assisted by defReader
    val reader = defReader
    reader.skipBinarys(7, 1, valueReader)
    reader.skipBinarys(5, 1, valueReader)
    reader.skipBinarys(0, 1, valueReader)

    // read binary to a vector and assert read value
    val vector = ColumnVector.allocate(10, BinaryType, MemoryMode.ON_HEAP)
    valueReader.readBinary(1, vector, 0)
    assert(vector.getBinary(0).sameElements("F".getBytes))
  }

  /**
   * For ut build a unified SkippableVectorizedRleValuesReader with data
   * [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1], reader will use RLE mode
   * @return SkippableVectorizedRleValuesReader  represent definition level values
   */
  private def defReader: SkippableVectorizedRleValuesReader = {
    val defWriter = new RunLengthBitPackingHybridValuesWriter(3, 5, 10)
    Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1).foreach(defWriter.writeInteger)
    val defData = defWriter.getBytes.toByteArray
    val defReader = new SkippableVectorizedRleValuesReader(3)
    defReader.initFromPage(10, defData, 0)
    defReader
  }
}
