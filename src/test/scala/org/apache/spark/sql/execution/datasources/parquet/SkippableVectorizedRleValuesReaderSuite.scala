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

import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging

class SkippableVectorizedRleValuesReaderSuite extends SparkFunSuite with Logging {

  test("bit packing read and skip Integer") {
    // prepare data
    val writer = new RunLengthBitPackingHybridValuesWriter(3, 5, 10)
    (0 until 100).foreach(i => writer.writeInteger(i % 3))

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedRleValuesReader(3)
    reader.initFromPage(100, data, 0)

    // test skip and read Integer
    reader.skipIntegers(32)
    assert(reader.readInteger() == 2)
    reader.skipInteger()
    assert(reader.readInteger() == 1)
  }

  test("rle read and skip Integer") {
    // prepare data
    val writer = new RunLengthBitPackingHybridValuesWriter(3, 5, 10)
    (0 until 10).foreach(_ => writer.writeInteger(4))
    (0 until 10).foreach(_ => writer.writeInteger(5))

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedRleValuesReader(3)
    reader.initFromPage(20, data, 0)

    // test skip and read Integer
    reader.skipIntegers(8)
    assert(reader.readInteger() == 4)
    reader.skipInteger()
    assert(reader.readInteger() == 5)
  }

  test("rle read and skip Boolean") {
    // prepare data
    val writer = new RunLengthBitPackingHybridValuesWriter(3, 5, 10)
    (0 until 10).foreach(_ => writer.writeBoolean(true))
    (0 until 10).foreach(_ => writer.writeBoolean(false))

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedRleValuesReader(3)
    reader.initFromPage(20, data, 0)

    // test skip and read boolean
    reader.skipBoolean()
    assert(reader.readBoolean())
    (0 until 10).foreach(_ => reader.skipBoolean())
    assert(!reader.readBoolean())
  }


  test("not support skip other type") {

    // init reader
    val data = Array.emptyByteArray
    val reader = new SkippableVectorizedRleValuesReader()
    reader.initFromPage(0, data, 0)

    // skipByte should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipByte()
    }

    // skipLong should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipLong()
    }

    // skipFloat should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipFloat()
    }

    // skipDouble should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipDouble()
    }

    // skipBinaryByLen should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipBinaryByLen(1)
    }

    // skipBooleans should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipBooleans(1)
    }

    // skipBytes should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipBytes(1)
    }

    // skipLongs should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipLongs(1)
    }

    // skipFloats should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipFloats(1)
    }

    // skipDoubles should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipDoubles(1)
    }

    // skipBinary should throw UnsupportedOperationException
    intercept[UnsupportedOperationException] {
      reader.skipBinary(1)
    }
  }
}
