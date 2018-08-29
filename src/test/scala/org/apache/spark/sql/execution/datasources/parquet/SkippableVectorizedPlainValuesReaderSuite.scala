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

import org.apache.parquet.column.values.plain._
import org.apache.parquet.io.api.Binary

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.execution.vectorized.ColumnVector
import org.apache.spark.sql.types.BinaryType

class SkippableVectorizedPlainValuesReaderSuite extends SparkFunSuite with Logging {
  test("read and skip Boolean") {
    // prepare data
    val writer = new BooleanPlainValuesWriter()
    writer.writeBoolean(true)
    writer.writeBoolean(true)
    writer.writeBoolean(false)
    writer.writeBoolean(true)
    writer.writeBoolean(false)
    writer.writeBoolean(false)
    writer.writeBoolean(true)
    writer.writeBoolean(true)
    writer.writeBoolean(false)

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedPlainValuesReader()
    reader.initFromPage(9, data, 0)

    // test skip and read boolean data
    reader.skipBooleans(2)
    assert(!reader.readBoolean())
    reader.skipBooleans(3)
    assert(reader.readBoolean())
    reader.skipBoolean()
    assert(!reader.readBoolean())
  }

  test("read and skip Integer") {
    // prepare data
    val writer = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (0 until 10).foreach(writer.writeInteger)

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedPlainValuesReader()
    reader.initFromPage(9, data, 0)

    // test skip and read boolean data
    reader.skipIntegers(2)
    assert(reader.readInteger() == 2)
    reader.skipIntegers(3)
    assert(reader.readInteger() == 6)
    reader.skipInteger()
    assert(reader.readInteger() == 8)
    assert(reader.readInteger() == 9)
  }


  test("read and skip Long") {
    // prepare data
    val writer = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (0 until 10).foreach(i => writer.writeLong(Int.int2long(i)))

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedPlainValuesReader()
    reader.initFromPage(9, data, 0)

    // test skip and read boolean data
    reader.skipLongs(2)
    assert(reader.readLong() == 2L)
    reader.skipLongs(3)
    assert(reader.readLong() == 6L)
    reader.skipLong()
    assert(reader.readLong() == 8L)
    assert(reader.readLong() == 9L)
  }

  test("read and skip Double") {
    // prepare data
    val writer = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (0 until 10).foreach(i => writer.writeDouble(Int.int2double(i)))

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedPlainValuesReader()
    reader.initFromPage(9, data, 0)

    // test skip and read boolean data
    reader.skipDoubles(2)
    assert(reader.readDouble() == 2.0D)
    reader.skipDoubles(3)
    assert(reader.readDouble() == 6.0D)
    reader.skipDouble()
    assert(reader.readDouble() == 8.0D)
    assert(reader.readDouble() == 9.0D)
  }

  test("read and skip Float") {
    // prepare data
    val writer = new PlainValuesWriter(64 * 1024, 64 * 1024)
    (0 until 10).foreach(i => writer.writeFloat(Int.int2float(i)))

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedPlainValuesReader()
    reader.initFromPage(9, data, 0)

    // test skip and read boolean data
    reader.skipFloats(2)
    assert(reader.readFloat() == 2.0F)
    reader.skipFloats(3)
    assert(reader.readFloat() == 6.0F)
    reader.skipFloat()
    assert(reader.readFloat() == 8.0F)
    assert(reader.readFloat() == 9.0F)
  }

  test("read and skip Bytes") {
    // prepare data
    val writer = new PlainValuesWriter(64 * 1024, 64 * 1024)
    // Bytes are stored as a 4-byte little endian int. Just read the first byte.
    // this comments from VectorizedPlainValuesReader.readBytes,
    // so for 1 byte we should write byte and 3 zero.
    "ABCDEFGHI".getBytes.foreach { v =>
      writer.writeByte(v)
      writer.writeByte(0)
      writer.writeByte(0)
      writer.writeByte(0)
    }

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedPlainValuesReader()
    reader.initFromPage(9, data, 0)

    // test skip and read boolean data
    reader.skipBytes(2)
    assert(reader.readByte() == 'C'.toInt)
    reader.skipBytes(3)
    assert(reader.readByte() == 'G'.toInt)
    reader.skipByte()
    assert(reader.readByte() == 'I'.toInt)
  }

  test("read and skip Binary") {
    // prepare data
    val writer = new PlainValuesWriter(64 * 1024, 64 * 1024)
    writer.writeBytes(Binary.fromString("AB"))
    writer.writeBytes(Binary.fromString("CDE"))
    writer.writeBytes(Binary.fromString("F"))
    writer.writeBytes(Binary.fromString("GHI"))
    writer.writeBytes(Binary.fromString("JK"))

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedPlainValuesReader()
    reader.initFromPage(5, data, 0)

    // test skip and read boolean data
    reader.skipBinary(2)
    val vector = ColumnVector.allocate(10, BinaryType, MemoryMode.ON_HEAP)
    reader.readBinary(3, vector, 0)
    assert(vector.getBinary(0).sameElements("F".getBytes))
    assert(vector.getBinary(1).sameElements("GHI".getBytes))
    assert(vector.getBinary(2).sameElements("JK".getBytes))
  }

  test("read and skip Binary By Len") {
    // prepare data
    val writer = new FixedLenByteArrayPlainValuesWriter(12, 64 * 1024, 64 * 1024)
    writer.writeBytes(Binary.fromString("012345678901"))
    writer.writeBytes(Binary.fromString("890101234567"))

    // init reader
    val data = writer.getBytes.toByteArray
    val reader = new SkippableVectorizedPlainValuesReader()
    reader.initFromPage(2, data, 0)

    // test skip and read boolean data
    reader.skipBinaryByLen(12)
    val binary = reader.readBinary(12)
    assert(binary.equals(Binary.fromString("890101234567")))
  }
}
