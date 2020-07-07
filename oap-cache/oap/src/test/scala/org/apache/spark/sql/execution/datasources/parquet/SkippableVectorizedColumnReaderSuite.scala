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

import java.io.File
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.Random

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{NanoTime, SimpleGroupFactory}
import org.apache.parquet.hadoop.OapParquetFileReader
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition.REQUIRED
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.util.Utils

class SkippableVectorizedColumnReaderSuite extends SparkFunSuite with SharedOapContext
  with BeforeAndAfterEach with Logging {

  private val fileDir: File = Utils.createTempDir()

  private val fileName: String = Utils.tempFileWith(fileDir).getAbsolutePath

  private val unitSize: Int = 4096

  override def beforeEach(): Unit = {
    configuration.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key,
      SQLConf.PARQUET_BINARY_AS_STRING.defaultValue.get)
    configuration.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValue.get)
    configuration.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get)
    // SQLConf.PARQUET_INT64_AS_TIMESTAMP_MILLIS is defined in Spark 2.2 and later
    configuration.setBoolean("spark.sql.parquet.int64AsTimestampMillis", false)
  }

  override def afterEach(): Unit = {
    configuration.unset(SQLConf.PARQUET_BINARY_AS_STRING.key)
    configuration.unset(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key)
    configuration.unset(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key)
    configuration.unset("spark.sql.parquet.int64AsTimestampMillis")
    val path = new Path(fileName)
    val fs = path.getFileSystem(configuration)
    if (fs.exists(path.getParent)) {
      fs.delete(path.getParent, true)
    }
  }

  test("skip And read booleans") {
    // write parquet data, type is boolean
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, BOOLEAN, "boolean_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("boolean_field", i % 2 == 0)
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector by BooleanType
    val columnVector = skipAndReadToVector(parquetSchema, BooleanType)

    // assert result
    (0 until unitSize).foreach { i =>
      assert(columnVector.getBoolean(i) == (i % 2 == 0))
    }
  }

  test("skip booleans and throw UnsupportedOperationException") {
    // write parquet data, type is boolean
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, BOOLEAN, "boolean_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("boolean_field", i % 2 == 0)
      )
    }
    writeData(parquetSchema, data)

    // skip with wrong type
    intercept[UnsupportedOperationException] {
      skipAndThrowUnsupportedOperation(parquetSchema, IntegerType)
    }
  }

  test("skip And read int32") {
    // write parquet data, type is int32
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, INT32, "int32_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup().append("int32_field", i))
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector use IntegerType
    val integerTypeVector = skipAndReadToVector(parquetSchema, IntegerType)

    // assert result
    (0 until unitSize).foreach { i =>
      assert(integerTypeVector.getInt(i) == i + unitSize)
    }

    // skip and read data to ColumnVector use DateType
    val dateTypeVector = skipAndReadToVector(parquetSchema, DateType)

    // assert result, DateType read as int32
    (0 until unitSize).foreach { i =>
      assert(dateTypeVector.getInt(i) == i + unitSize)
    }

    // skip and read data to ColumnVector use ShortType
    val shortTypeVector = skipAndReadToVector(parquetSchema, ShortType)

    // assert result, ShortType read as int32
    (0 until unitSize).foreach { i =>
      assert(shortTypeVector.getShort(i) == (i + unitSize).toShort)
    }

    // skip and read data to ColumnVector use ByteType
    val byteTypeVector = skipAndReadToVector(parquetSchema, ByteType)

    // assert result, ByteType read as int32
    (0 until unitSize).foreach { i =>
      assert(byteTypeVector.getByte(i) == (i + unitSize).toByte)
    }

    // skip and read data to ColumnVector use DecimalType(8, 0)
    val precision = 8
    val scale = 0
    val intDecimalVector = skipAndReadToVector(parquetSchema, DecimalType(precision, scale))

    // assert result, DecimalType read as int32
    (0 until unitSize).foreach { i =>
      val actual = intDecimalVector.getDecimal(i, precision, scale)
      val excepted = Decimal.createUnsafe(i + unitSize, precision, scale)
      assert(actual.equals(excepted))
    }
  }

  /**
   *  Similar to "skip And read int32" but data have dictionary.
   */
  test("skip And read int32 with dic") {
    // write parquet data, type is int32 and with dic
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, INT32, "int32_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i =>
        if (i < unitSize) factory.newGroup().append("int32_field", 1)
        else factory.newGroup().append("int32_field", 2)
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector use IntegerType
    val integerTypeVector = skipAndReadToVector(parquetSchema, IntegerType)

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = integerTypeVector.getInt(i)
      val excepted = 2
      assert(actual == excepted)
    }

    // skip and read data to ColumnVector use DateType
    val dateTypeVector = skipAndReadToVector(parquetSchema, DateType)

    // assert result, DateType read as int32
    (0 until unitSize).foreach { i =>
      assert(dateTypeVector.getInt(i) == 2)
    }

    // skip and read data to ColumnVector use ShortType
    val shortTypeVector = skipAndReadToVector(parquetSchema, ShortType)

    // assert result, ShortType read as int32
    (0 until unitSize).foreach { i =>
      assert(shortTypeVector.getShort(i) == 2.toShort)
    }

    // skip and read data to ColumnVector use ByteType
    val byteTypeVector = skipAndReadToVector(parquetSchema, ByteType)

    // assert result, ByteType read as int32
    (0 until unitSize).foreach { i =>
      assert(byteTypeVector.getByte(i) == 2.toByte)
    }


    // skip and read data to ColumnVector use DecimalType(8, 0)
    val precision = 8
    val scale = 0
    val intDecimalVector = skipAndReadToVector(parquetSchema, DecimalType(precision, scale))

    // assert result, DecimalType read as int32
    (0 until unitSize).foreach { i =>
      val actual = intDecimalVector.getDecimal(i, precision, scale)
      val excepted = Decimal.createUnsafe(2, precision, scale)
      assert(actual.equals(excepted))
    }
  }

  test("skip int32 and throw UnsupportedOperation") {
    // write parquet data, type is int
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, INT32, "int32_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup().append("int32_field", i))
    }
    writeData(parquetSchema, data)

    // skip with wrong type
    intercept[UnsupportedOperationException] {
      skipAndThrowUnsupportedOperation(parquetSchema, BooleanType)
    }

    // skip with wrong type, precision > Decimal.MAX_INT_DIGITS
    val precision = 10
    val scale = 0
    intercept[UnsupportedOperationException] {
      skipAndThrowUnsupportedOperation(parquetSchema, DecimalType(precision, scale))
    }
  }

  test("skip And read float") {
    // write parquet data, type is float
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, FLOAT, "float_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup().append("float_field", i.toFloat))
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector by FloatType
    val columnVector = skipAndReadToVector(parquetSchema, FloatType)

    // assert result
    (0 until unitSize).foreach { i =>
      assert(columnVector.getFloat(i) == (i + unitSize).toFloat)
    }
  }

  /**
   * Similar to "skip And read float" but data have dictionary.
   */
  test("skip And read float with dic") {
    // write parquet data, type is float and with dic
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, FLOAT, "float_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i =>
        if (i < unitSize) factory.newGroup().append("float_field", 1F)
        else factory.newGroup().append("float_field", 2F)
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, FloatType)

    // assert result
    (0 until unitSize).foreach { i =>
      assert(columnVector.getFloat(i) == 2F)
    }
  }

  test("skip float and throw UnsupportedOperation") {
    // write parquet data, type is float
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, FLOAT, "float_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("float_field", i.toFloat)
      )
    }
    writeData(parquetSchema, data)

    // skip with wrong type
    intercept[UnsupportedOperationException] {
      skipAndThrowUnsupportedOperation(parquetSchema, BooleanType)
    }
  }

  test("skip And read double") {
    // write parquet data, type is double
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, DOUBLE, "double_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("double_field", i.toDouble)
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector by DoubleType
    val columnVector = skipAndReadToVector(parquetSchema, DoubleType)

    // assert result
    (0 until unitSize).foreach { i =>
      assert(columnVector.getDouble(i) == (i + unitSize).toDouble)
    }
  }

  /**
   * Similar to "skip And read double" but data have dictionary.
   */
  test("skip And read double with dic") {
    // write parquet data, type is double and with dic
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, DOUBLE, "double_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i =>
        if (i < unitSize) factory.newGroup().append("double_field", 1D)
        else factory.newGroup().append("double_field", 2D)
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, DoubleType)

    // assert result
    (0 until unitSize).foreach { i =>
      assert(columnVector.getDouble(i) == 2D)
    }
  }

  test("skip double and throw UnsupportedOperation") {
    // write parquet data, type is double
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, DOUBLE, "double_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("double_field", i.toDouble)
      )
    }
    writeData(parquetSchema, data)

    // skip with wrong type
    intercept[UnsupportedOperationException] {
      skipAndThrowUnsupportedOperation(parquetSchema, BooleanType)
    }
  }

  test("skip And read int64") {
    // write parquet data, type is int64
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, INT64, "int64_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("int64_field", i.toLong)
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector by LongType
    val longTypeVector = skipAndReadToVector(parquetSchema, LongType)

    // assert result
    (0 until unitSize).foreach { i =>
      assert(longTypeVector.getLong(i) == (i + unitSize).toLong)
    }

    // skip and read data to ColumnVector by DecimalType
    val precision = 18
    val scale = 0
    val decimalTypeVector = skipAndReadToVector(parquetSchema, DecimalType(precision, scale))

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = decimalTypeVector.getDecimal(i, precision, scale)
      val excepted = Decimal.createUnsafe(i + unitSize, precision, scale)
      assert(actual.equals(excepted))
    }
  }

  /**
   * Similar to "skip And read int64" but data have dictionary.
   */
  test("skip And read int64 with dic") {
    // write parquet data, type is int64 and with dic
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, INT64, "int64_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i =>
        if (i < unitSize) factory.newGroup().append("int64_field", 1L)
        else factory.newGroup().append("int64_field", 2L)
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector by LongType
    val longTypeVector = skipAndReadToVector(parquetSchema, LongType)

    // assert result
    (0 until unitSize).foreach { i =>
      assert(longTypeVector.getLong(i) == 2L)
    }

    // skip and read data to ColumnVector by DecimalType
    val precision = 18
    val scale = 0
    val decimalTypeVector = skipAndReadToVector(parquetSchema, DecimalType(precision, scale))

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = decimalTypeVector.getDecimal(i, precision, scale)
      val excepted = Decimal.createUnsafe(2, precision, scale)
      assert(actual.equals(excepted))
    }
  }

  test("skip int64 and throw UnsupportedOperation") {
    // write parquet data, type is int64
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, INT64, "int64_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("int64_field", i.toLong)
      )
    }
    writeData(parquetSchema, data)

    // skip with wrong type
    intercept[UnsupportedOperationException] {
      skipAndThrowUnsupportedOperation(parquetSchema, BooleanType)
    }

    // skip with wrong type, 20 > Decimal.MAX_LONG_DIGITS
    val precision = 20
    val scale = 0
    intercept[UnsupportedOperationException] {
      skipAndThrowUnsupportedOperation(parquetSchema, DecimalType(precision, scale))
    }
  }

  /**
   * Similar to "skip And read int96" but data have dictionary.
   */
  test("skip And read int96 with dic") {
    // write parquet data, type is int96, actually int96 always has dic ...
    val times = new Array[NanoTime](unitSize *2)
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, INT96, "int96_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => {
        val time = new NanoTime(i, i.toLong)
        times(i) = time
        factory.newGroup().append("int96_field", time)
      })
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector by TimestampType
    val columnVector = skipAndReadToVector(parquetSchema, TimestampType)

    val fromZone = ZoneId.systemDefault
    val toZone: ZoneId = ZoneOffset.UTC

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getLong(i)
      val rawTime = ParquetRowConverter.binaryToSQLTimestamp(times(i + unitSize).toBinary)
      val excepted = DateTimeUtils.convertTz(rawTime, fromZone, toZone)
      assert(actual == excepted)
    }
  }

  test("skip And read string") {
    // write parquet data, type is string
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, BINARY, "string_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("string_field", String.valueOf(i))
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, BinaryType)

    // assert result
    (0 until unitSize).foreach { i =>
      assert(columnVector.getUTF8String(i).toString == String.valueOf(i + unitSize))
    }
  }

  /**
   * Similar to "skip And read string" but data have dictionary.
   */
  test("skip And read string with dic") {
    // write parquet data, type is string and with dic
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, BINARY, "string_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i =>
        if (i < unitSize) factory.newGroup().append("string_field", String.valueOf(1))
        else factory.newGroup().append("string_field", String.valueOf(2))
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, BinaryType)

    // assert result
    (0 until unitSize).foreach { i =>
      assert(columnVector.getUTF8String(i).toString == String.valueOf(2))
    }
  }

  test("skip string and throw UnsupportedOperation") {
    // write parquet data, type is string
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, BINARY, "string_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("string_field", String.valueOf(i))
      )
    }
    writeData(parquetSchema, data)

    // skip with wrong type
    intercept[UnsupportedOperationException] {
      skipAndThrowUnsupportedOperation(parquetSchema, BooleanType)
    }
  }

  test("skip And read binary") {
    // write parquet data, type is binary
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, BINARY, "binary_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("binary_field", Binary.fromCharSequence(String.valueOf(i)))
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, BinaryType)

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getBinary(i)
      val excepted = String.valueOf(i + unitSize).getBytes
      assert(actual.sameElements(excepted))
    }
  }

  /**
   * Similar to "skip And read binary" but data have dictionary.
   */
  test("skip And read binary with dic") {
    // write parquet data, type is binary and with dic
    val v1 = Binary.fromCharSequence("1")
    val v2 = Binary.fromCharSequence("2")
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, BINARY, "binary_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i =>
        if (i < unitSize) factory.newGroup().append("binary_field", v1)
        else factory.newGroup().append("binary_field", v2)
      )
    }
    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, BinaryType)

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getBinary(i)
      val excepted = v2.getBytes
      assert(actual.sameElements(excepted))
    }
  }

  test("skip binary and throw UnsupportedOperation") {
    // write parquet data, type is binary
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, BINARY, "binary_field")
    )
    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize * 2).map(i => factory.newGroup()
        .append("binary_field", Binary.fromCharSequence(String.valueOf(i)))
      )
    }
    writeData(parquetSchema, data)

    // skip with wrong type
    intercept[UnsupportedOperationException] {
      skipAndThrowUnsupportedOperation(parquetSchema, BooleanType)
    }
  }

  /**
   * length <= Decimal.MAX_INT_DIGITS, store in ColumnVector as int
   */
  test("skip And read fixed length byte array 8 bytes") {
    // write parquet data, type is FIXED_LEN_BYTE_ARRAY
    val length = 8
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, length, "binary_field")
    )
    val binaryDataArray = new Array[Binary](unitSize * 2)
    val dataGenerator = FixedLengthDataGenerator(length)
    for(i <- binaryDataArray.indices) {
      binaryDataArray(i) = dataGenerator.nextValue
    }

    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      binaryDataArray.map(v => factory.newGroup().append("binary_field", v))
    }

    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, DecimalType(length, 0))

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getInt(i)
      val excepted = ParquetRowConverter.binaryToUnscaledLong(binaryDataArray(i + unitSize)).toInt
      assert(actual == excepted)
    }
  }

  /**
   * Similar to "skip And read fixed length byte array 8 bytes" but data have dictionary.
   */
  test("skip And read fixed length byte array 8 bytes with dic") {
    // write parquet data, type is FIXED_LEN_BYTE_ARRAY
    val length = 8
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, length, "binary_field")
    )
    val dataGenerator = FixedLengthDataGenerator(length)
    val v1 = dataGenerator.nextValue
    val v2 = dataGenerator.nextValue

    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize *2).map(i => {
        if (i < unitSize) factory.newGroup().append("binary_field", v1)
        else factory.newGroup().append("binary_field", v2)
      })
    }

    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, DecimalType(length, 0))

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getInt(i)
      val excepted = ParquetRowConverter.binaryToUnscaledLong(v2).toInt
      assert(actual == excepted)
    }
  }

  /**
   * length <= Decimal.MAX_LONG_DIGITS, store in ColumnVector as long
   */
  test("skip And read fixed length byte array 16 bytes") {
    // write parquet data, type is FIXED_LEN_BYTE_ARRAY
    val length = 16
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, length, "binary_field")
    )
    val binaryDataArray = new Array[Binary](unitSize * 2)
    val dataGenerator = FixedLengthDataGenerator(length)
    for(i <- binaryDataArray.indices) {
      binaryDataArray(i) = dataGenerator.nextValue
    }

    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      binaryDataArray.map(v => factory.newGroup().append("binary_field", v))
    }

    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, DecimalType(length, 0))

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getLong(i)
      val excepted = ParquetRowConverter.binaryToUnscaledLong(binaryDataArray(i + unitSize))
      assert(actual == excepted)
    }
  }

  /**
   * Similar to "skip And read fixed length byte array 16 bytes" but data have dictionary.
   */
  test("skip And read fixed length byte array 16 bytes with dic") {
    // write parquet data, type is FIXED_LEN_BYTE_ARRAY
    val length = 16
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, length, "binary_field")
    )
    val dataGenerator = FixedLengthDataGenerator(length)
    val v1 = dataGenerator.nextValue
    val v2 = dataGenerator.nextValue

    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize *2).map(i => {
        if (i < unitSize) factory.newGroup().append("binary_field", v1)
        else factory.newGroup().append("binary_field", v2)
      })
    }

    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, DecimalType(length, 0))

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getLong(i)
      val excepted = ParquetRowConverter.binaryToUnscaledLong(v2)
      assert(actual == excepted)
    }
  }

  /**
   * length > Decimal.MAX_LONG_DIGITS, store in ColumnVector as ByteArray
   */
  test("skip And read fixed length byte array 20 bytes") {
    // write parquet data, type is FIXED_LEN_BYTE_ARRAY
    val length = 20
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, length, "binary_field")
    )
    val binaryDataArray = new Array[Binary](unitSize * 2)
    val dataGenerator = FixedLengthDataGenerator(length)
    for(i <- binaryDataArray.indices) {
      binaryDataArray(i) = dataGenerator.nextValue
    }

    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      binaryDataArray.map(v => factory.newGroup().append("binary_field", v))
    }

    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, DecimalType(length, 0))

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getBinary(i)
      val excepted = binaryDataArray(i + unitSize).getBytes
      assert(actual.sameElements(excepted))
    }
  }

  /**
   * Similar to "skip And read fixed length byte array 20 bytes" but data have dictionary.
   */
  test("skip And read fixed length byte array 20 bytes with dic") {
    // write parquet data, type is FIXED_LEN_BYTE_ARRAY
    val length = 20
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, length, "binary_field")
    )
    val dataGenerator = FixedLengthDataGenerator(length)
    val v1 = dataGenerator.nextValue
    val v2 = dataGenerator.nextValue

    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      (0 until unitSize *2).map(i => {
        if (i < unitSize) factory.newGroup().append("binary_field", v1)
        else factory.newGroup().append("binary_field", v2)
      })
    }

    writeData(parquetSchema, data)

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, DecimalType(length, 0))

    // assert result
    (0 until unitSize).foreach { i =>
      assert(columnVector.getBinary(i).sameElements(v2.getBytes))
    }
  }

  test("skip fixed length byte array 8 bytes throw UnsupportedOperation") {
    // write parquet data, type is FIXED_LEN_BYTE_ARRAY
    val length = 8
    val parquetSchema: MessageType = new MessageType("test",
      new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, length, "binary_field")
    )
    val binaryDataArray = new Array[Binary](unitSize * 2)
    val dataGenerator = FixedLengthDataGenerator(length)
    for(i <- binaryDataArray.indices) {
      binaryDataArray(i) = dataGenerator.nextValue
    }

    val data: Seq[Group] = {
      val factory = new SimpleGroupFactory(parquetSchema)
      binaryDataArray.map(v => factory.newGroup().append("binary_field", v))
    }

    writeData(parquetSchema, data)

    // skip with wrong type
    intercept[UnsupportedOperationException] {
      skipAndThrowUnsupportedOperation(parquetSchema, BooleanType)
    }
  }

  private def skipAndReadToVector(parquetSchema: MessageType, dataType: DataType): ColumnVector = {
    val footer = OapParquetFileReader
      .readParquetFooter(configuration, new Path(fileName)).toParquetMetadata
    var reader: OapParquetFileReader = null
    try {
      reader = OapParquetFileReader.open(configuration, new Path(fileName), footer)
      reader.setRequestedSchema(parquetSchema)
      val rowGroup = reader.readNextRowGroup()
      val descriptor = parquetSchema.getColumns.get(0)
      val originalType = parquetSchema.asGroupType().getFields.get(0).getOriginalType
      val pageReader = rowGroup.getPageReader(descriptor)
      val timeZone = ZoneId.systemDefault
      val columnReader =
        new SkippableVectorizedColumnReader(descriptor, originalType, pageReader, timeZone, "LEGACY")
      val columnVector = new OnHeapColumnVector(unitSize, dataType)
      columnReader.skipBatch(unitSize, columnVector.dataType)
      columnVector.reset()
      columnReader.readBatch(unitSize, columnVector)
      columnVector
    } finally {
      if (reader != null) reader.close()
    }

  }

  private def skipAndThrowUnsupportedOperation(
      parquetSchema: MessageType,
      dataType: DataType,
      isArray: Boolean = false): Unit = {
    val footer = OapParquetFileReader
      .readParquetFooter(configuration, new Path(fileName)).toParquetMetadata
    var reader: OapParquetFileReader = null
    try {
      reader = OapParquetFileReader.open(configuration, new Path(fileName), footer)
      reader.setRequestedSchema(parquetSchema)
      val rowGroup = reader.readNextRowGroup()
      val descriptor = parquetSchema.getColumns.get(0)
      val originalType = parquetSchema.asGroupType().getFields.get(0).getOriginalType
      val pageReader = rowGroup.getPageReader(descriptor)
      val timeZone = ZoneId.systemDefault
      val columnReader =
        new SkippableVectorizedColumnReader(descriptor, originalType, pageReader, timeZone, "LEGACY")
      columnReader.skipBatch(unitSize, dataType)
    } finally {
      if (reader != null) reader.close()
    }
  }

  private def writeData(writeSchema: MessageType, data: Seq[Group]): Unit = {
    GroupWriteSupport.setSchema(writeSchema, configuration)
    val writer = ExampleParquetWriter.builder(new Path(fileName))
      .withCompressionCodec(UNCOMPRESSED)
      .withDictionaryEncoding(true)
      .withValidation(false)
      .withWriterVersion(PARQUET_1_0)
      .withConf(configuration)
      .build()

    data.foreach(writer.write)
    writer.close()
  }
}

/**
 * Use to generate fixed length binary type data.
 * @param length fixed length
 */
case class FixedLengthDataGenerator(length: Int) {

  private val random = new Random()

  private def randomInt: Int = randomInt(Integer.MAX_VALUE - 1)

  private def randomInt(maximum: Int): Int = {
    random.nextInt(Math.abs(maximum) + 1)
  }

  def nextValue: Binary = {
    val buffer = new Array[Byte](length)
    for (i <- buffer.indices) {
      buffer(i) = randomInt.toByte
    }
    Binary.fromReusedByteArray(buffer)
  }
}
