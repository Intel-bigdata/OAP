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

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.OapParquetFileReader
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition.REQUIRED
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.execution.vectorized.ColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StructField, StructType}
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

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, BooleanType)

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getBoolean(i)
      val excepted = i % 2 == 0
      assert(actual == excepted)
    }
  }

  test("skip And read longs") {
    // write parquet data, type is long
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

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, LongType)

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getLong(i)
      val excepted = (i + unitSize).toLong
      assert(actual == excepted)
    }
  }

  test("skip And read longs with dic") {
    // write parquet data, type is long and with dic
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

    // skip and read data to ColumnVector
    val columnVector = skipAndReadToVector(parquetSchema, LongType)

    // assert result
    (0 until unitSize).foreach { i =>
      val actual = columnVector.getLong(i)
      val excepted = 2L
      assert(actual == excepted)
    }
  }

  private def skipAndReadToVector(parquetSchema: MessageType, dataType: DataType): ColumnVector = {
    val footer = OapParquetFileReader
      .readParquetFooter(configuration, new Path(fileName)).toParquetMetadata
    val reader = OapParquetFileReader.open(configuration, new Path(fileName), footer)
    reader.setRequestedSchema(parquetSchema)
    val rowGroup = reader.readNextRowGroup()
    val columnDescriptor = parquetSchema.getColumns.get(0)
    val pageReader = rowGroup.getPageReader(columnDescriptor)
    val columnReader = new SkippableVectorizedColumnReader(columnDescriptor, pageReader)
    val columnVector = ColumnVector.allocate(unitSize, dataType, MemoryMode.ON_HEAP)
    columnReader.skipBatch(unitSize, columnVector.dataType, columnVector.isArray)
    columnVector.reset()
    columnReader.readBatch(unitSize, columnVector)
    columnVector
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
