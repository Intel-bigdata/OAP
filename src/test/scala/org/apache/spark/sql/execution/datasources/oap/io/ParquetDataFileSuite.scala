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

package org.apache.spark.sql.execution.datasources.oap.io

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{SimpleGroup, SimpleGroupFactory}
import org.apache.parquet.example.Paper
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition.REQUIRED
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


abstract class ParquetDataFileSuite extends SparkFunSuite
  with BeforeAndAfterEach with Logging {

  protected val fileDir: File = Utils.createTempDir()

  protected val fileName: String = Utils.tempFileWith(fileDir).getAbsolutePath

  protected val configuration: Configuration = {
    val conf = new Configuration()
    conf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key,
      SQLConf.PARQUET_BINARY_AS_STRING.defaultValue.get)
    conf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValue.get)
    conf.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get)
    conf
  }

  protected def data: Seq[Group]

  protected def parquetSchema: MessageType

  override def beforeEach(): Unit = prepareData()

  override def afterEach(): Unit = cleanDir()

  private def prepareData(): Unit = {
    val dictPageSize = 512
    val blockSize = 128 * 1024 * 1024
    val pageSize = 1024 * 1024
    GroupWriteSupport.setSchema(parquetSchema, configuration)
    val writer = ExampleParquetWriter.builder(new Path(fileName))
      .withCompressionCodec(UNCOMPRESSED)
      .withRowGroupSize(blockSize)
      .withPageSize(pageSize)
      .withDictionaryPageSize(dictPageSize)
      .withDictionaryEncoding(true)
      .withValidation(false)
      .withWriterVersion(PARQUET_2_0)
      .withConf(configuration)
      .build()

    data.foreach(writer.write)
    writer.close()
  }

  private def cleanDir(): Unit = {
    val path = new Path(fileName)
    val fs = path.getFileSystem(configuration)
    if (fs.exists(path.getParent)) {
      fs.delete(path.getParent, true)
    }
  }
}

class SimpleDataSuite extends ParquetDataFileSuite {

  private val requestSchema: StructType = new StructType()
    .add(StructField("int32_field", IntegerType))
    .add(StructField("int64_field", LongType))
    .add(StructField("boolean_field", BooleanType))
    .add(StructField("float_field", FloatType))

  override def parquetSchema: MessageType = new MessageType("test",
    new PrimitiveType(REQUIRED, INT32, "int32_field"),
    new PrimitiveType(REQUIRED, INT64, "int64_field"),
    new PrimitiveType(REQUIRED, BOOLEAN, "boolean_field"),
    new PrimitiveType(REQUIRED, FLOAT, "float_field"),
    new PrimitiveType(REQUIRED, DOUBLE, "double_field")
  )

  override def data: Seq[Group] = {
    val factory = new SimpleGroupFactory(parquetSchema)
    (0 until 1000).map(i => factory.newGroup()
      .append("int32_field", i)
      .append("int64_field", 64L)
      .append("boolean_field", true)
      .append("float_field", 1.0f)
      .append("double_field", 2.0d))
  }

  test("read by columnIds and rowIds") {
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0, 1)
    val rowIds = Array(0, 1, 7, 8, 120, 121, 381, 382)
    val iterator = reader.iterator(configuration, requiredIds, rowIds)
    val result = ArrayBuffer[Int]()
    while (iterator.hasNext) {
      val row = iterator.next
      assert(row.numFields == 2)
      result += row.getInt(0)
    }
    iterator.close()
    assert(rowIds.length == result.length)
    for (i <- rowIds.indices) {
      assert(rowIds(i) == result(i))
    }
  }

  test("read by columnIds and empty rowIds array") {
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0, 1)
    val rowIds = Array.emptyIntArray
    val iterator = reader.iterator(configuration, requiredIds, rowIds)
    assert(!iterator.hasNext)
    val e = intercept[java.util.NoSuchElementException] {
      iterator.next()
    }.getMessage
    iterator.close()
    assert(e.contains("next on empty iterator"))
  }

  test("read by columnIds ") {
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0)
    val iterator = reader.iterator(configuration, requiredIds)
    val result = ArrayBuffer[ Int ]()
    while (iterator.hasNext) {
      val row = iterator.next
      result += row.getInt(0)
    }
    iterator.close()
    val length = data.length
    assert(length == result.length)
    for (i <- 0 until length) {
      assert(i == result(i))
    }
  }

  test("createDataFileHandle") {
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    val meta = reader.createDataFileHandle()
    val footer = meta.footer
    assert(footer.getFileMetaData != null)
    assert(footer.getBlocks != null)
    assert(!footer.getBlocks.isEmpty)
    assert(footer.getBlocks.size() == 1)
    assert(footer.getBlocks.get(0).getRowCount == data.length)
  }
}

class NestedDataSuite extends ParquetDataFileSuite {

  private val requestStructType: StructType = new StructType()
    .add(StructField("DocId", LongType))
    .add("Links", new StructType()
      .add(StructField("Backward", ArrayType(LongType)))
      .add(StructField("Forward", ArrayType(LongType))))
    .add("Name", ArrayType(new StructType()
      .add(StructField("Language",
          ArrayType(new StructType()
          .add(StructField("Code", BinaryType))
          .add(StructField("Country", BinaryType)))))
      .add(StructField("Url", BinaryType))
      ))

  override def parquetSchema: MessageType = Paper.schema

  override def data: Seq[Group] = {
    val r1 = new SimpleGroup(parquetSchema)
      r1.add("DocId", 10L)
      r1.addGroup("Links")
        .append("Forward", 20L)
        .append("Forward", 40L)
        .append("Forward", 60L)
      var name = r1.addGroup("Name")
      name.addGroup("Language")
        .append("Code", "en-us")
        .append("Country", "us")
      name.addGroup("Language")
        .append("Code", "en")
      name.append("Url", "http://A")

      name = r1.addGroup("Name")
      name.append("Url", "http://B")

      name = r1.addGroup("Name")
      name.addGroup("Language")
        .append("Code", "en-gb")
        .append("Country", "gb")

      val r2 = new SimpleGroup(parquetSchema)
      r2.add("DocId", 20L)
      r2.addGroup("Links")
        .append("Backward", 10L)
        .append("Backward", 30L)
        .append("Forward", 80L)
      r2.addGroup("Name")
        .append("Url", "http://C")
    Seq(r1, r2)
  }

  test("skip read record 1") {
    val reader = ParquetDataFile(fileName, requestStructType, configuration)
    val requiredIds = Array(0, 1, 2)
    val rowIds = Array(1)
    val iterator = reader.iterator(configuration, requiredIds, rowIds)
    assert(iterator.hasNext)
    val row = iterator.next
    assert(row.numFields == 3)
    val docId = row.getLong(0)
    assert(docId == 20L)
    iterator.close()
  }

  test("read all ") {
    val reader = ParquetDataFile(fileName, requestStructType, configuration)
    val requiredIds = Array(0, 2)
    val iterator = reader.iterator(configuration, requiredIds)
    assert(iterator.hasNext)
    val rowOne = iterator.next
    assert(rowOne.numFields == 2)
    val docIdOne = rowOne.getLong(0)
    assert(docIdOne == 10L)
    assert(iterator.hasNext)
    val rowTwo = iterator.next
    assert(rowTwo.numFields == 2)
    val docIdTwo = rowTwo.getLong(0)
    assert(docIdTwo == 20L)
    iterator.close()
  }
}

