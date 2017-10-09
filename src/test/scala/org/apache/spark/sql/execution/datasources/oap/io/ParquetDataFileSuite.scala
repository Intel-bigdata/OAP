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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED
import org.apache.parquet.schema.MessageTypeParser.parseMessageType

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType


class ParquetDataFileSuite extends org.apache.spark.SparkFunSuite
  with org.scalatest.BeforeAndAfterAll with org.apache.spark.internal.Logging {

  val requestSchema: String =
    """{"type": "struct",
          "fields": [{"name": "int32_field","type": "integer","nullable": true,"metadata": {}},
                     {"name": "int64_field","type": "long","nullable": true,"metadata": {}},
                     {"name": "boolean_field","type": "boolean","nullable": true,"metadata": {}},
                     {"name": "float_field","type": "float","nullable": true,"metadata": {}}
                    ]
          }
    """.stripMargin

  val requestStructType: StructType = StructType.fromString(requestSchema)

  val fileName: String = DataGenerator.TARGET_DIR + "/PARQUET-TEST"

  override protected def beforeAll(): Unit = {
    DataGenerator.clean()
    DataGenerator.generate()
  }

  override protected def afterAll(): Unit = DataGenerator.clean()

  test("read by columnIds and rowIds") {

    val reader = ParquetDataFile(fileName, requestStructType, DataGenerator.configuration)

    val requiredIds = Array(0, 1)

    val rowIds = Array(0L, 1L, 7L, 8L, 120L, 121L, 381L, 382L)

    val iterator = reader.iterator(DataGenerator.configuration, requiredIds, rowIds)

    val result = ArrayBuffer[ Int ]()
    while (iterator.hasNext) {
      val row = iterator.next
      assert(row.numFields == 2)
      result += row.getInt(0)
    }

    assert(rowIds.length == result.length)

    for (i <- rowIds.indices) {
      assert(rowIds(i) == result(i))
    }
  }

  test("read by columnIds and empty rowIds array") {

    val reader = ParquetDataFile(fileName, requestStructType, DataGenerator.configuration)

    val requiredIds = Array(0, 1)

    val rowIds = new Array[Long](0)

    val iterator = reader.iterator(DataGenerator.configuration, requiredIds, rowIds)

    assert(!iterator.hasNext)

    val e = intercept[java.util.NoSuchElementException] {
      iterator.next()
    }.getMessage

    assert(e.contains("Input is Empty RowIds Array"))
  }

  test("read by columnIds ") {

    val reader = ParquetDataFile(fileName, requestStructType, DataGenerator.configuration)

    val requiredIds = Array(0)

    val iterator = reader.iterator(DataGenerator.configuration, requiredIds)

    val result = ArrayBuffer[ Int ]()

    while (iterator.hasNext) {
      val row = iterator.next
      result += row.getInt(0)
    }

    assert(DataGenerator.ONE_K == result.length)

    for (i <- 0 until DataGenerator.ONE_K) {
      assert(i == result(i))
    }
  }

  test("createDataFileHandle") {
    val reader = ParquetDataFile(fileName, requestStructType, DataGenerator.configuration)
    val meta = reader.createDataFileHandle()
    val footer = meta.footer

    assert(footer.getFileMetaData != null)
    assert(footer.getBlocks != null)
    assert(!footer.getBlocks.isEmpty)
    assert(footer.getBlocks.size() == 1)
    assert(footer.getBlocks.get(0).getRowCount == DataGenerator.ONE_K)
  }

}


object DataGenerator {

  val DICT_PAGE_SIZE = 512

  val TARGET_DIR = "target/tests/ParquetBenchmarks"

  val FILE_TEST = new Path(TARGET_DIR + "/PARQUET-TEST")

  val configuration = {
    val conf = new Configuration()
    conf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key,
      SQLConf.PARQUET_BINARY_AS_STRING.defaultValue.get)
    conf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValue.get)
    conf.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get)
    conf
  }

  val BLOCK_SIZE_DEFAULT = ParquetWriter.DEFAULT_BLOCK_SIZE

  val PAGE_SIZE_DEFAULT = ParquetWriter.DEFAULT_PAGE_SIZE

  val FIXED_LEN_BYTEARRAY_SIZE = 1024

  val ONE_K = 1000

  def generate() : Unit = {
    generateData(FILE_TEST, configuration, PARQUET_2_0, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT,
      FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, ONE_K)
  }

  private def deleteIfExists(conf: Configuration, path: Path) = {
    val fs = path.getFileSystem(conf)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  def clean() : Unit = deleteIfExists(configuration, FILE_TEST)

  def generateData(outFile: Path,
                   configuration: Configuration,
                   version: ParquetProperties.WriterVersion,
                   blockSize: Int,
                   pageSize: Int,
                   fixedLenByteArraySize: Int,
                   codec: CompressionCodecName,
                   nRows: Int) : Unit = {
    val schema = parseMessageType("message test { "
      + "required int32 int32_field; "
      + "required int64 int64_field; "
      + "required boolean boolean_field; "
      + "required float float_field; "
      + "required double double_field; "
      + "} ")
    GroupWriteSupport.setSchema(schema, configuration)
    val f = new SimpleGroupFactory(schema)
    val writer = new ParquetWriter[ Group ](outFile, new GroupWriteSupport(), codec,
      blockSize, pageSize, DICT_PAGE_SIZE, true, false, version, configuration)
    for (i <- 0 until nRows) {
      writer.write(f.newGroup()
        .append("int32_field", i)
        .append("int64_field", 64L)
        .append("boolean_field", true)
        .append("float_field", 1.0f)
        .append("double_field", 2.0d))
    }
    writer.close()
  }
}
