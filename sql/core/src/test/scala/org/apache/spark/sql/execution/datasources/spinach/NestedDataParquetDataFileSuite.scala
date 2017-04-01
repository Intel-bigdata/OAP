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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0
import org.apache.parquet.example.Paper
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED

import org.apache.spark.sql.execution.datasources.spinach.io.ParquetDataFile
import org.apache.spark.sql.types.StructType


class NestedDataParquetDataFileSuite extends org.apache.spark.SparkFunSuite
  with org.scalatest.BeforeAndAfterAll with org.apache.spark.internal.Logging {

  val requestSchema =
    """{
      |    "type": "struct",
      |    "fields": [
      |        {
      |            "name": "DocId",
      |            "type": "long",
      |            "nullable": true,
      |            "metadata": {}
      |        },
      |        {
      |            "name": "Links",
      |            "type": {
      |                "type": "struct",
      |                "fields": [
      |                    {
      |                        "name": "Backward",
      |                        "type": {
      |                            "type": "array",
      |                            "elementType": "long",
      |                            "containsNull": true
      |                        },
      |                        "nullable": true,
      |                        "metadata": {}
      |                    },
      |                    {
      |                        "name": "Forward",
      |                        "type": {
      |                            "type": "array",
      |                            "elementType": "long",
      |                            "containsNull": true
      |                        },
      |                        "nullable": true,
      |                        "metadata": {}
      |                    }
      |                ]
      |            },
      |            "nullable": true,
      |            "metadata": {}
      |        },
      |        {
      |            "name": "Name",
      |            "type": {
      |                "type": "array",
      |                "elementType": {
      |                    "type": "struct",
      |                    "fields": [
      |                        {
      |                            "name": "Language",
      |                            "type": {
      |                                "type": "array",
      |                                "elementType": {
      |                                    "type": "struct",
      |                                    "fields": [
      |                                        {
      |                                            "name": "Code",
      |                                            "type": "binary",
      |                                            "nullable": true,
      |                                            "metadata": {}
      |                                        },
      |                                        {
      |                                            "name": "Country",
      |                                            "type": "binary",
      |                                            "nullable": true,
      |                                            "metadata": {}
      |                                        }
      |                                    ]
      |                                },
      |                                "containsNull": true
      |                            },
      |                            "nullable": true,
      |                            "metadata": {}
      |                        },
      |                        {
      |                            "name": "Url",
      |                            "type": "binary",
      |                            "nullable": true,
      |                            "metadata": {}
      |                        }
      |                    ]
      |                },
      |                "containsNull": true
      |            },
      |            "nullable": true,
      |            "metadata": {}
      |        }
      |    ]
      |}
    """.stripMargin

  val requestStructType = StructType.fromString(requestSchema)

  val fileName = DataGenerator.TARGET_DIR + "/Paper.parquet"

  override protected def beforeAll(): Unit = {
    DataGenerator.clean()
    DataGenerator.generate()
  }

  override protected def afterAll(): Unit = DataGenerator.clean()

  test("skip read record 1") {

    val reader = ParquetDataFile(fileName, requestStructType)

    val requiredIds = Array(0, 1, 2)

    val rowIds = Array(1L)

    val iterator = reader.iterator(DataGenerator.configuration, requiredIds, rowIds)

    assert(iterator.hasNext)

    val row = iterator.next

    val docId = row.getLong(0)

    assert(docId == 20L)

  }

  test("read all ") {

    val reader = ParquetDataFile(fileName, requestStructType)

    val requiredIds = Array(0, 1, 2)

    val iterator = reader.iterator(DataGenerator.configuration, requiredIds)

    assert(iterator.hasNext)

    val rowOne = iterator.next

    val docIdOne = rowOne.getLong(0)

    assert(docIdOne == 10L)


    assert(iterator.hasNext)

    val rowTwo = iterator.next

    val docIdTwo = rowTwo.getLong(0)

    assert(docIdTwo == 20L)
  }




  object DataGenerator {

    val DICT_PAGE_SIZE = 512

    val TARGET_DIR = "target/tests/ParquetBenchmarks"

    val FILE_TEST = new Path(TARGET_DIR + "/Paper.parquet")

    val configuration = new Configuration()

    val BLOCK_SIZE_DEFAULT = ParquetWriter.DEFAULT_BLOCK_SIZE

    val PAGE_SIZE_DEFAULT = ParquetWriter.DEFAULT_PAGE_SIZE

    val FIXED_LEN_BYTEARRAY_SIZE = 1024


    def generate(): Unit = {
      generateData(FILE_TEST, configuration, PARQUET_2_0, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT,
        FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED)
    }

    private def deleteIfExists(conf: Configuration, path: Path) = {
      val fs = path.getFileSystem(conf)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    }

    def clean(): Unit = deleteIfExists(configuration, FILE_TEST)

    def generateData(outFile: Path,
                     configuration: Configuration,
                     version: ParquetProperties.WriterVersion,
                     blockSize: Int,
                     pageSize: Int,
                     fixedLenByteArraySize: Int,
                     codec: CompressionCodecName): Unit = {

      GroupWriteSupport.setSchema(Paper.schema, configuration)

      val r1 = new SimpleGroup(Paper.schema)
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

      val r2 = new SimpleGroup(Paper.schema)
      r2.add("DocId", 20L)
      r2.addGroup("Links")
        .append("Backward", 10L)
        .append("Backward", 30L)
        .append("Forward", 80L)
      r2.addGroup("Name")
        .append("Url", "http://C")


      val writer = new ParquetWriter[Group](outFile, new GroupWriteSupport(), codec,
        blockSize, pageSize, DICT_PAGE_SIZE, true, false, version, configuration)

      writer.write(r1)
      writer.write(r2)

      writer.close()
    }
  }

}
