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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0
import org.apache.parquet.example.Paper
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


class NestedDataParquetDataFileSuite extends SparkFunSuite
  with BeforeAndAfterEach with Logging {

  val requestStructType: StructType = new StructType()
    .add(StructField("DocId", LongType))
    .add("Links", new StructType()
      .add(new StructField("Backward", new ArrayType(LongType, true)))
      .add(new StructField("Forward", new ArrayType(LongType, true))))
    .add("Name", new ArrayType(new StructType()
      .add(new StructField("Language",
        new ArrayType(new StructType()
          .add(new StructField("Code", BinaryType))
          .add(new StructField("Country", BinaryType)), true)))
      .add(new StructField("Url", BinaryType))
      , true))

  val fileName: String = DataGenerator.TARGET_DIR + "/Paper.parquet"

  override def beforeEach(): Unit = {
    DataGenerator.clean()
    DataGenerator.generate()
  }

  override def afterEach(): Unit = DataGenerator.clean()






  object DataGenerator {

    val DICT_PAGE_SIZE = 512

    val TARGET_DIR = "target/tests/ParquetBenchmarks"

    val FILE_TEST = new Path(TARGET_DIR + "/Paper.parquet")

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


      val writer = ExampleParquetWriter.builder(outFile)
        .withCompressionCodec(codec)
        .withRowGroupSize(blockSize)
        .withPageSize(pageSize)
        .withDictionaryPageSize(DICT_PAGE_SIZE)
        .withDictionaryEncoding(true)
        .withValidation(false)
        .withWriterVersion(version)
        .withConf(configuration)
        .build()

      writer.write(r1)
      writer.write(r2)

      writer.close()
    }
  }
}
