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

package org.apache.spark.sql.execution.datasources.oap

import java.io.File

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.util.Utils


class OapSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {
  import testImplicits._
  private var path: File = null
  private var parquetPath: File = null

  sparkConf.set("spark.memory.offHeap.size", "100m")

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext.conf.setConf(SQLConf.OAP_IS_TESTING, true)
    path = Utils.createTempDir()
    path.delete()
    parquetPath = Utils.createTempDir()
    parquetPath.delete()

    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")

    df.write.format("oap").mode(SaveMode.Overwrite).save(path.getAbsolutePath)
    df.write.format("parquet").mode(SaveMode.Overwrite).save(parquetPath.getAbsolutePath)
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(path)
    } finally {
      super.afterAll()
    }
  }

  test("reading oap file") {
    verifyFrame(sqlContext.read.format("oap").load(path.getAbsolutePath))
  }

  test("No Lease Exception on Parquet File Format in Index Building (#243)") {
    val df = sqlContext.read.format("parquet").load(parquetPath.getAbsolutePath)
    df.createOrReplaceTempView("parquet_table")
    val defaultMaxBytes = sqlContext.conf.getConf(SQLConf.FILES_MAX_PARTITION_BYTES)
    sqlContext.conf.setConf(SQLConf.FILES_MAX_PARTITION_BYTES, 100L)
    val numTasks = sql("select * from parquet_table").queryExecution.toRdd.partitions.length
    try {
      sql("create oindex parquet_idx on parquet_table (a)")
      assert(numTasks == parquetPath.listFiles().count(_.getName.endsWith(".index")))
      sqlContext.conf.setConf(SQLConf.FILES_MAX_PARTITION_BYTES, defaultMaxBytes)
    } finally {
      sql("drop oindex parquet_idx on parquet_table")
    }
  }

  test("Add the corresponding compression type for the OAP data file name if any") {
    Seq("GZIP", "SNAPPY", "LZO", "UNCOMPRESSED").foreach (codec => {
      sqlContext.conf.setConfString(SQLConf.OAP_COMPRESSION.key, codec)
      val df = sqlContext.read.format("oap").load(path.getAbsolutePath)
      df.write.format("oap").mode(SaveMode.Overwrite).save(path.getAbsolutePath)
      val compressionType =
        sqlContext.conf.getConfString(SQLConf.OAP_COMPRESSION.key).toLowerCase()
      val fileNameIterator = path.listFiles()
      for (fileName <- fileNameIterator) {
        if (fileName.toString().endsWith(OapFileFormat.OAP_DATA_EXTENSION)) {
          // If the OAP data file is uncompressed, keep the original file name.
          if (!codec.matches("UNCOMPRESSED")) {
            assert(fileName.toString().contains(compressionType) == true)
          } else {
            assert(fileName.toString().contains(compressionType) == false)
          }
        }
      }
    })
  }

  /** Verifies data and schema. */
  private def verifyFrame(df: DataFrame): Unit = {
    // schema
    assert(df.schema == new StructType()
      .add("a", IntegerType).add("b", IntegerType).add("c", StringType))

    // verify content
    val data = df.collect().sortBy(_.getInt(0)) // sort locally
    assert(data.length == 100)
    assert(data(0) == Row(1, 101, "this is row 1"))
    assert(data(1) == Row(2, 102, "this is row 2"))
    assert(data(99) == Row(100, 200, "this is row 100"))
  }
}
