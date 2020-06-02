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

package com.intel.oap.spark.sql.execution.datasources.arrow

import java.io.File
import java.lang.management.ManagementFactory

import com.intel.oap.spark.sql.DataFrameReaderImplicits._
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowOptions
import com.sun.management.UnixOperatingSystemMXBean
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class ArrowDataSourceTest extends QueryTest with SharedSparkSession {
  private val parquetFile1 = "parquet-1.parquet"
  private val parquetFile2 = "parquet-2.parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    import testImplicits._
    spark.read
      .json(Seq("{\"col\": -1}", "{\"col\": 0}", "{\"col\": 1}", "{\"col\": 2}", "{\"col\": null}")
        .toDS())
      .repartition(1)
      .write.parquet(ArrowDataSourceTest.locateResourcePath(parquetFile1))

    spark.read
      .json(Seq("{\"col\": \"a\"}", "{\"col\": \"b\"}")
        .toDS())
      .repartition(1)
      .write.parquet(ArrowDataSourceTest.locateResourcePath(parquetFile2))
  }

  override def afterAll(): Unit = {
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile1))
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile2))
    super.afterAll()
  }

  test("reading parquet file") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    verifyParquet(
      spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .arrow(path))
  }

  test("simple SQL query on parquet file") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    verifyParquet(spark.sql("select * from ptab"))
    verifyParquet(spark.sql("select col from ptab"))
    verifyParquet(spark.sql("select col from ptab where col is not null or col is null"))
  }

  test("simple SQL query on parquet file with pushed filters") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    spark.sql("select col from ptab where col = 1").explain(true)
    val result = spark.sql("select col from ptab where col = 1") // fixme rowcount == 2?
    assert(
      result.schema ===
        StructType(Seq(StructField("col", LongType))))
    assert(result.collect().length === 1)
  }

  test("ignore unrecognizable types when pushing down filters") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile2)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    val rows = spark.sql("select * from ptab where col = 'b'").collect()
    assert(rows.length === 1)
  }

  test("file descriptor leak") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")

    def getFdCount: Long = {
      ManagementFactory.getOperatingSystemMXBean
        .asInstanceOf[UnixOperatingSystemMXBean]
        .getOpenFileDescriptorCount
    }

    val initialFdCount = getFdCount
    for (_ <- 0 until 100) {
      verifyParquet(spark.sql("select * from ptab"))
    }
    val fdGrowth = getFdCount - initialFdCount
    assert(fdGrowth < 100)
  }

  // csv cases: not implemented
  private val csvFile = "cars.csv"

  ignore("reading csv file without specifying original format") {
    verifyCsv(spark.read.format("arrow").load(csvFile))
  }

  ignore("reading csv file") {
    val path = ArrowDataSourceTest.locateResourcePath(csvFile)
    verifyCsv(
      spark.read
        .format("arrow")
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "csv")
        .load(path))
  }

  ignore("read csv file - programmatic API ") {
    val path = ArrowDataSourceTest.locateResourcePath(csvFile)
    verifyCsv(
      spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "csv")
        .arrow(path))
  }

  def verifyCsv(frame: DataFrame): Unit = {
    // todo assert something
  }

  def verifyParquet(frame: DataFrame): Unit = {
    assert(
      frame.schema ===
        StructType(Seq(StructField("col", LongType))))
    assert(frame.collect().length === 5)
  }

  def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }
}

object ArrowDataSourceTest {
  private def locateResourcePath(resource: String): String = {
    classOf[ArrowDataSourceTest].getClassLoader.getResource("")
      .getPath.concat(File.separator).concat(resource)
  }
}
