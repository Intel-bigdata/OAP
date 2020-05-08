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

import com.intel.oap.spark.sql.DataFrameReaderImplicits._
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowOptions
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.datasources.v2.arrow.ArrowUtils
import org.apache.spark.sql.internal.SQLConf
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
      .write.parquet(ArrowDataSourceTest.locateResourcePath(parquetFile1))

    spark.read
      .json(Seq("{\"col\": \"a\"}", "{\"col\": \"b\"}")
        .toDS())
      .write.parquet(ArrowDataSourceTest.locateResourcePath(parquetFile2))
  }

  override def afterAll(): Unit = {
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile1))
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile2))
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

  // tpc-h query cases: generated tpc-h dataset required
  private val prefix = "/PATH/TO/TCPH/DATA/"
  private val tpchFolder = "tpch100"
  private val lineitem = prefix + tpchFolder + "/lineitem"
  private val part = prefix + tpchFolder + "/part"
  private val partSupp = prefix + tpchFolder + "/partsupp"
  private val supplier = prefix + tpchFolder + "/supplier"
  private val orders = prefix + tpchFolder + "/orders"

  ignore("tpch lineitem - desc") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(lineitem)
    frame.createOrReplaceTempView("lineitem")

    spark.sql("describe lineitem").show()
  }

  ignore("tpch lineitem - read partition values") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(orders)
    frame.createOrReplaceTempView("orders")

    spark.sql("select o_orderdate from orders limit 100").show()
  }

  ignore("tpch lineitem - asterisk select") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(lineitem)
    frame.createOrReplaceTempView("lineitem")

    spark.sql("select * from lineitem limit 10").show()
  }

  ignore("tpch query 6") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(lineitem)
    frame.createOrReplaceTempView("lineitem")

    spark.sql("select\n\tsum(l_extendedprice * l_discount) as revenue\n" +
      "from\n\tlineitem\n" +
      "where\n\tl_shipdate >= date '1994-01-01'\n\t" +
      "and l_shipdate < date '1994-01-01' + interval '1' year\n\t" +
      "and l_discount between .06 - 0.01 and .06 + 0.01\n\t" +
      "and l_quantity < 24").show()
  }

  ignore("tpch query 6 - performance comparision") {
    val iterations = 10
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "false") {
      val frame1 = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .arrow(lineitem)
      frame1.createOrReplaceTempView("lineitem_arrow")

      val frame2 = spark.read
        .parquet(lineitem)
      frame2.createOrReplaceTempView("lineitem_parquet")

      val pPrev = System.currentTimeMillis()
      (0 until iterations).foreach(_ =>
        spark.sql("select\n\tsum(l_extendedprice * l_discount) as revenue\n" +
          "from\n\tlineitem_parquet\n" +
          "where\n\tl_shipdate >= date '1994-01-01'\n\t" +
          "and l_shipdate < date '1994-01-01' + interval '1' year\n\t" +
          "and l_discount between .06 - 0.01 and .06 + 0.01\n\t" +
          "and l_quantity < 24").show()
      )
      val parquetExecTime = System.currentTimeMillis() - pPrev

      val aPrev = System.currentTimeMillis()
      (0 until iterations).foreach(_ => {
        // scalastyle:off println
        println(ArrowUtils.rootAllocator().getAllocatedMemory())
        // scalastyle:on println
        spark.sql("select\n\tsum(l_extendedprice * l_discount) as revenue\n" +
          "from\n\tlineitem_arrow\n" +
          "where\n\tl_shipdate >= date '1994-01-01'\n\t" +
          "and l_shipdate < date '1994-01-01' + interval '1' year\n\t" +
          "and l_discount between .06 - 0.01 and .06 + 0.01\n\t" +
          "and l_quantity < 24").show()
      }
      )
      val arrowExecTime = System.currentTimeMillis() - aPrev

      // unstable assert
      assert(arrowExecTime < parquetExecTime)
    }
  }

  ignore("tpch query 16 - performance comparision") {
    val iterations = 1
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "false") {
      val frame1 = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .arrow(partSupp)
      frame1.createOrReplaceTempView("partsupp_arrow")

      val frame2 = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .arrow(part)
      frame2.createOrReplaceTempView("part_arrow")

      val frame3 = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .arrow(supplier)
      frame3.createOrReplaceTempView("supplier_arrow")

      val frame4 = spark.read
        .parquet(partSupp)
      frame4.createOrReplaceTempView("partsupp_parquet")

      val frame5 = spark.read
        .parquet(part)
      frame5.createOrReplaceTempView("part_parquet")

      val frame6 = spark.read
        .parquet(supplier)
      frame6.createOrReplaceTempView("supplier_parquet")

      val pPrev = System.currentTimeMillis()
      (0 until iterations).foreach(_ =>
        spark.sql("select\n\tp_brand,\n\tp_type,\n\tp_size," +
          "\n\tcount(distinct ps_suppkey) as supplier_cnt\n" +
          "from\n\tpartsupp_parquet,\n\tpart_parquet\nwhere\n\tp_partkey" +
          " = ps_partkey\n\tand p_brand <> 'Brand#45'\n\t" +
          "and p_type not like 'MEDIUM POLISHED%'\n\tand p_size in " +
          "(49, 14, 23, 45, 19, 3, 36, 9)\n\t" +
          "and ps_suppkey not in (\n\t\tselect\n\t\t\ts_suppkey\n\t\t" +
          "from\n\t\t\tsupplier_parquet\n\t\twhere\n\t\t\t" +
          "s_comment like '%Customer%Complaints%'\n\t)\ngroup by\n\t" +
          "p_brand,\n\tp_type,\n\tp_size\norder by\n\t" +
          "supplier_cnt desc,\n\tp_brand,\n\tp_type,\n\tp_size").show()
      )
      val parquetExecTime = System.currentTimeMillis() - pPrev

      val aPrev = System.currentTimeMillis()
      (0 until iterations).foreach(_ =>
        spark.sql("select\n\tp_brand,\n\tp_type,\n\tp_size," +
          "\n\tcount(distinct ps_suppkey) as supplier_cnt\n" +
          "from\n\tpartsupp_arrow,\n\tpart_arrow\nwhere\n\tp_partkey" +
          " = ps_partkey\n\tand p_brand <> 'Brand#45'\n\t" +
          "and p_type not like 'MEDIUM POLISHED%'\n\tand p_size in " +
          "(49, 14, 23, 45, 19, 3, 36, 9)\n\t" +
          "and ps_suppkey not in (\n\t\tselect\n\t\t\ts_suppkey\n\t\t" +
          "from\n\t\t\tsupplier_arrow\n\t\twhere\n\t\t\t" +
          "s_comment like '%Customer%Complaints%'\n\t)\ngroup by\n\t" +
          "p_brand,\n\tp_type,\n\tp_size\norder by\n\t" +
          "supplier_cnt desc,\n\tp_brand,\n\tp_type,\n\tp_size").show()
      )
      val arrowExecTime = System.currentTimeMillis() - aPrev

      // scalastyle:off println
      println(arrowExecTime)
      println(parquetExecTime)
      // scalastyle:on println
      // unstable assert
      assert(arrowExecTime < parquetExecTime)
    }
  }

  ignore("tpch query 1") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(lineitem)
    frame.createOrReplaceTempView("lineitem")

    spark.sql("select\n\tl_returnflag,\n\tl_linestatus," +
      "\n\tsum(l_quantity) as sum_qty,\n\t" +
      "sum(l_extendedprice) as sum_base_price," +
      "\n\tsum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n\t" +
      "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge," +
      "\n\tavg(l_quantity) as avg_qty,\n\t" +
      "avg(l_extendedprice) as avg_price,\n\tavg(l_discount) as avg_disc," +
      "\n\tcount(*) as count_order\nfrom\n\t" +
      "lineitem\nwhere\n\tl_shipdate <= date '1998-12-01' - interval '90' day" +
      "\ngroup by\n\tl_returnflag,\n\t" +
      "l_linestatus\norder by\n\tl_returnflag,\n\tl_linestatus").explain(true)
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
