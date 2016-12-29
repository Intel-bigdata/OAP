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

import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
 * Index suite for Bloom filter
 */
class BloomFilterIndexSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    System.setProperty("spinach.rowgroup.size", "1024")
    val path_tmp = Utils.createTempDir().getAbsolutePath
    val path = path_tmp.replace("\\", "\\\\")
    sql(s"""CREATE TEMPORARY TABLE spinach_test (a INT, b STRING)
            | USING spn
            | OPTIONS (path '$path')""".stripMargin)
    sql(s"""CREATE TEMPORARY TABLE spinach_test_date (a INT, b DATE)
            | USING spn
            | OPTIONS (path '$path')""".stripMargin)
    sql(s"""CREATE TEMPORARY TABLE parquet_test (a INT, b STRING)
            | USING parquet
            | OPTIONS (path '$path')""".stripMargin)
    sql(s"""CREATE TEMPORARY TABLE parquet_test_date (a INT, b DATE)
            | USING parquet
            | OPTIONS (path '$path')""".stripMargin)
    sql(s"""CREATE TABLE t_refresh (a int, b int)
            | USING spn
            | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE t_refresh_parquet (a int, b int)
            | USING parquet
            | PARTITIONED by (b)""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("spinach_test")
    sqlContext.dropTempTable("spinach_test_date")
    sqlContext.dropTempTable("parquet_test")
    sqlContext.dropTempTable("parquet_test_date")
    sql("DROP TABLE IF EXISTS t_refresh")
    sql("DROP TABLE IF EXISTS t_refresh_parquet")
  }

  test("Bloom filter index") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index_bf on spinach_test (a)")
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 1"),
      Row(1, "this is test 1") :: Nil)

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a > 1 AND a <= 3"),
      Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
  }

  test("filtering") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index1 on spinach_test (a)")

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 1"),
      Row(1, "this is test 1") :: Nil)

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a > 1 AND a <= 3"),
      Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
    sql("drop sindex index1 on spinach_test")
  }

  test("multi-index support bf + B+tree") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index1 on spinach_test (a)")
    sql("create sindex index2_bf on spinach_test (a)")
    sql("SELECT * FROM spinach_test WHERE a = 10").show()

  }

  test("bitset test") {
    val bs = new mutable.BitSet(1 << 10)
    bs.add(10)
  }
}
