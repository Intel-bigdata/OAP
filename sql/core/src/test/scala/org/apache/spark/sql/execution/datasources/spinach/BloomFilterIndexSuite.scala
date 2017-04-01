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


/**
 * Index suite for Bloom filter
 */
class BloomFilterIndexSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    System.setProperty("spinach.rowgroup.size", "1024")
    val path = Utils.createTempDir().getAbsolutePath
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
    sql(s"""CREATE TEMPORARY TABLE spinach_double_test (a DOUBLE, b STRING)
           | USING spn
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("spinach_test")
    sqlContext.dropTempTable("spinach_double_test")
    sqlContext.dropTempTable("spinach_test_date")
    sqlContext.dropTempTable("parquet_test")
    sqlContext.dropTempTable("parquet_test_date")
    sql("DROP TABLE IF EXISTS t_refresh")
    sql("DROP TABLE IF EXISTS t_refresh_parquet")
  }

  test("filtering without index") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 1"),
      Row(1, "this is test 1") :: Nil)

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a > 1 AND a <= 3"),
      Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
  }

  test("Bloom filter on replicated items") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("insert into spinach_test select * from t")
    sql("create sindex index_bf on spinach_test (a) USING BLOOM")
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 10"),
      Row(10, "this is test 10") :: Row(10, "this is test 10") :: Nil)
  }

  test("Single Bloom filter single equal value test") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index_bf on spinach_test (a) USING BLOOM")
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 10"),
      Row(10, "this is test 10") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 20"),
      Row(20, "this is test 20") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 100"),
      Row(100, "this is test 100") :: Nil)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 301").count() == 0)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 310").count() == 0)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 10301").count() == 0)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 801").count() == 0)
    sql("drop sindex index_bf on spinach_test")
  }

  test("Single Bloom filter multiple equal value test") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index_bf on spinach_test (a) USING BLOOM")
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 10 AND a = 11").count() == 0)
    checkAnswer(sql(s"SELECT * FROM spinach_test WHERE a = 20 OR a = 21"),
      Row(20, "this is test 20") :: Row(21, "this is test 21") :: Nil)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 10 AND a = 11").count() == 0)
    sql("drop sindex index_bf on spinach_test")
  }

  test("Bloom filter index for range predicate") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index_bf on spinach_test (a)")
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a >= 3 AND a <= 3 AND a < 5"),
      Row(3, "this is test 3") :: Nil)
    sql("drop sindex index_bf on spinach_test")
  }

  test("Single Bloom filter test on other column") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index_bf on spinach_test (b) USING BLOOM")
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 10"),
      Row(10, "this is test 10") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 20"),
      Row(20, "this is test 20") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 100"),
      Row(100, "this is test 100") :: Nil)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 301").count() == 0)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 310").count() == 0)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 10301").count() == 0)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 801").count() == 0)
    sql("drop sindex index_bf on spinach_test")
  }

  test("Bloom filter index null value test on spinach format") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
      .map(tuple => (tuple._1,
        if (tuple._1 == 7) null
        else if (tuple._1 == 11)""
        else tuple._2))
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index_bf on spinach_test (a) USING BLOOM")
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 10"),
      Row(10, "this is test 10") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 20"),
      Row(20, "this is test 20") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 100"),
      Row(100, "this is test 100") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 7"),
      Row(7, null) :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 11"),
      Row(11, "") :: Nil)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 301").count() == 0)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 310").count() == 0)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 10301").count() == 0)
    assert(sql(s"SELECT * FROM spinach_test WHERE a = 801").count() == 0)
    sql("drop sindex index_bf on spinach_test")
  }

  test("Bloom filter index null value test on parquet format") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
      .map(tuple => (tuple._1,
        if (tuple._1 == 7) null
        else if (tuple._1 == 11)""
        else tuple._2))
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table parquet_test select * from t")
    sql("create sindex index_bf on parquet_test (a) USING BLOOM")
    checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 10"),
      Row(10, "this is test 10") :: Nil)
    checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 20"),
      Row(20, "this is test 20") :: Nil)
    checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 100"),
      Row(100, "this is test 100") :: Nil)
    checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 7"),
      Row(7, null) :: Nil)
    checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 11"),
      Row(11, "") :: Nil)
    assert(sql(s"SELECT * FROM parquet_test WHERE a = 301").count() == 0)
    assert(sql(s"SELECT * FROM parquet_test WHERE a = 310").count() == 0)
    assert(sql(s"SELECT * FROM parquet_test WHERE a = 10301").count() == 0)
    assert(sql(s"SELECT * FROM parquet_test WHERE a = 801").count() == 0)
    sql("drop sindex index_bf on parquet_test")
  }

  test("Bloom filter on non-INT column") {
    val data: Seq[(Double, String)] = (1 to 300).map { i => (i + 0.0, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_double_test select * from t")
    sql("create sindex index_bf on spinach_double_test (a) USING BLOOM")
    checkAnswer(sql("SELECT * FROM spinach_double_test WHERE a = 10.0"),
      Row(10.0, "this is test 10") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_double_test WHERE a = 20.0"),
      Row(20.0, "this is test 20") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_double_test WHERE a = 100"),
      Row(100.0, "this is test 100") :: Nil)
    assert(sql(s"SELECT * FROM spinach_double_test WHERE a = 301").count() == 0)
    assert(sql(s"SELECT * FROM spinach_double_test WHERE a = 310").count() == 0)
    assert(sql(s"SELECT * FROM spinach_double_test WHERE a = 10301").count() == 0)
    assert(sql(s"SELECT * FROM spinach_double_test WHERE a = 801").count() == 0)
    sql("drop sindex index_bf on spinach_double_test")
  }

  test("Multiple column Bloom filter index test") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index_bf on spinach_test (a, b) USING BTREE")
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 100 AND b = 'this is test 100'"),
      Row(100, "this is test 100") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_test WHERE b = 'this is test 100'"),
      Row(100, "this is test 100") :: Nil)
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 301"), Nil)
    sql("drop sindex index_bf on spinach_test")
  }
}
