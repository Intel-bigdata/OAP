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

import java.sql.Date

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class FilterSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
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
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("spinach_test")
    sqlContext.dropTempTable("spinach_test_date")
    sqlContext.dropTempTable("parquet_test")
    sqlContext.dropTempTable("parquet_test_date")
    sql("DROP TABLE IF EXISTS t_refresh")
    sql("DROP TABLE IF EXISTS t_refresh_parquet")
  }

  test("empty table") {
    checkAnswer(sql("SELECT * FROM spinach_test"), Nil)
  }

  test("insert into table") {
    val data: Seq[(Int, String)] = (1 to 3000).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    checkAnswer(sql("SELECT * FROM spinach_test"), Seq.empty[Row])
    sql("insert overwrite table spinach_test select * from t")
    checkAnswer(sql("SELECT * FROM spinach_test"), data.map { row => Row(row._1, row._2) })
  }

  test("test spinach row group size change") {
    val defaultRowGroupSize = System.getProperty("spinach.rowgroup.size")
    assert(defaultRowGroupSize != null)
    // change default row group size
    System.setProperty("spinach.rowgroup.size", "1025")
    val data: Seq[(Int, String)] = (1 to 3000).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    checkAnswer(sql("SELECT * FROM spinach_test"), Seq.empty[Row])
    sql("insert overwrite table spinach_test select * from t")
    checkAnswer(sql("SELECT * FROM spinach_test"), data.map { row => Row(row._1, row._2) })
    // set back to default value
    System.setProperty("spinach.rowgroup.size", defaultRowGroupSize)
  }

  test("test date type") {
    val data: Seq[(Int, Date)] = (1 to 3000).map { i => (i, DateTimeUtils.toJavaDate(i)) }
    data.toDF("key", "value").registerTempTable("d")
    sql("insert overwrite table spinach_test_date select * from d")
    checkAnswer(sql("SELECT * FROM spinach_test_date"), data.map {row => Row(row._1, row._2)})
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

  test("filtering parquet") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table parquet_test select * from t")
    sql("create sindex index1 on parquet_test (a)")

    checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
      Row(1, "this is test 1") :: Nil)

    checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 1 AND a <= 3"),
      Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
  }

  test("test refresh in parquet format on same partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").registerTempTable("t")

    sql(
      """
        |INSERT OVERWRITE TABLE t_refresh_parquet
        |partition (b=1)
        |SELECT key from t where value < 4
      """.stripMargin)

    sql("create sindex index1 on t_refresh_parquet (a)")

    checkAnswer(sql("select * from t_refresh_parquet"),
      Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Nil)

    sql(
      """
        |INSERT INTO TABLE t_refresh_parquet
        |partition (b=1)
        |SELECT key from t where value == 4
      """.stripMargin)

    sql("refresh sindex on t_refresh_parquet")

    checkAnswer(sql("select * from t_refresh_parquet"),
      Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(4, 1) :: Nil)
  }

  test("test refresh in parquet format on different partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").registerTempTable("t")

    sql(
      """
        |INSERT OVERWRITE TABLE t_refresh_parquet
        |partition (b=1)
        |SELECT key from t where value < 4
      """.stripMargin)

    sql("create sindex index1 on t_refresh_parquet (a)")

    checkAnswer(sql("select * from t_refresh_parquet"),
      Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Nil)

    sql(
      """
        |INSERT INTO TABLE t_refresh_parquet
        |partition (b=2)
        |SELECT key from t where value == 4
      """.stripMargin)

    sql("refresh sindex on t_refresh_parquet")

    checkAnswer(sql("select * from t_refresh_parquet"),
      Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(4, 2) :: Nil)
  }

  test("test refresh in spn format on same partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").registerTempTable("t")

    sql(
      """
        |INSERT OVERWRITE TABLE t_refresh
        |partition (b=1)
        |SELECT key from t where value < 4
      """.stripMargin)

    sql("create sindex index1 on t_refresh (a)")

    checkAnswer(sql("select * from t_refresh"),
      Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Nil)

    sql(
      """
        |INSERT INTO TABLE t_refresh
        |partition (b=1)
        |SELECT key from t where value == 4
      """.stripMargin)

    sql("refresh sindex on t_refresh")

    checkAnswer(sql("select * from t_refresh"),
      Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(4, 1) :: Nil)
  }

  test("test refresh in spn format on different partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").registerTempTable("t")

    sql(
      """
        |INSERT OVERWRITE TABLE t_refresh
        |partition (b=1)
        |SELECT key from t where value < 4
      """.stripMargin)

    sql("create sindex index1 on t_refresh (a)")

    checkAnswer(sql("select * from t_refresh"),
      Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Nil)

    sql(
      """
        |INSERT INTO TABLE t_refresh
        |partition (b=2)
        |SELECT key from t where value == 4
      """.stripMargin)

    sql(
      """
        |INSERT INTO TABLE t_refresh
        |partition (b=1)
        |SELECT key from t where value == 5
      """.stripMargin)

    sql("refresh sindex on t_refresh")

    checkAnswer(sql("select * from t_refresh"),
      Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(4, 2) :: Row(5, 1) :: Nil)
  }

  test("refresh table of spinach format without partition") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index1 on spinach_test (a)")

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 1"),
      Row(1, "this is test 1") :: Nil)

    sql("insert into table spinach_test select * from t")
    sql("refresh sindex on spinach_test")

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 1"),
      Row(1, "this is test 1") :: Row(1, "this is test 1") :: Nil)
  }

  test("refresh table of parquet format without partition") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table parquet_test select * from t")
    sql("create sindex index1 on parquet_test (a)")

    checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
      Row(1, "this is test 1") :: Nil)

    sql("insert into table parquet_test select * from t")
    sql("refresh sindex on parquet_test")

    checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
      Row(1, "this is test 1") :: Row(1, "this is test 1") :: Nil)
  }

  test("filtering by string") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")
    sql("create sindex index1 on spinach_test (a) using btree")

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 1"),
      Row(1, "this is test 1") :: Nil)

    sql("insert into table spinach_test select * from t")
    sql("refresh sindex on spinach_test")

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 1"),
      Row(1, "this is test 1") :: Row(1, "this is test 1") :: Nil)
  }

  test("support data append without refresh") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")

    sql("insert overwrite table spinach_test select * from t where key > 100")
    sql("create sindex index1 on spinach_test (a)")
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 100"), Nil)
    sql("insert into table spinach_test select * from t where key = 100")
    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 100"),
      Row(100, "this is test 100") :: Nil)

    sql("insert overwrite table parquet_test select * from t where key > 100")
    sql("create sindex index1 on parquet_test (a)")
    checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 100"), Nil)
    sql("insert into table parquet_test select * from t where key = 100")
    checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 100"),
      Row(100, "this is test 100") :: Nil)
  }
}
