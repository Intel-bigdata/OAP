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
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils


class SpinachDDLSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    System.setProperty("spinach.rowgroup.size", "1024")
    val path1 = Utils.createTempDir().getAbsolutePath
    val path2 = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY VIEW spinach_test_1 (a INT, b STRING)
           | USING parquet
           | OPTIONS (path '$path1')""".stripMargin)
    sql(s"""CREATE TEMPORARY VIEW spinach_test_2 (a INT, b STRING)
           | USING spn
           | OPTIONS (path '$path2')""".stripMargin)
    sql(s"""CREATE TABLE spinach_partition_table (a int, b int, c STRING)
            | USING parquet
            | PARTITIONED by (b, c)""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("spinach_test_1")
    sqlContext.dropTempTable("spinach_test_2")
    sqlContext.dropTempTable("spinach_partition_table")
  }

  test("show index") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    checkAnswer(sql("show sindex from spinach_test_1"), Nil)
    sql("insert overwrite table spinach_test_1 select * from t")
    sql("insert overwrite table spinach_test_2 select * from t")
    sql("create sindex index1 on spinach_test_1 (a)")
    checkAnswer(sql("show sindex from spinach_test_2"), Nil)
    sql("create sindex index2 on spinach_test_1 (b desc)")
    sql("create sindex index3 on spinach_test_1 (b asc, a desc)")
    sql("create sindex index4 on spinach_test_2 (a) using btree")
    sql("create sindex index5 on spinach_test_2 (b desc)")
    sql("create sindex index6 on spinach_test_2 (a) using bitmap")
    sql("create sindex index1 on spinach_test_2 (a desc, b desc)")

    checkAnswer(sql("show sindex from spinach_test_1"),
      Row("spinach_test_1", "index1", 0, "a", "A", "BTREE") ::
        Row("spinach_test_1", "index2", 0, "b", "D", "BTREE") ::
        Row("spinach_test_1", "index3", 0, "b", "A", "BTREE") ::
        Row("spinach_test_1", "index3", 1, "a", "D", "BTREE") :: Nil)

    checkAnswer(sql("show sindex in spinach_test_2"),
      Row("spinach_test_2", "index4", 0, "a", "A", "BTREE") ::
        Row("spinach_test_2", "index5", 0, "b", "D", "BTREE") ::
        Row("spinach_test_2", "index6", 0, "a", "A", "BITMAP") ::
        Row("spinach_test_2", "index1", 0, "a", "D", "BTREE") ::
        Row("spinach_test_2", "index1", 1, "b", "D", "BTREE") :: Nil)
  }

  test("create and drop index with partition specify") {
    val data: Seq[(Int, Int)] = (1 to 10).map { i => (i, i) }
    data.toDF("key", "value").registerTempTable("t")

    val path = new Path(spark.sqlContext.conf.warehousePath)

    sql(
      """
        |INSERT OVERWRITE TABLE spinach_partition_table
        |partition (b=1, c='c1')
        |SELECT key from t where value < 4
      """.stripMargin)

    sql(
      """
        |INSERT INTO TABLE spinach_partition_table
        |partition (b=2, c='c2')
        |SELECT key from t where value == 4
      """.stripMargin)

    sql("create sindex index1 on spinach_partition_table (a) partition (b=1, c='c1')")

    checkAnswer(sql("select * from spinach_partition_table where a < 4"),
      Row(1, 1, "c1") :: Row(2, 1, "c1") :: Row(3, 1, "c1") :: Nil)

    assert(path.getFileSystem(
      new Configuration()).globStatus(new Path(path,
        "spinach_partition_table/b=1/c=c1/*.index")).length != 0)
    assert(path.getFileSystem(
      new Configuration()).globStatus(new Path(path,
        "spinach_partition_table/b=2/c=c2/*.index")).length == 0)

    sql("create sindex index1 on spinach_partition_table (a) partition (b=2, c='c2')")
    sql("drop sindex index1 on spinach_partition_table partition (b=1, c='c1')")

    checkAnswer(sql("select * from spinach_partition_table"),
      Row(1, 1, "c1") :: Row(2, 1, "c1") :: Row(3, 1, "c1") :: Row(4, 2, "c2") :: Nil)
    assert(path.getFileSystem(
      new Configuration()).globStatus(new Path(path,
      "spinach_partition_table/b=1/c=c1/*.index")).length == 0)
    assert(path.getFileSystem(
      new Configuration()).globStatus(new Path(path,
      "spinach_partition_table/b=2/c=c2/*.index")).length != 0)
  }
}

