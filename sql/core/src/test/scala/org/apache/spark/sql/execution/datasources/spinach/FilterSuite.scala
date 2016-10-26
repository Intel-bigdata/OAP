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
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("spinach_test")
    sqlContext.dropTempTable("spinach_test_date")
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
    sql("insert overwrite table spinach_test  select * from t")
    sql("create index index1 on spinach_test (a) using btree")

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a = 1"),
      Row(1, "this is test 1") :: Nil)

    checkAnswer(sql("SELECT * FROM spinach_test WHERE a > 1 AND a <= 3"),
      Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
  }
}
