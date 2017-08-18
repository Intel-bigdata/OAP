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

package org.apache.spark.sql.execution.datasources.oap.statistics

import java.sql.Date

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

// integration test for all statistics
class StatisticsManagerSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  import testImplicits._

  sparkConf.set("spark.memory.offHeap.size", "100m")

  override def beforeEach(): Unit = {
    sqlContext.conf.setConf(SQLConf.OAP_IS_TESTING, true)
    val path = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY VIEW oap_test
           | (attr_int INT, attr_str STRING, attr_double DOUBLE,
           |     attr_float FLOAT, attr_date DATE)
           | USING oap
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
  }

  def rowGen(i: Int): (Int, String, Double, Float, Date) =
    (i, s"test#$i", i + 0.0d, i + 0.0f, DateTimeUtils.toJavaDate(i))

  test("test without index") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_int = 1"),
      Row.fromTuple(rowGen(1)) :: Nil)
  }

  test("btree with statistics, data type int") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")

    sql("create oindex index1 on oap_test (attr_int)")
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_int = 1"),
      Row.fromTuple(rowGen(1)) :: Nil)
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_int >= 249 AND attr_int < 261"),
      (249 until 261).map(i => Row.fromTuple(rowGen(i))))
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_int > 495 AND attr_int < 510"),
      (496 to 500).map(i => Row.fromTuple(rowGen(i))))
    sql("drop oindex index1 on oap_test")
  }

  test("btree with statistics, data type string") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")

    sql("create oindex index2 on oap_test (attr_str)")
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_str = \"test#1\""),
      Row.fromTuple(rowGen(1)) :: Nil)
    sql("drop oindex index2 on oap_test")
  }

  test("btree with statistics, data type double") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")


    sql("create oindex index3 on oap_test (attr_double)")
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_double = 1.0"),
      Row.fromTuple(rowGen(1)) :: Nil)
    sql("drop oindex index3 on oap_test")
  }

  test("btree with statistics, data type float") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")

    sql("create oindex index4 on oap_test (attr_float)")
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_float = 1.0"),
      Row.fromTuple(rowGen(1)) :: Nil)
    sql("drop oindex index4 on oap_test")
  }

  test("btree with statistics, data type date") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")

    sql("create oindex index5 on oap_test (attr_date)")
    checkAnswer(sql(s"SELECT * FROM oap_test " +
      s"WHERE attr_date = '${DateTimeUtils.toJavaDate(1).toString}'"),
      Row.fromTuple(rowGen(1)) :: Nil)
    sql("drop oindex index5 on oap_test")
  }

  test("bitmap with statistics, data type int") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")


    sql("create oindex index1 on oap_test (attr_int) USING BITMAP")
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_int = 1"),
      Row.fromTuple(rowGen(1)) :: Nil)
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_int >= 249 AND attr_int < 261"),
      (249 until 261).map(i => Row.fromTuple(rowGen(i))))
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_int > 495 AND attr_int < 510"),
      (496 to 500).map(i => Row.fromTuple(rowGen(i))))
    sql("drop oindex index1 on oap_test")
  }

  test("bitmap with statistics, data type string") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")

    sql("create oindex index2 on oap_test (attr_str) USING BITMAP")
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_str = \"test#1\""),
      Row.fromTuple(rowGen(1)) :: Nil)
    sql("drop oindex index2 on oap_test")
  }

  test("bitmap with statistics, data type double") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")


    sql("create oindex index3 on oap_test (attr_double) USING BITMAP")
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_double = 1.0"),
      Row.fromTuple(rowGen(1)) :: Nil)
    sql("drop oindex index3 on oap_test")
  }

  test("bitmap with statistics, data type float") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")

    sql("create oindex index4 on oap_test (attr_float) USING BITMAP")
    checkAnswer(sql("SELECT * FROM oap_test WHERE attr_float = 1.0"),
      Row.fromTuple(rowGen(1)) :: Nil)
    sql("drop oindex index4 on oap_test")
  }

  test("bitmap with statistics, data type date") {
    val data: Seq[(Int, String, Double, Float, Date)] =
      (1 to 500).map(i => rowGen(i))
    data.toDF("attr_int", "attr_str", "attr_double", "attr_float", "attr_date")
      .createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")

    sql("create oindex index5 on oap_test (attr_date) USING BITMAP")
    checkAnswer(sql(s"SELECT * FROM oap_test " +
      s"WHERE attr_date = '${DateTimeUtils.toJavaDate(1).toString}'"),
      Row.fromTuple(rowGen(1)) :: Nil)
    sql("drop oindex index5 on oap_test")
  }
}
