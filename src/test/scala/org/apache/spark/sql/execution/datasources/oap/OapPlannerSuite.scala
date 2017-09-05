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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

case class Source(name: String, age: Int, addr: String, phone: String, height: Int)

class OapPlannerSuite
  extends QueryTest
  with SharedSQLContext
  with BeforeAndAfterEach
  with OAPStrategies
{
  import testImplicits._
  sparkConf.set("spark.memory.offHeap.size", "100m")

  override def beforeEach(): Unit = {
    sqlContext.conf.setConf(SQLConf.OAP_IS_TESTING, true)
    val path1 = Utils.createTempDir().getAbsolutePath
    val path2 = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY VIEW oap_sort_opt_table (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path1')""".stripMargin)

    sql(s"""CREATE TEMPORARY VIEW oap_distinct_opt_table (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path2')""".stripMargin)

  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_sort_opt_table")
    sqlContext.dropTempTable("oap_distinct_opt_table")
  }

  test("SortPushDown Test") {
    spark.experimental.extraStrategies = SortPushDownStrategy :: Nil

    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)
    val data = (1 to 300).map{ i => (i%102, s"this is test $i")}
    val dataRDD = spark.sparkContext.parallelize(data, 10)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_sort_opt_table select * from t")
    sql("create oindex index1 on oap_sort_opt_table (a)")
    sql("create oindex index2 on oap_sort_opt_table (b)")

    // check strategy is applied.
    checkKeywordsExist(
      sql("explain SELECT a FROM oap_sort_opt_table WHERE a >= 0 AND a <= 10 ORDER BY a LIMIT 7"),
      "OapOrderLimitFileScanExec")

    // ASC
    checkAnswer(
      sql("SELECT a FROM oap_sort_opt_table WHERE a >= 0 AND a <= 10 ORDER BY a LIMIT 7"),
                Row(0) :: Row(0) :: Row(1) :: Row(1) :: Row(1) :: Row(2) :: Row(2) :: Nil)

    // DESC
    checkAnswer(
      sql("SELECT a FROM oap_sort_opt_table WHERE a >= 90 AND a <= 101 ORDER BY a DESC LIMIT 14"),
          Row(101) :: Row(101) :: Row(100) :: Row(100) :: Row(99) :: Row(99) :: Row(98) ::
          Row( 98) :: Row( 97) :: Row( 97) :: Row( 96) :: Row(96) :: Row(96) :: Row(95) :: Nil)

    sql("drop oindex index1 on oap_sort_opt_table")
    sql("drop oindex index2 on oap_sort_opt_table")
  }

  test("SortPushDown Test with Different Project") {
    spark.experimental.extraStrategies = SortPushDownStrategy :: Nil

    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)
    val data = (1 to 300).map{ i => (i, s"this is test $i")}
    val dataRDD = spark.sparkContext.parallelize(data, 10)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_sort_opt_table select * from t")
    sql("create oindex index1 on oap_sort_opt_table (a)")
    sql("create oindex index2 on oap_sort_opt_table (b)")

    checkAnswer(
      sql("SELECT b FROM oap_sort_opt_table WHERE a >= 1 AND a <= 10 ORDER BY a LIMIT 4"),
        Row("this is test 1") ::
        Row("this is test 2") ::
        Row("this is test 3") ::
        Row("this is test 4") :: Nil)

    sql("drop oindex index1 on oap_sort_opt_table")
    sql("drop oindex index2 on oap_sort_opt_table")
  }

  test("Distinct index scan if SemiJoin Test") {
    spark.sqlContext.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)
    val data = (1 to 300).map{ i => (i, s"this is test $i")}
    val dataRDD = spark.sparkContext.parallelize(data, 10)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_sort_opt_table select * from t")
    sql("create oindex index1 on oap_sort_opt_table (a)")

    val data1 = (1 to 300).map{ i => (i % 10, s"this is test $i")}
    val dataRDD1 = spark.sparkContext.parallelize(data1, 5)

    dataRDD1.toDF("key", "value").createOrReplaceTempView("t1")
    sql("insert overwrite table oap_distinct_opt_table select * from t1")
    sql("create oindex index1 on oap_distinct_opt_table (a)")

    spark.experimental.extraStrategies = SortPushDownStrategy :: OAPSemiJoinStrategy :: Nil
    checkKeywordsExist(
      sql("explain SELECT * " +
      "FROM oap_sort_opt_table t1 " +
      "WHERE EXISTS " +
      "(SELECT 1 FROM oap_distinct_opt_table t2 " +
      "WHERE t1.a = t2.a AND t2.a >= 1 AND t1.a < 5) " +
      "ORDER BY a"), "OapDistinctFileScanExec")

    checkAnswer(
      sql("SELECT * " +
      "FROM oap_sort_opt_table t1 " +
      "WHERE EXISTS " +
      "(SELECT 1 FROM oap_distinct_opt_table t2 " +
      "WHERE t1.a = t2.a AND t2.a >= 1 AND t1.a < 5) " +
      "ORDER BY a"),
      Seq(
        Row(1, "this is test 1"),
        Row(2, "this is test 2"),
        Row(3, "this is test 3"),
        Row(4, "this is test 4")))

    sql("drop oindex index1 on oap_sort_opt_table")
    sql("drop oindex index1 on oap_distinct_opt_table")
  }

  test("OapFileScan WholeStageCodeGen Check") {
    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)
    val data = (1 to 300).map{ i => (i, s"this is test $i")}
    val dataRDD = spark.sparkContext.parallelize(data, 10)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_sort_opt_table select * from t")
    sql("create oindex index1 on oap_sort_opt_table (a)")

    spark.experimental.extraStrategies = SortPushDownStrategy :: OAPSemiJoinStrategy :: Nil
    spark.sqlContext.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    val sqlString =
      "explain SELECT a FROM oap_sort_opt_table WHERE a >= 0 AND a <= 10 ORDER BY a LIMIT 7"

    // OapOrderLimitFileScanExec is applied.
    checkKeywordsExist(sql(sqlString), "OapOrderLimitFileScanExec")
    // OapOrderLimitFileScanExec WholeStageCodeGen is disabled.
    checkKeywordsNotExist(sql(sqlString), "*OapOrderLimitFileScanExec")

    spark.sqlContext.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    // OapOrderLimitFileScanExec WholeStageCodeGen is enabled.
    checkKeywordsExist(sql(sqlString), "*OapOrderLimitFileScanExec")

    sql("drop oindex index1 on oap_sort_opt_table")
  }
}
