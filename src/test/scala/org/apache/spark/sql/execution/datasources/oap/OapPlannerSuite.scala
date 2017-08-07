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
import org.apache.spark.sql.execution.OAPStrategies
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

  override def beforeEach(): Unit = {
    sqlContext.conf.setConf(SQLConf.OAP_IS_TESTING, true)
    val path1 = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY VIEW oap_sort_opt_table (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path1')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_sort_opt_table")
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
}
