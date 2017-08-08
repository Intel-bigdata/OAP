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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.OAPStrategies
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfterEach


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

  test("SortPushDown Test") {
    spark.experimental.extraStrategies = SortPushDownStrategy :: Nil

    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)
    val data = (1 to 300).map{ i => (i, s"this is test $i")}
    val dataRDD = spark.sparkContext.parallelize(data, 3)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_sort_opt_table select * from t")
    sql("create oindex index1 on oap_sort_opt_table (a)")

    checkAnswer(
      sql("SELECT * FROM oap_sort_opt_table WHERE a >= 98 AND a <= 106 ORDER BY a LIMIT 4"),
                Row(98, "this is test 98") ::
                Row(99, "this is test 99") ::
                Row(100, "this is test 100") ::
                Row(101, "this is test 101") :: Nil)

    checkAnswer(
      sql("SELECT * FROM oap_sort_opt_table WHERE a >= 98 AND a <= 101 ORDER BY a DESC LIMIT 4"),
          Row(101, "this is test 101") ::
          Row(100, "this is test 100") ::
          Row(99, "this is test 99") ::
          Row(98, "this is test 98") :: Nil)

    sql("drop oindex index1 on oap_sort_opt_table")
  }

}
