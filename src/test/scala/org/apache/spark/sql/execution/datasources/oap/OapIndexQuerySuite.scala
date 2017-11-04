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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils


class OapIndexQuerySuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  import testImplicits._

  sparkConf.set("spark.memory.offHeap.size", "100m")

  override def beforeEach(): Unit = {
    val path1 = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY VIEW oap_test_1 (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path1')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test_1")
  }

  test("index integrity") {
      val data: Seq[(Int, String)] =
        scala.util.Random.shuffle(1 to 300).map{ i => (i, s"this is test $i") }.toSeq
      data.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table oap_test_1 select * from t")
      sql("create oindex index1 on oap_test_1 (a) using bitmap")

      val dfwithIdx = sql("SELECT * FROM oap_test_1 WHERE a > 8 and a <= 200")
      sql("drop oindex index1 on oap_test_1")
      val dfWithoutIdx = sql("SELECT * FROM oap_test_1 WHERE a > 8 and a <= 200")
      val dfOriginal = sql("SELECT * FROM t WHERE key > 8 and key <= 200")
      assert(dfWithoutIdx.count == dfwithIdx.count)
      assert(dfWithoutIdx.count == dfOriginal.count)
  }

  test("index row boundary") {
    val groupSize = 1024 // use a small row group to check boundary.

    val testRowId = groupSize - 1
    val data: Seq[(Int, String)] = (0 until groupSize * 3)
                                    .map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test_1 select * from t")
    sql("create oindex index1 on oap_test_1 (a)")

    checkAnswer(sql(s"SELECT * FROM oap_test_1 WHERE a = $testRowId"),
      Row(testRowId, s"this is test $testRowId") :: Nil)

    sql("drop oindex index1 on oap_test_1")
  }
}
