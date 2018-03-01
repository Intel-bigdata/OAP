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
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class OapIndexQuerySuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    val path1 = Utils.createTempDir().getAbsolutePath
    val path2 = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY VIEW oap_test_1 (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path1')""".stripMargin)

    sql(s"""CREATE TEMPORARY VIEW oap_parquet_test_1 (a INT, b STRING)
           | USING parquet
           | OPTIONS (path '$path2')""".stripMargin)

  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test_1")
    sqlContext.dropTempTable("oap_parquet_test_1")
  }

  test("index integrity") {
      val data: Seq[(Int, String)] =
        scala.util.Random.shuffle(1 to 300).map{ i => (i, s"this is test $i") }
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

  test("check sequence reading if parquet format") {
    val data: Seq[(Int, String)] = (1 to 300).map { i =>
      if (i == 10) (1, s"this is test $i") else (i, s"this is test $i")
    }
    data.toDF("key", "value").createOrReplaceTempView("t")

    sql("insert overwrite table oap_parquet_test_1 select * from t")
    sql("create oindex index1 on oap_parquet_test_1 (a)")

    // While a in (0, 3), rowIds = (1, 10, 2)
    // sort to ensure the sequence reading on parquet. so results are (1, 2, 10)
    val parquetRslt = sql("select * from oap_parquet_test_1 where a > 0 and a < 3").collect
    assert(parquetRslt.corresponds(
      Row(1, "this is test 1") ::
      Row(2, "this is test 2") ::
      Row(1, "this is test 10") :: Nil) {_ == _})

    sql("insert overwrite table oap_test_1 select * from t")
    sql("create oindex index1 on oap_test_1 (a)")

    // Sort is unnecessary for oap format, so rowIds should be (1, 10, 2)
    val oapResult = sql("select * from oap_test_1 where a > 0 and a < 3").collect
    assert(oapResult.corresponds(
      Row(1, "this is test 1") ::
      Row(1, "this is test 10") ::
      Row(2, "this is test 2") :: Nil) {_ == _})

    sql("drop oindex index1 on oap_parquet_test_1")
    sql("drop oindex index1 on oap_test_1")

  }
}
