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
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}
import org.apache.spark.sql.types._
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
    withIndex(TestIndex("oap_test_1", "index1")) {
      val dfWithoutIdx = sql("SELECT * FROM oap_test_1 WHERE a > 8 and a <= 200")
      val dfOriginal = sql("SELECT * FROM t WHERE key > 8 and key <= 200")
      sql("create oindex index1 on oap_test_1 (a) using bitmap")
      val dfwithIdx = sql("SELECT * FROM oap_test_1 WHERE a > 8 and a <= 200")
      assert(dfWithoutIdx.count == dfwithIdx.count)
      assert(dfWithoutIdx.count == dfOriginal.count)
    }
  }

  test("index row boundary") {
    val groupSize = 1024 // use a small row group to check boundary.

    val testRowId = groupSize - 1
    val data: Seq[(Int, String)] = (0 until groupSize * 3)
                                    .map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test_1 select * from t")

    withIndex(TestIndex("oap_test_1", "index1")) {
      sql("create oindex index1 on oap_test_1 (a)")

      checkAnswer(sql(s"SELECT * FROM oap_test_1 WHERE a = $testRowId"),
        Row(testRowId, s"this is test $testRowId") :: Nil)
    }
  }

  test("check sequence reading if parquet format") {
    val data: Seq[(Int, String)] = (1 to 300).map { i =>
      if (i == 10) (1, s"this is test $i") else (i, s"this is test $i")
    }
    data.toDF("key", "value").createOrReplaceTempView("t")

    withIndex(TestIndex("oap_parquet_test_1", "index1")) {
      sql("insert overwrite table oap_parquet_test_1 select * from t")
      sql("create oindex index1 on oap_parquet_test_1 (a)")

      // While a in (0, 3), rowIds = (1, 10, 2)
      // sort to ensure the sequence reading on parquet. so results are (1, 2, 10)
      val parquetRslt = sql("select * from oap_parquet_test_1 where a > 0 and a < 3")
      checkAnswer(parquetRslt, Row(1, "this is test 1") ::
        Row(2, "this is test 2") ::
        Row(1, "this is test 10") :: Nil)
    }

    withIndex(TestIndex("oap_test_1", "index1")) {
      sql("insert overwrite table oap_test_1 select * from t")
      sql("create oindex index1 on oap_test_1 (a)")

      // Sort is unnecessary for oap format, so rowIds should be (1, 10, 2)
      val oapResult = sql("select * from oap_test_1 where a > 0 and a < 3")
      checkAnswer(oapResult,
        Row(1, "this is test 1") ::
          Row(1, "this is test 10") ::
          Row(2, "this is test 2") :: Nil)
    }
  }

  test("#604 bitmap index core dump error") {
    val rowRDD = spark.sparkContext.parallelize(1 to 31, 3).map(i =>
      Seq(i % 20, s"this is row $i")).map(Row.fromSeq)
    val schema =
      StructType(
        StructField("a", IntegerType) ::
          StructField("b", StringType) :: Nil)
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("t")

    sql("insert overwrite table oap_test_1 select * from t")

    withIndex(TestIndex("oap_test_1", "bmidx1")) {
      sql("create oindex bmidx1 on oap_test_1 (a) using bitmap")

      val df1 = sql("SELECT * FROM oap_test_1 WHERE a = 1")
      checkAnswer(df1, Row(1, "this is row 1") :: Row(1, "this is row 21") :: Nil)
    }
  }

  test("startswith using index") {
    sqlConf.setConf(OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY, false)
    val data: Seq[(Int, String)] =
      scala.util.Random.shuffle(1 to 30).map(i => (i, s"this$i is test"))
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test_1 select * from t")
    withIndex(TestIndex("oap_test_1", "index1")) {
      sql("create oindex index1 on oap_test_1 (b) using btree")
      checkAnswer(sql("SELECT * FROM oap_test_1 WHERE b like 'this3%'"),
        Row(3, "this3 is test") :: Row(30, "this30 is test") :: Nil)
    }
    sqlConf.setConf(OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY, true)
  }

  test("startswith using multi-dimension index") {
    sqlConf.setConf(OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY, false)
    val data: Seq[(Int, String)] =
      scala.util.Random.shuffle(1 to 30).map(i => (i, s"this$i is test"))
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test_1 select * from t")
    withIndex(TestIndex("oap_test_1", "index1")) {
      sql("create oindex index1 on oap_test_1 (b, a) using btree")
      checkAnswer(sql("SELECT * FROM oap_test_1 WHERE b like 'this3%'"),
        Row(3, "this3 is test") :: Row(30, "this30 is test") :: Nil)
    }
    sqlConf.setConf(OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY, true)
  }

  test("startswith using multi-dimension index - multi-filters") {
    sqlConf.setConf(OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY, false)
    val data: Seq[(Int, String)] =
      scala.util.Random.shuffle(1 to 30).map(i => (i % 7, s"this$i is test"))
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test_1 select * from t")
    withIndex(TestIndex("oap_test_1", "index1")) {
      sql("create oindex index1 on oap_test_1 (a, b) using btree")
      checkAnswer(sql("SELECT * FROM oap_test_1 WHERE a = 3 and b like 'this3%'"),
        Row(3, "this3 is test") :: Nil)
    }
    sqlConf.setConf(OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY, true)
  }

  test("startswith using multi-dimension index 2") {
    sqlConf.setConf(OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY, false)
    val data: Seq[(Int, String)] = Seq(
      15, 29, 26, 4, 28, 17, 16, 11, 12, 27, 22, 6, 10, 18, 19, 20, 30, 21, 14, 25, 1, 2,
      13, 23, 7, 24, 3, 8, 5, 9).map(i => (i, s"this$i is test"))
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test_1 select * from t")
    withIndex(TestIndex("oap_test_1", "index1")) {
      sql("create oindex index1 on oap_test_1 (b) using btree")
      checkAnswer(sql("SELECT * FROM oap_test_1 WHERE b like 'this3%'"),
        Row(3, "this3 is test") :: Row(30, "this30 is test") :: Nil)
    }
    sqlConf.setConf(OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY, true)
  }
}
