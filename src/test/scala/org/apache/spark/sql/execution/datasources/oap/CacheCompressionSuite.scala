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
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheManager
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class CacheCompressionSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach{
  import testImplicits._

  private var currentPath: String = _

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
    currentPath = path
    sql(s"""CREATE TEMPORARY VIEW oap_test (a INT, b INT)
           | USING oap
           | OPTIONS (path '$path')""".stripMargin)

    sql(s"""CREATE TEMPORARY VIEW parquet_test (a INT, b INT)
           | USING parquet
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
    sqlContext.dropTempTable("parquet_test")
  }

  test("filtering parquet with compressed FiberCache") {
    FiberCacheManager.setCompressionConf(indexEnable = true, indexCodec = "SNAPPY")
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i*2) }
    data.toDF("a", "b").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    sql("create oindex index1 on parquet_test (a) USING BTREE")
    sql("create oindex index2 on parquet_test (b) USING BITMAP")

    checkAnswer(sql("SELECT b FROM parquet_test WHERE a = 2"),
      Row(4) :: Nil)

    checkAnswer(sql("SELECT a, b FROM parquet_test WHERE  b < 9"),
      Row(1, 2) :: Row(2, 4) ::  Row(3, 6) ::  Row(4, 8) :: Nil)

    sql("drop oindex index1 on parquet_test")
    sql("drop oindex index2 on parquet_test")
    FiberCacheManager.setCompressionConf()
  }

  test("filtering oap with compressed FiberCache") {
    FiberCacheManager.setCompressionConf(indexEnable = true, dataEnable = true,
      indexCodec = "SNAPPY", dataCodec = "SNAPPY")
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i*2) }
    data.toDF("a", "b").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")
    sql("create oindex index1 on oap_test (a) USING BTREE")
    sql("create oindex index2 on oap_test (b) USING BITMAP")

    checkAnswer(sql("SELECT b FROM oap_test WHERE a = 2"),
      Row(4) :: Nil)

    checkAnswer(sql("SELECT a, b FROM oap_test WHERE  b < 9"),
      Row(1, 2) :: Row(2, 4) ::  Row(3, 6) ::  Row(4, 8) :: Nil)
  }
}

