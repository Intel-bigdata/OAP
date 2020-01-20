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

package org.apache.spark.sql.execution.datasources.oap.orc

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex, TestPartition}
import org.apache.spark.util.Utils



class OapOrcQuerySuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {

  import testImplicits._

  private var currentPath: String = _
  private var defaultEis: Boolean = true

  override def beforeAll(): Unit = {
    super.beforeAll()
    // In this suite we don't want to skip index even if the cost is higher.
    defaultEis = sqlContext.conf.getConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION)
    sqlContext.conf.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION, false)
  }

  override def afterAll(): Unit = {
    sqlContext.conf.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION, defaultEis)
    super.afterAll()
  }

  override def beforeEach(): Unit = {

    val path = Utils.createTempDir().getAbsolutePath
    currentPath = path
    sql(s"""CREATE TEMPORARY VIEW orc_test (a INT, b STRING)
           | USING orc
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("orc_test")
    sql("DROP TABLE IF EXISTS t_refresh_orc")
  }

  test("Query orc Format in FiberCache") {
    withSQLConf(OapConf.OAP_ORC_BINARY_DATA_CACHE_ENABLED.key -> "true",
      SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
      val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
      data.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table orc_test select * from t")
      checkAnswer(sql("SELECT * FROM orc_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)
      checkAnswer(sql("SELECT * FROM orc_test WHERE a > 1 AND a <= 3"),
        Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
    }
  }
}
