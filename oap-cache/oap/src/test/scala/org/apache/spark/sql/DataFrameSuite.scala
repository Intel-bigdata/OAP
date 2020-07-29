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

package org.apache.spark.sql

import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.{SharedSQLContext}
import org.apache.spark.util.Utils

class DataFrameSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("reuse exchange OAP") {
    val path = Utils.createTempDir().getAbsolutePath
    sql(s"""CREATE TEMPORARY VIEW parquet_test (a INT, b STRING)
           | USING parquet
           | OPTIONS (path '$path')""".stripMargin)
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "2",
      OapConf.OAP_PARQUET_BINARY_DATA_CACHE_ENABLED.key -> "true") {
      val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
      data.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table parquet_test select * from t")
      val df = sql("select * from parquet_test")
      val join = df.join(df, "a")
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "a").join(broadcasted, "a")
      join2.explain()
      assert(
        join2.queryExecution.executedPlan
          .collect { case _: ReusedExchangeExec => true }.size == 4)
    }
    sqlContext.dropTempTable("parquet_test")
  }

  test("reuse exchange") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "2") {
      val df = spark.range(100).toDF()
      val join = df.join(df, "id")
      val plan = join.queryExecution.executedPlan
      checkAnswer(join, df)
      assert(
        join.queryExecution.executedPlan.collect { case e: ShuffleExchangeExec => true }.size === 1)
      assert(
        join.queryExecution.executedPlan.collect { case e: ReusedExchangeExec => true }.size === 1)
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "id").join(broadcasted, "id")
      checkAnswer(join2, df)
      assert(
        join2.queryExecution.executedPlan.collect { case e: ShuffleExchangeExec => true }.size == 1)
      assert(
        join2.queryExecution.executedPlan
          .collect { case e: BroadcastExchangeExec => true }.size === 1)
      assert(
        join2.queryExecution.executedPlan.collect { case e: ReusedExchangeExec => true }.size == 4)
    }
  }

}


