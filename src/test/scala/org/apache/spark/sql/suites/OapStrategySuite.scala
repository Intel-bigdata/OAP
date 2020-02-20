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
package org.apache.spark.sql.suites

import org.apache.spark.sql.{BenchmarkConfig, OapBenchmarkDataBuilder, OapStrategyConfigSet, OapTestSuite}
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.internal.oap.OapConf

object OapStrategySuite extends OapTestSuite with OapStrategyConfigSet  {

  override def beforeAll(conf: Map[String, String] = Map.empty): Unit = {
    super.beforeAll(conf)
    spark.experimental.extraStrategies = spark.sessionState.planner.extraPlanningStrategies
  }

  override def afterAll(): Unit = {
    spark.experimental.extraStrategies = null
    super.afterAll()
  }

  private def databaseName =
    OapBenchmarkDataBuilder.getDatabase(activeConf.getBenchmarkConf(BenchmarkConfig.FILE_FORMAT))

  private def isDataBaseExists: Boolean = {
    if (spark.sqlContext.sql(s"show databases").collect().exists(_.getString(0) == databaseName)) {
      spark.sqlContext.sql(s"USE $databaseName")
      true
    } else {
      sys.error(s"ERROR: $databaseName does not exist!")
      false
    }
  }

  private def isTableReady: Boolean = {
    if (spark.sqlContext.sql(s"show tables").collect().exists(_.getString(1) == table)) {
      val conf = activeConf
      if (conf.getBenchmarkConf(BenchmarkConfig.INDEX_ENABLE) == "true") {
        if (spark.sqlContext.sql(s"show oindex from $table")
          .collect().exists(_.getString(3) == btreeIndexAttr)) {
          if (spark.sqlContext.sql(s"show oindex from $table")
            .collect().exists(_.getString(3) == bitmapIndexAttr)) {
            true
          } else {
            sys.error(s"ERROR: index on $bitmapIndexAttr does not exist!")
            false
          }
        } else {
          sys.error(s"ERROR: index on $btreeIndexAttr does not exist!")
          false
        }
      } else {
        true
      }
    } else {
      sys.error(s"ERROR: table $table does not exist!")
      false
    }
  }

  private def isDataReady(): Boolean = isDataBaseExists && isTableReady

  private def setRunningParams(): Boolean = {
    val conf = activeConf
    if (conf.getBenchmarkConf(BenchmarkConfig.INDEX_ENABLE) == "false") {
      spark.sqlContext.conf.setConf(OapConf.OAP_ENABLE_OINDEX, false)
    }

    conf.allSparkOptions().foreach{ setting =>
      spark.sqlContext.setConf(setting._1, setting._2)
    }

    spark.sqlContext.sql(s"USE $databaseName")
    true
  }

  override def prepare(): Boolean = {
    if (isDataReady()) {
      setRunningParams()
    } else {
      false
    }
  }

  val table = "store_sales"

  val leftTable = "store_sales_dup"

  val btreeIndexAttr = "ss_customer_sk"

  val bitmapIndexAttr = "ss_item_sk1"

  val range1to100 = (1 to 100).mkString(",")

  override def testSet: Seq[OapBenchmarkTest] = Seq(
    // Order by limit
    OapBenchmarkTest("Limit 10 from whole table",
      s"SELECT * FROM $table WHERE $btreeIndexAttr > 0 ORDER BY $btreeIndexAttr LIMIT 10"),
    OapBenchmarkTest("Limit 10 in range [0, 20000]",
      s"SELECT * FROM $table WHERE $btreeIndexAttr BETWEEN 0 AND 20000 ORDER BY $btreeIndexAttr LIMIT 10"),

    // Semi join
    // TODO: add semi join test case.

    // Aggregation
    OapBenchmarkTest("btree index aggregation value in 1 to 100",
      s"SELECT $btreeIndexAttr, max(${bitmapIndexAttr}) FROM $table " +
        s"WHERE $btreeIndexAttr IN ( $range1to100 ) " +
        s"GROUP BY $btreeIndexAttr"),
      OapBenchmarkTest("btree index aggregation value < 100000",
      s"SELECT $btreeIndexAttr, max(${bitmapIndexAttr}) FROM $table " +
        s"WHERE $btreeIndexAttr < 100000 " +
        s"GROUP BY $btreeIndexAttr")
  )
}
