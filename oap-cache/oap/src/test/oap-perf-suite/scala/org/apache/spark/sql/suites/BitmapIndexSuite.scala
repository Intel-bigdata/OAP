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

import org.apache.spark.sql.{BenchmarkConfig, OapBenchmarkDataBuilder, OapTestSuite, ParquetVsOrcConfigSet}
import org.apache.spark.sql.internal.oap.OapConf

object BitmapIndexSuite extends OapTestSuite with ParquetVsOrcConfigSet {
  override protected def getAppName: String = "BitmapIndexBenchmarkSuite"

  private val table = "store_sales"

  private val attr = "ss_item_sk1"

  private val range1to10 = (1 to 10).mkString(",")

  private val range1to5 = (1 to 5).mkString(",")

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
              .collect().exists(_.getString(3) == attr)) {
          true
        } else {
          sys.error(s"ERROR: index on $attr does not exist!")
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

  /**
   * (name, sql sentence, TODO: profile, etc)
   */
  override def testSet = Seq(
    OapBenchmarkTest("attr in range1to10",
      s"SELECT * FROM $table WHERE $attr in ( $range1to10 )"),
    OapBenchmarkTest("attr in range1to5",
      s"SELECT * FROM $table WHERE $attr in ( $range1to5 )"),
    // Two columns query
    OapBenchmarkTest("attr in range1to10 & ss_customer_sk >= 120000",
      s"SELECT * FROM $table WHERE $attr in ( $range1to10 ) AND ss_customer_sk >= 120000"),
    OapBenchmarkTest("attr in range1to5 & ss_list_price < 100.0",
      s"SELECT * FROM $table WHERE $attr in ( $range1to5 ) AND ss_list_price < 100.0"),
    // Three columns query
    OapBenchmarkTest("attr in range1to10 & ss_customer_sk >= 120000 & ss_list_price < 100.0",
      s"SELECT * FROM $table WHERE $attr in ( $range1to10 ) AND ss_customer_sk >= 120000 AND ss_list_price < 100.0"),
    OapBenchmarkTest("attr in range1to5 & ss_list_price < 100.0 & ss_net_paid > 500.0",
      s"SELECT * FROM $table WHERE $attr in ( $range1to5 ) AND ss_list_price < 100.0 AND ss_net_paid > 500.0"),
    OapBenchmarkTest("attr=10 & ss_net_paid>100.0 & ss_net_paid<200.0 & ss_list_price<100.0",
      s"SELECT * FROM $table WHERE $attr = 10 AND ss_net_paid > 100.0 AND ss_net_paid < 200.0 AND ss_list_price < 100.0")
  )
}
