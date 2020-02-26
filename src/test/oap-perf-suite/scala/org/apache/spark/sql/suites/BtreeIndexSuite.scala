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

import org.apache.spark.sql._
import org.apache.spark.sql.internal.oap.OapConf

object BtreeIndexSuite
    extends OapTestSuite with OapPerfSuiteContext with ParquetVsOapVsOrcConfigSet {
  override protected def getAppName: String = "BtreeIndexBenchmarkSuite"

  val table = "store_sales"

  val attr = "ss_customer_sk"

  def databaseName =
    OapBenchmarkDataBuilder.getDatabase(activeConf.getBenchmarkConf(BenchmarkConfig.FILE_FORMAT))

  private def isDataBaseReady: Boolean = {
    val dbCandidates = spark.sqlContext.sql(s"show databases").collect()
    if (dbCandidates.exists(_.getString(0) == databaseName)) {
      spark.sqlContext.sql(s"USE $databaseName")
      true
    } else {
      logError(s"$dbCandidates does not contain $databaseName!")
      false
    }
  }

  private def isTableReady: Boolean = {
    val tables = spark.sqlContext.sql(s"show tables").collect()
    if (tables.exists(_.getString(1) == table)) {
      val conf = activeConf
      if (conf.getBenchmarkConf(BenchmarkConfig.INDEX_ENABLE) == "true"){
        // Check if index exists.
        spark.sqlContext.sql(s"show oindex from $table").collect().exists(_.getString(3) == attr)
      } else {
        true
      }
    } else {
      logError(s"$tables does not contain $table!")
      false
    }
  }

  private def isDataReady(): Boolean = isDataBaseReady && isTableReady

  private def setRunningParams(): Boolean = {
    val conf = activeConf
    if (conf.getBenchmarkConf(BenchmarkConfig.INDEX_ENABLE) == "false"){
      spark.sqlContext.conf.setConf(OapConf.OAP_ENABLE_OINDEX, false)
    }

    true
  }

  override def prepare(): Boolean = {
    if (isDataReady()) {
      setRunningParams()
    } else {
      sys.error("ERROR: Data is not ready!")
      false
    }
  }

  /**
   * (name, sql sentence, TODO: profile, etc)
   */
  override def testSet = Seq(
    OapBenchmarkTest("attr < Int.MaxValue",
      s"SELECT * FROM $table WHERE $attr < ${Int.MaxValue}"),
    OapBenchmarkTest("attr < 100000",
      s"SELECT * FROM $table WHERE $attr < 100000"),
    OapBenchmarkTest("attr < 10000",
      s"SELECT * FROM $table WHERE $attr < 10000"),
    OapBenchmarkTest("attr < 1000",
      s"SELECT * FROM $table WHERE $attr < 1000"),
    OapBenchmarkTest("attr = 600000",
      s"SELECT * FROM $table WHERE $attr = 600000"),
    OapBenchmarkTest("attr BETWEEN 10 & 20",
      s"SELECT * FROM $table WHERE $attr BETWEEN 10 AND 20"),
    OapBenchmarkTest("attr BETWEEN 10 & 40",
      s"SELECT * FROM $table WHERE $attr BETWEEN 10 AND 40"),
    OapBenchmarkTest("attr BETWEEN 10 & 80",
      s"SELECT * FROM $table WHERE $attr BETWEEN 10 AND 80"),
    
    // Two columns query
    OapBenchmarkTest("attr < 100000 & ss_ticket_number >= 120000",
      s"SELECT * FROM $table WHERE $attr < 100000 AND ss_ticket_number >= 120000"),
    OapBenchmarkTest("attr < 10000 & ss_list_price < 100.0",
      s"SELECT * FROM $table WHERE $attr < 10000 AND ss_list_price < 100.0"),
    OapBenchmarkTest("attr < 1000 & ss_net_paid < 200.0",
      s"SELECT * FROM $table WHERE $attr < 1000 AND ss_net_paid > 100.0 AND ss_net_paid < 200.0"),
    OapBenchmarkTest("attr < 1000 & ss_net_paid in [100.0, 110.0]",
      s"SELECT * FROM $table WHERE $attr < 1000 AND ss_net_paid BETWEEN 100.0 AND 110.0"),

    // Three columns query
    OapBenchmarkTest("attr < 100000 & ss_ticket_number >= 120000 & ss_list_price < 100.0",
      s"SELECT * FROM $table WHERE $attr < 100000 AND ss_ticket_number >= 120000 AND ss_list_price < 100.0"),
    OapBenchmarkTest("attr < 10000 & ss_list_price < 100.0 & ss_net_paid > 500.0",
      s"SELECT * FROM $table WHERE $attr < 10000 AND ss_list_price < 100.0 AND ss_net_paid > 500.0"),
    OapBenchmarkTest("attr < 1000 & ss_net_paid > 100.0 & ss_net_paid < 110.0 & ss_list_price < 100.0",
      s"SELECT * FROM $table WHERE $attr < 1000 AND ss_net_paid > 100.0 AND ss_net_paid < 110.0 AND ss_list_price < 100.0")
  )
}
