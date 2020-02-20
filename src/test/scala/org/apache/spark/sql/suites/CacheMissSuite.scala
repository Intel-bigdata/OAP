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

import org.apache.spark.sql.{BenchmarkConfig, CacheMissConfigSet, OapBenchmarkDataBuilder, OapTestSuite}

object CacheMissSuite extends OapTestSuite with CacheMissConfigSet {

  val table = "store_sales"

  val attr = "ss_customer_sk"

  private def databaseName = OapBenchmarkDataBuilder.getDatabase(activeConf.getBenchmarkConf(BenchmarkConfig.FILE_FORMAT))

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
    spark.sqlContext.sql(s"show tables").collect().exists(_.getString(1) == table)
  }

  private def isDataReady(): Boolean = isDataBaseExists && isTableReady

  private def setRunningParams(): Boolean = {
    val conf = activeConf
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


  override def testSet: Seq[OapBenchmarkTest] = Seq(
    OapBenchmarkTest("full scan table",
      s"SELECT * FROM $table WHERE $attr < ${Int.MaxValue}"),
    OapBenchmarkTest("attr < 10000",
      s"SELECT * FROM $table WHERE $attr < 10000")
  )
}
