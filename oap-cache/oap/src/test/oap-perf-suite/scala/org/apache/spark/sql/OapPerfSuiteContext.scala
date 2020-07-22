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

/*
 * Benchmark Session.
 */
package org.apache.spark.sql

import sys.process._
import org.apache.spark._
import org.apache.spark.sql.oap.OapRuntime

trait OapPerfSuiteContext {

  /**
    * The [[SparkSession]] to use for all tests in this suite.
    *
    * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
    * mode with the default test configurations.
    */
  private var _spark: SparkSession = null

  /**
    * The [[SparkSession]] to use for all tests in this suite.
    */
  protected implicit def spark: SparkSession = _spark

  /**
    * The [[SparkSession]] to use for all tests in this suite.
    */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  def isBootStrapping = false
  protected def createSparkSession(conf: Map[String, String] = Map.empty): SparkSession = {
    if (isBootStrapping) {
      val sparkConf = new SparkConf().set("spark.sql.testkey", "true")
      conf.foreach(option => sparkConf.set(option._1, option._2))

      new SparkSession(
        // TODO: support s"local-cluster[2, 1, ${4*1024*1024}]",
        new SparkContext(
          "local[2]",
          "test-sql-context",
          sparkConf
        )
      )
    } else {
      val builder = SparkSession.builder().appName(getAppName)
      conf.foreach(option => builder.config(option._1, option._2))
      builder.enableHiveSupport().getOrCreate()
    }
  }

  protected def getAppName: String = "defaultTest"

  /**
   * Initialize the [[SparkSession]].
   */
  def beforeAll(conf: Map[String, String] = Map.empty): Unit = {
    if (_spark == null) {
      _spark = createSparkSession(conf)
      SparkSession.setActiveSession(_spark)
    }
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
  def afterAll(): Unit = {
    if (_spark != null) {
      SparkSession.clearActiveSession()
      OapRuntime.stop()
      _spark.stop()
      _spark = null
      assert(("rm -f ./metastore_db/db.lck" !) == 0)
      assert(("rm -f ./metastore_db/dbex.lck" !) == 0)
      assert(("sync" !) == 0)

      if (!isBootStrapping) {
        val dropCacheResult = Seq("bash", "-c", "echo 3 > /proc/sys/vm/drop_caches").!
        assert(dropCacheResult == 0)
      }
    }
  }
}
