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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Helper trait for OAP SQL test suites where all tests share a single [[TestOapSession]].
 */
trait OapSharedSQLContext extends SQLTestUtils {

  protected val sparkConf = new SparkConf()

  /**
    * The [[TestOapSession]] to use for all tests in this suite.
    *
    * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
    * mode with the default test configurations.
    */
  private var _spark: TestOapSession = _

  /**
    * The [[TestOapSession]] to use for all tests in this suite.
    */
  protected implicit def spark: SparkSession = _spark

  /**
    * The [[TestOapContext]] to use for all tests in this suite.
    */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  /**
    * Initialize the [[TestOapSession]].
    */
  protected override def beforeAll(): Unit = {
    SparkSession.sqlListener.set(null)
    if (_spark == null) {
      _spark = new TestOapSession(sparkConf)
    }
    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
    // A small TRICK here. Clear database before testing to avoid the "table already exists" problem
    sql("DROP DATABASE IF EXISTS oap_test_database CASCADE")
    sql("CREATE DATABASE oap_test_database")
    sql("USE oap_test_database")

  }

  /**
    * Stop the underlying [[org.apache.spark.SparkContext]], if any.
    */
  protected override def afterAll(): Unit = {
    try {
      if (_spark != null) {
        _spark.stop()
        _spark = null
      }
    } finally {
      super.afterAll()
    }
  }
}
