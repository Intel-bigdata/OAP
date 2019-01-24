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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.OapSparkSqlParser
import org.apache.spark.sql.execution.datasources.OapFileSourceStrategy
import org.apache.spark.sql.execution.datasources.oap._

class OapExtensionsSuite extends SparkFunSuite {

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  test("Use OapExtensions SparkSession") {
    val withOapExtensionsSession = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions", classOf[OapExtensions].getCanonicalName)
      .getOrCreate()
    try {
      val oapStrategies = withOapExtensionsSession.sessionState.planner.extraPlanningStrategies
      val expected = Seq(OapSortLimitStrategy, OapSemiJoinStrategy, OapGroupAggregateStrategy,
        OapFileSourceStrategy)
      assert(oapStrategies.nonEmpty)
      assert(oapStrategies.length == 4)
      assert(oapStrategies.equals(expected))
      val sqlParser = withOapExtensionsSession.sessionState.sqlParser
      assert(sqlParser.isInstanceOf[OapSparkSqlParser])
    } finally {
      stop(withOapExtensionsSession)
    }
  }
}
