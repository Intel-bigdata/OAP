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

package com.intel.sparkColumnarPlugin

import org.apache.spark.sql.{Row, SparkSession}

import org.scalatest.FunSuite

class ExtensionSuite extends FunSuite {

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  test("inject columnar exchange") {
    val session = SparkSession
      .builder()
      .master("local[1]")
      .config("org.apache.spark.example.columnar.enabled", value = true)
      .config("spark.sql.extensions", "com.intel.sparkColumnarPlugin.ColumnarPlugin")
      .appName("inject columnar exchange")
      .getOrCreate()

    try {
      import session.sqlContext.implicits._

      val input = Seq((100), (200), (300))
      val data = input.toDF("vals").repartition(1)
      val result = data.collect()
      assert(result sameElements input.map(x => Row(x)))
    } finally {
      stop(session)
    }
  }
}
