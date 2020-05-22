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

package org.apache.spark.sql.execution.datasources.oap.filecache

import org.scalatest.BeforeAndAfterEach
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.test.oap.SharedOapContext

class TaskLocationSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    sql(s"""CREATE TABLE parquet_test (a int, b string, c int)
           | USING parquet
           | PARTITIONED by (c)""".stripMargin)
    OapRuntime.getOrCreate.fiberCacheManager.clearAllFibers()
    OapRuntime.getOrCreate.fiberSensor.executorToCacheManager.clear()
  }

  override def afterEach(): Unit = {
    sql("DROP TABLE IF EXISTS parquet_test")
  }

  test("task locations of query job before and after cache column") {
    val data: Seq[(Int, String, Int)] = (1 to 100).map { i => (i, s"this is test $i", i % 2) }
    data.toDF("column_1", "column_2", "column_3").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    val noCacheRdd = spark.sql("SELECT * FROM parquet_test").rdd
    val noCacheLocations = noCacheRdd.partitions.indices.map(
      spark.sparkContext.dagScheduler.getPreferredLocs(noCacheRdd, _))
    assert(noCacheLocations.forall(_.isEmpty))
    sql("cache columns on parquet_test (b)")
    val cachedRdd = spark.sql("SELECT * FROM parquet_test").rdd
    val cachedLocations = cachedRdd.partitions.indices.map(
      spark.sparkContext.dagScheduler.getPreferredLocs(cachedRdd, _))
    assert(cachedLocations.forall(_.nonEmpty))
    // after cache column, the task locations should be ExecutorCacheTaskLocation
    assert(cachedLocations.forall(locations =>
      locations.forall(_.isInstanceOf[ExecutorCacheTaskLocation])))
  }
}
