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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.{DataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class OptimizedParquetFilterSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {
  // TODO move Parquet TestSuite from FilterSuite
  import testImplicits._

  private var currentPath: String = _
  private var defaultEis: Boolean = true

  override def beforeAll(): Unit = {
    super.beforeAll()
    // In this suite we don't want to skip index even if the cost is higher.
    defaultEis = sqlContext.conf.getConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION)
    sqlContext.conf.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION, false)
  }

  override def afterAll(): Unit = {
    sqlContext.conf.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION, defaultEis)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
    currentPath = path
    sql(s"""CREATE TEMPORARY VIEW parquet_test (a INT, b STRING)
           | USING parquet
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("parquet_test")
  }

  test("enable data cache but no .oap.meta file") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")

    withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true") {
      val df = sql("SELECT b FROM parquet_test WHERE b = 'this is test 1'")
      checkAnswer(df, Row("this is test 1") :: Nil)
      val plans = new ArrayBuffer[SparkPlan]
      df.queryExecution.executedPlan.foreach(node => plans.append(node))
      val dataSources = plans.filter(p => p.isInstanceOf[DataSourceScanExec])
      assert(dataSources.nonEmpty)
      dataSources.foreach(p =>
        p.asInstanceOf[DataSourceScanExec].relation match {
          case h: HadoopFsRelation =>
            assert(h.fileFormat.isInstanceOf[OapFileFormat])
          case _ => assert(false)
        }
      )
    }
  }
}
