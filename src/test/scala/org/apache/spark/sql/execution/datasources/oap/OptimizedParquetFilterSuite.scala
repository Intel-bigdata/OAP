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
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}
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

    sql(s"""CREATE TABLE partitioned_parquet (a int, b STRING, c int)
           | USING parquet
           | PARTITIONED by (c)""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("parquet_test")
    sql("DROP TABLE IF EXISTS partitioned_parquet")
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

  test("disable index and use data cache independent") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")

    withIndex(TestIndex("parquet_test", "index1")) {
      withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true",
        OapConf.OAP_PARQUET_INDEX_ENABLED.key -> "false") {
        sql("create oindex index1 on parquet_test (b)")
        val df = sql("SELECT b FROM parquet_test WHERE b = 'this is test 1'")
        checkAnswer(df, Row("this is test 1") :: Nil)
        val plans = new ArrayBuffer[SparkPlan]
        df.queryExecution.executedPlan.foreach(node => plans.append(node))
        val dataSources = plans.filter(p => p.isInstanceOf[DataSourceScanExec])
        assert(dataSources.nonEmpty)
        dataSources.foreach(p =>
          p.asInstanceOf[DataSourceScanExec].relation match {
            case h: HadoopFsRelation =>
              assert(h.fileFormat.isInstanceOf[OptimizedParquetFileFormat])
              val format = h.fileFormat.asInstanceOf[OptimizedParquetFileFormat]
              assert(format.getHitIndexColumns.isEmpty)
            case _ => assert(false)
          }
        )
      }
    }
  }

  test("OAP#1031- query failed when Some index file not exists") {
    val data: Seq[(Int, String, Int)] = (1 to 300).map { i => (i, s"this is test $i", i % 2) }
    data.toDF("key", "value1", "value2").createOrReplaceTempView("t")
    sql(
      """
        |INSERT OVERWRITE TABLE partitioned_parquet
        |partition (c=0)
        |SELECT key, value1 from t where value2 = 0
      """.stripMargin)

    sql(
      """
        |INSERT OVERWRITE TABLE partitioned_parquet
        |partition (c=1)
        |SELECT key, value1 from t where value2 = 1
      """.stripMargin)

    withIndex(TestIndex("partitioned_parquet", "index1"),
      TestIndex("partitioned_parquet", "index2")) {
      withSQLConf(OapConf.OAP_INDEXER_CHOICE_MAX_SIZE.key -> "2") {
        // create index
        sql("create oindex index1 on partitioned_parquet (b)")
        sql("create oindex index2 on partitioned_parquet (a) partition(c = 0)")

        // push down indexScanners a and b , but b not exists.
        val df = sql("SELECT b FROM partitioned_parquet WHERE b = 'this is test 1' and a = 1")
        checkAnswer(df, Row("this is test 1") :: Nil)
      }
    }
  }

  test("OAP#1038 Index can be used when timestamps is constant on partitioned table") {
    val data: Seq[(Int, String, Int)] = (1 to 300).map { i => (i, s"this is test $i", i % 2) }
    data.toDF("key", "value1", "value2").createOrReplaceTempView("t")
    sql(
      """
        |INSERT OVERWRITE TABLE partitioned_parquet
        |partition (c=0)
        |SELECT key, value1 from t where value2 = 0
      """.stripMargin)
    sql(
      """
        |INSERT OVERWRITE TABLE partitioned_parquet
        |partition (c=1)
        |SELECT key, value1 from t where value2 = 1
      """.stripMargin)
    withIndex(TestIndex("partitioned_parquet", "indexA")) {
      withSQLConf(OapConf.OAP_INDEXER_USE_CONSTANT_TIMESTAMPS_ENABLED.key -> "true") {
        // create index
        sql("create oindex indexA on partitioned_parquet (a) partition(c = 0)")
        sql("create oindex indexA on partitioned_parquet (a) partition(c = 1)")
        val beforeQuery = OapRuntime.getOrCreate.fiberCacheManager.cacheStats.indexFiberCount
        val df = sql("SELECT b FROM partitioned_parquet WHERE a = 1 or a = 2")
        checkAnswer(df, Row("this is test 1") :: Row("this is test 2") :: Nil)
        // After `INSERT TABLE` there are 4 files, there are 4 footer fiber need load,
        // 2 files need load node pos fiber and rowIdList fiber, so after query increment is 8.
        // If `OAP_INDEXER_USE_CONSTANT_TIMESTAMPS_ENABLED` if false, the indexFiberCount only 4
        // because one of partition not use index.
        val afterQuery = OapRuntime.getOrCreate.fiberCacheManager.cacheStats.indexFiberCount
        assert(afterQuery == beforeQuery + 8)
      }
    }
  }
}
