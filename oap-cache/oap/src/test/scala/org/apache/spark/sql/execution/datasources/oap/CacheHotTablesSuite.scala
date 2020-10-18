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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}
import org.apache.spark.util.Utils

abstract class CacheHotTablesSuite extends QueryTest with SharedOapContext with
  BeforeAndAfterEach {

  import testImplicits._

  protected def testTableName: String

  protected def fileFormat: String

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
    sql(s"""CREATE TABLE $testTableName (a INT, b STRING)
           | USING $fileFormat
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $testTableName")
  }

  protected def verifyProjectFilterScan(
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")
    val plan =
      sql(s"SELECT b FROM $testTableName WHERE b = 'this is test 1'")
        .queryExecution.optimizedPlan
    val optimizedSparkPlans = OapFileSourceStrategy(plan)
    assert(optimizedSparkPlans.size == 1)

    val optimizedSparkPlan = optimizedSparkPlans.head
    assert(optimizedSparkPlan.isInstanceOf[ProjectExec])
    assert(optimizedSparkPlan.children.nonEmpty)
    assert(optimizedSparkPlan.children.length == 1)

    val filter = optimizedSparkPlan.children.head
    assert(filter.isInstanceOf[FilterExec])
    assert(filter.children.nonEmpty)
    assert(filter.children.length == 1)

    val scan = filter.children.head
    assert(scan.isInstanceOf[FileSourceScanExec])
    val relation = scan.asInstanceOf[FileSourceScanExec].relation
    assert(relation.isInstanceOf[HadoopFsRelation])
    assert(verifyFileFormat(relation.fileFormat))

    val sparkPlans = FileSourceStrategy(plan)
    assert(sparkPlans.size == 1)
    val sparkPlan = sparkPlans.head
    assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))

  }

  protected def verifyProjectScan(
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    val plan = sql(s"SELECT a FROM $testTableName").queryExecution.optimizedPlan
    val optimizedSparkPlans = OapFileSourceStrategy(plan)
    assert(optimizedSparkPlans.size == 1)

    val optimizedSparkPlan = optimizedSparkPlans.head
    assert(optimizedSparkPlan.isInstanceOf[ProjectExec])
    assert(optimizedSparkPlan.children.nonEmpty)
    assert(optimizedSparkPlan.children.length == 1)

    val scan = optimizedSparkPlan.children.head
    assert(scan.isInstanceOf[FileSourceScanExec])
    val relation = scan.asInstanceOf[FileSourceScanExec].relation
    assert(relation.isInstanceOf[HadoopFsRelation])
    assert(verifyFileFormat(relation.fileFormat))

    val sparkPlans = FileSourceStrategy(plan)
    assert(sparkPlans.size == 1)
    val sparkPlan = sparkPlans.head
    assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))
  }

  protected def verifyScan(
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    val plan = sql(s"FROM $testTableName").queryExecution.optimizedPlan
    val optimizedSparkPlans = OapFileSourceStrategy(plan)
    assert(optimizedSparkPlans.size == 1)
    val optimizedSparkPlan = optimizedSparkPlans.head
    assert(optimizedSparkPlan.isInstanceOf[FileSourceScanExec])
    val relation = optimizedSparkPlan.asInstanceOf[FileSourceScanExec].relation
    assert(verifyFileFormat(relation.fileFormat))

    val sparkPlans = FileSourceStrategy(plan)
    assert(sparkPlans.size == 1)
    val sparkPlan = sparkPlans.head

    assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))
  }
}

class CacheHotTablesForParquetSuite extends CacheHotTablesSuite {
  protected def testTableName: String = "parquet_test"

  protected def fileFormat: String = "parquet"

  test("Project-> Filter -> Scan : Optimized") {
    withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true") {
      verifyProjectFilterScan(
        format => format.isInstanceOf[OptimizedParquetFileFormat],
        (plan1, plan2) => !plan1.sameResult(plan2)
      )
    }
  }

  test("Project-> Filter -> Scan : Not Optimized, only cache hot Tables") {
    withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true",
      OapConf.OAP_CACHE_TABLE_LISTS_ENABLED.key -> "true") {
      verifyProjectFilterScan(
        format => format.isInstanceOf[ParquetFileFormat],
        (plan1, plan2) => plan1.sameResult(plan2)
      )
    }
  }

  test("Project-> Filter -> Scan : Optimized, only cache hot Tables") {
    withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true",
      OapConf.OAP_CACHE_TABLE_LISTS_ENABLED.key -> "true",
      OapConf.OAP_CACHE_TABLE_LISTS.key -> "default.parquet_test;xxx") {
      verifyProjectFilterScan(
        format => format.isInstanceOf[OptimizedParquetFileFormat],
        (plan1, plan2) => !plan1.sameResult(plan2)
      )
    }
  }

  test("Project -> Scan : Optimized") {
    withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true") {
      verifyProjectScan(
        format => format.isInstanceOf[OptimizedParquetFileFormat],
        (plan1, plan2) => !plan1.sameResult(plan2)
      )
    }
  }

  test("Project -> Scan :  Not Optimized, only cache hot Tables") {
    withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true",
      OapConf.OAP_CACHE_TABLE_LISTS_ENABLED.key -> "true") {
      verifyProjectScan(
        format => format.isInstanceOf[ParquetFileFormat],
        (plan1, plan2) => plan1.sameResult(plan2)
      )
    }
  }

  test("Project -> Scan :  Optimized, only cache hot Tables") {
    withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true",
      OapConf.OAP_CACHE_TABLE_LISTS_ENABLED.key -> "true",
      OapConf.OAP_CACHE_TABLE_LISTS.key -> "default.parquet_test;xxx") {
      verifyProjectScan(
        format => format.isInstanceOf[OptimizedParquetFileFormat],
        (plan1, plan2) => !plan1.sameResult(plan2)
      )
    }
  }
}
