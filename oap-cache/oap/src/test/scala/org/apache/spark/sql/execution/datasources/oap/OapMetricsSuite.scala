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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.Utils

class OapMetricsSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {
  private var currentPath: String = _

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
    currentPath = path
    sql(s"""CREATE TEMPORARY VIEW oap_test (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path')""".stripMargin)
    sql(s"""CREATE TEMPORARY VIEW parquet_test (a INT, b STRING)
           | USING parquet
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
    sqlContext.dropTempTable("parquet_test")
  }

  def getValue(metrics: Option[SQLMetric]): Long = metrics.map(_.value).getOrElse(0L)
  def accumulate(fileFormats: Set[Option[OapFileFormat]], func: OapFileFormat => Long): Long =
    fileFormats.foldLeft(0L)((sum, of) => sum + of.map(func).getOrElse(0L))

  case class RowMetrics(
      totalRows: Long,
      rowsSkippedForStatistic: Long,
      rowsReadWhenHitIndex: Long,
      rowsSkippedWhenHitIndex: Long,
      rowsReadWhenIgnoreIndex: Long,
      rowsReadWhenMissIndex: Long)

  case class TaskMetrics(
      totalTasks: Long,
      tasksSkippedForStatistic: Long,
      tasksHitIndex: Long,
      tasksIgnoreIndex: Long,
      tasksMissIndex: Long)

  def assertAccumulators(
      fileFormats: Set[Option[OapFileFormat]],
      rowMetrics: Option[RowMetrics] = None,
      taskMetrics: Option[TaskMetrics] = None): Unit = {
    rowMetrics.foreach(rowMetrics => {
      assert(rowMetrics.totalRows == accumulate(fileFormats,
        f => getValue(f.oapMetrics.totalRows)))
      assert(rowMetrics.rowsSkippedForStatistic == accumulate(fileFormats,
        f => getValue(f.oapMetrics.rowsSkippedForStatistic)))
      assert(rowMetrics.rowsReadWhenHitIndex == accumulate(fileFormats,
        f => getValue(f.oapMetrics.rowsReadWhenHitIndex)))
      assert(rowMetrics.rowsSkippedWhenHitIndex == accumulate(fileFormats,
        f => getValue(f.oapMetrics.rowsSkippedWhenHitIndex)))
      assert(rowMetrics.rowsReadWhenIgnoreIndex == accumulate(fileFormats,
        f => getValue(f.oapMetrics.rowsReadWhenIgnoreIndex)))
      assert(rowMetrics.rowsReadWhenMissIndex == accumulate(fileFormats,
        f => getValue(f.oapMetrics.rowsReadWhenMissIndex)))
    })

    taskMetrics.foreach(taskMetrics => {
      assert(taskMetrics.totalTasks == accumulate(fileFormats,
        f => getValue(f.oapMetrics.totalTasks)))
      assert(taskMetrics.tasksSkippedForStatistic == accumulate(fileFormats,
        f => getValue(f.oapMetrics.skipForStatisticTasks)))
      assert(taskMetrics.tasksHitIndex == accumulate(fileFormats,
        f => getValue(f.oapMetrics.hitIndexTasks)))
      assert(taskMetrics.tasksIgnoreIndex == accumulate(fileFormats,
        f => getValue(f.oapMetrics.ignoreIndexTasks)))
      assert(taskMetrics.tasksMissIndex == accumulate(fileFormats,
        f => getValue(f.oapMetrics.missIndexTasks)))
    })
  }

  def buildData(table: String, rowNum: Int): Unit = {
    val rowRDD = spark.sparkContext.parallelize(1 to rowNum, 3).map(i =>
      Seq(i, s"this is row $i")).map(Row.fromSeq)
    val schema =
      StructType(
        StructField("a", IntegerType) ::
          StructField("b", StringType) :: Nil)
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("t")

    sql(s"insert overwrite table $table select * from t")
  }

  def buildParquetData(rowNum: Int = 100): Unit = buildData("parquet_test", rowNum)

  def buildOapData(rowNum: Int = 100): Unit = buildData("oap_test", rowNum)

  test("test OAP accumulator on OapFileFormat") {
    buildOapData()
    sql("create oindex idx1 on oap_test (a)")

    // SQL 1: skipped task for statistic, OapFileFormat
    val df = sql("SELECT * FROM oap_test where a = 10000")
    checkAnswer(df, Nil)

    // rowsSkippedForStatistic = 100 && taskSkippedForStatistic = 3
    val fileFormats = getOapFileFormat(df.queryExecution.sparkPlan)
    assertAccumulators(fileFormats,
      Some(RowMetrics(100L, 100L, 0L, 0L, 0L, 0L)),
      Some(TaskMetrics(3L, 3L, 0L, 0L, 0L)))

    sql("drop oindex idx1 on oap_test")
  }

  test("test OAP accumulators on Parquet when hit index") {
    buildParquetData()
    sql("create oindex idx1 on parquet_test (a)")

    // SQL 1: hit index => hit 1 row
    val df = sql("SELECT * FROM parquet_test WHERE a = 1")
    checkAnswer(df, Row(1, "this is row 1") :: Nil)
    val ret1 = getColumnsHitIndex(df.queryExecution.sparkPlan)
    assert(ret1.keySet.size == 1 && ret1.keySet.head == "a")
    assert(ret1.values.head.toString == BTreeIndex(BTreeIndexEntry(0)::Nil).toString)

    // hitIndex && rowsRead = 1 && rowsSkipped = 99 && hitIndexTasks = 3
    val fileFormats = getOapFileFormat(df.queryExecution.sparkPlan)
    fileFormats.foreach(f => assert(f.orNull != null))
    assertAccumulators(fileFormats,
      Some(RowMetrics(100, 0, 1, 99, 0, 0)),
      Some(TaskMetrics(3, 0, 3, 0, 0)))

    sql("drop oindex idx1 on parquet_test")
  }

  test("test OAP accumulators on Parquet when join") {
    buildParquetData()
    sql("create oindex idx1 on parquet_test (a)")

    // SQL: join
    val df = sql("select t2.a, t2.b " +
      "from (select a from parquet_test where a = 1 ) t1 " +
      "inner join parquet_test t2 on t1.a = t2.a")
    checkAnswer(df, Row(1, "this is row 1") :: Nil)

    // rowsRead = 2 && rowsSkipped = 2 * 99
    // ReuseExchange optimization
    val fileFormats = getOapFileFormat(df.queryExecution.sparkPlan)
    fileFormats.foreach(f => assert(f.orNull != null))
    assertAccumulators(fileFormats,
      Some(RowMetrics(100, 0, 1, 99, 0, 0)),
      Some(TaskMetrics(3, 0, 3, 0, 0)))

    sql("drop oindex idx1 on parquet_test")
  }

  test("test OAP accumulators on Parquet when readBehavior return FULL_SCAN") {
    buildParquetData()
    sql("create oindex idx1 on parquet_test (a)")

    // satisfy indexFile / dataFile > 0.7
    sqlContext.conf.setConfString(OapConf.OAP_EXECUTOR_INDEX_SELECTION_FILE_POLICY.key, "true")

    // SQL 4: ignore index => readBehavior return FULL_SCAN
    val df = sql("SELECT * FROM parquet_test WHERE a = 1")
    checkAnswer(df, Row(1, "this is row 1") :: Nil)

    // rowsReadWhenIgnoreIndex = 100 && tasksIgnoreIndex = 3
    val fileFormats = getOapFileFormat(df.queryExecution.sparkPlan)
    assertAccumulators(fileFormats,
      Some(RowMetrics(100, 0, 0, 0, 100, 0)),
      Some(TaskMetrics(3, 0, 0, 3, 0)))

    sql("drop oindex idx1 on parquet_test")
    sqlContext.conf.setConfString(OapConf.OAP_EXECUTOR_INDEX_SELECTION_FILE_POLICY.key, "false")
  }

  test("test OAP accumulators on Oap when miss index") {
    buildOapData(99)

    // SQL 5: miss index
    val df = sql("SELECT * FROM oap_test where a = 1")
    checkAnswer(df, Row(1, "this is row 1") :: Nil)

    val fileFormats = getOapFileFormat(df.queryExecution.sparkPlan)
    // for task1(1~33) missIndex
    // for task2(34~66) and task3(67~99): filtered by statistic
    assertAccumulators(fileFormats,
      Some(RowMetrics(99, 66, 0, 0, 0, 33)),
      Some(TaskMetrics(3, 2, 0, 0, 1)))
  }
}
