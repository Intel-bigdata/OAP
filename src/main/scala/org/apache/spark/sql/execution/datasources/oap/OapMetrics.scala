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

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.datasources.oap.io.OapDataReader
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

private[sql] class OapMetrics extends Serializable {
  /**
   * 4 kinds of Tasks and 5 kinds of Rows:
   *   1.skipForStatisticTasks
   *     a.rowsSkippedForStatistic
   *   2.hitIndexTasks
   *     a.rowsReadWhenHitIndex
   *     b.rowsSkippedWhenHitIndex
   *   3.ignoreIndexTasks (hit index but ignore)
   *     a.rowsReadWhenIgnoreIndex
   *   4.missIndexTasks
   *     a.rowsReadWhenMissIndex
   */
  private var _totalTasks: Option[SQLMetric] = None
  private var _skipForStatisticTasks: Option[SQLMetric] = None
  private var _hitIndexTasks: Option[SQLMetric] = None
  private var _ignoreIndexTasks: Option[SQLMetric] = None
  private var _missIndexTasks: Option[SQLMetric] = None

  private var _totalRows: Option[SQLMetric] = None
  private var _rowsSkippedForStatistic: Option[SQLMetric] = None
  private var _rowsReadWhenHitIndex: Option[SQLMetric] = None
  private var _rowsSkippedWhenHitIndex: Option[SQLMetric] = None
  private var _rowsReadWhenIgnoreIndex: Option[SQLMetric] = None
  private var _rowsReadWhenMissIndex: Option[SQLMetric] = None

  def initMetrics(metrics: Map[String, SQLMetric]): Unit = {
    // set task-level Accumulator
    _totalTasks = Some(metrics(OapMetrics.totalTasksName))
    _skipForStatisticTasks = Some(metrics(OapMetrics.skipForStatisticTasksName))
    _hitIndexTasks = Some(metrics(OapMetrics.hitIndexTaskName))
    _ignoreIndexTasks = Some(metrics(OapMetrics.ignoreIndexTasksName))
    _missIndexTasks = Some(metrics(OapMetrics.missIndexTasksName))

    // set row-level Accumulator
    _totalRows = Some(metrics(OapMetrics.totalRowsName))
    _rowsSkippedForStatistic = Some(metrics(OapMetrics.rowsSkippedForStatisticName))
    _rowsReadWhenHitIndex = Some(metrics(OapMetrics.rowsReadWhenHitIndexName))
    _rowsSkippedWhenHitIndex = Some(metrics(OapMetrics.rowsSkippedWhenHitIndexName))
    _rowsReadWhenIgnoreIndex = Some(metrics(OapMetrics.rowsReadWhenIgnoreIndexName))
    _rowsReadWhenMissIndex = Some(metrics(OapMetrics.rowsReadWhenMissIndexName))
  }

  def totalTasks: Option[SQLMetric] = _totalTasks
  def skipForStatisticTasks: Option[SQLMetric] = _skipForStatisticTasks
  def hitIndexTasks: Option[SQLMetric] = _hitIndexTasks
  def ignoreIndexTasks: Option[SQLMetric] = _ignoreIndexTasks
  def missIndexTasks: Option[SQLMetric] = _missIndexTasks

  def totalRows: Option[SQLMetric] = _totalRows
  def rowsSkippedForStatistic: Option[SQLMetric] = _rowsSkippedForStatistic
  def rowsReadWhenHitIndex: Option[SQLMetric] = _rowsReadWhenHitIndex
  def rowsSkippedWhenHitIndex: Option[SQLMetric] = _rowsSkippedWhenHitIndex
  def rowsReadWhenIgnoreIndex: Option[SQLMetric] = _rowsReadWhenIgnoreIndex
  def rowsReadWhenMissIndex: Option[SQLMetric] = _rowsReadWhenMissIndex

  private def hitIndex(readRows: Long, skippedRows: Long): Unit = {
    _hitIndexTasks.foreach(_.add(1L))
    _rowsReadWhenHitIndex.foreach(_.add(readRows))
    _rowsSkippedWhenHitIndex.foreach(_.add(skippedRows))
  }

  private def missIndex(rows: Long): Unit = {
    _missIndexTasks.foreach(_.add(1L))
    _rowsReadWhenMissIndex.foreach(_.add(rows))
  }

  private def ignoreIndex(rows: Long): Unit = {
    _ignoreIndexTasks.foreach(_.add(1L))
    _rowsReadWhenIgnoreIndex.foreach(_.add(rows))
  }

  def skipForStatistic(rows: Long): Unit = {
    _skipForStatisticTasks.foreach(_.add(1L))
    _rowsSkippedForStatistic.foreach(_.add(rows))
  }

  def updateTotalRows(rows: Long): Unit = {
    _totalRows.foreach(_.add(rows))
    _totalTasks.foreach(_.add(1L))
  }

  def updateIndexAndRowRead(r: OapDataReader, totalRows: Long): Unit = {
    import org.apache.spark.sql.execution.datasources.oap.INDEX_STAT._
    r.indexStat match {
      case HIT_INDEX =>
        val rows = r.rowsReadByIndex.getOrElse(0L)
        hitIndex(rows, totalRows - rows)
      case IGNORE_INDEX =>
        ignoreIndex(totalRows)
      case MISS_INDEX =>
        missIndex(totalRows)
    }
  }
}

private[sql] object OapMetrics {
  val totalTasksName = "totalTasks"
  val skipForStatisticTasksName = "skipForStatisticTasks"
  val hitIndexTaskName = "hitIndexTasks"
  val ignoreIndexTasksName = "ignoreIndexTasks"
  val missIndexTasksName = "missIndexTasks"
  val totalRowsName = "totalRows"
  val rowsSkippedForStatisticName = "rowsSkippedForStatistic"
  val rowsReadWhenHitIndexName = "rowsReadWhenHitIndex"
  val rowsSkippedWhenHitIndexName = "rowsSkippedWhenHitIndex"
  val rowsReadWhenIgnoreIndexName = "rowsReadWhenIgnoreIndex"
  val rowsReadWhenMissIndexName = "rowsReadWhenMissIndex"

  def metrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(totalTasksName ->
      SQLMetrics.createMetric(sparkContext, "OAP:total tasks"),
      skipForStatisticTasksName ->
        SQLMetrics.createMetric(sparkContext, "OAP:tasks can skip"),
      hitIndexTaskName ->
        SQLMetrics.createMetric(sparkContext, "OAP:tasks hit index"),
      ignoreIndexTasksName ->
        SQLMetrics.createMetric(sparkContext, "OAP:tasks ignore index"),
      missIndexTasksName ->
        SQLMetrics.createMetric(sparkContext, "OAP:tasks miss index"),

      totalRowsName ->
        SQLMetrics.createMetric(sparkContext, "OAP:total rows"),
      rowsSkippedForStatisticName ->
        SQLMetrics.createMetric(sparkContext, "OAP:rows skipped when task skipped"),
      rowsReadWhenHitIndexName ->
        SQLMetrics.createMetric(sparkContext, "OAP:rows read when hit index"),
      rowsSkippedWhenHitIndexName ->
        SQLMetrics.createMetric(sparkContext, "OAP:rows skipped when hit index"),
      rowsReadWhenIgnoreIndexName ->
        SQLMetrics.createMetric(sparkContext, "OAP:rows read when ignore index"),
      rowsReadWhenMissIndexName ->
        SQLMetrics.createMetric(sparkContext, "OAP:rows read when miss index")
    )
}
