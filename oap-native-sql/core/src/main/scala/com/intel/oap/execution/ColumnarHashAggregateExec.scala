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

package com.intel.oap.execution

import com.intel.oap.ColumnarPluginConfig
import com.intel.oap.expression._
import com.intel.oap.vectorized._

import java.util.concurrent.TimeUnit._

import org.apache.spark.TaskContext
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{Utils, UserAddedJarUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}
import org.apache.spark.unsafe.KVIterator
import org.apache.spark.util.Utils

import scala.collection.Iterator

/**
 * Columnar Based HashAggregateExec.
 */
class ColumnarHashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
    extends HashAggregateExec(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child) {

  val sparkConf = sparkContext.getConf
  override def supportsColumnar = true

  // Disable code generation
  override def supportCodegen: Boolean = false

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of Input batches"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in aggregation process"),
    "totalTime" -> SQLMetrics
      .createTimingMetric(sparkContext, "totaltime_hashagg"))

  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")
  val numInputBatches = longMetric("numInputBatches")
  val aggTime = longMetric("aggTime")
  val totalTime = longMetric("totalTime")
  numOutputRows.set(0)
  numOutputBatches.set(0)
  numInputBatches.set(0)

  val (listJars, signature): (Seq[String], String) =
    if (ColumnarPluginConfig
          .getConf(sparkConf)
          .enableCodegenHashAggregate && groupingExpressions.nonEmpty) {
      val signature = ColumnarGroupbyHashAggregation.prebuild(
        groupingExpressions,
        child.output,
        aggregateExpressions,
        aggregateAttributes,
        resultExpressions,
        output,
        numInputBatches,
        numOutputBatches,
        numOutputRows,
        aggTime,
        totalTime,
        sparkConf)
      if (signature != "") {
        if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
          val tempDir = ColumnarPluginConfig.getRandomTempDir
          val jarFileName =
            s"${tempDir}/tmp/spark-columnar-plugin-codegen-precompile-${signature}.jar"
          sparkContext.addJar(jarFileName)
        }
        (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")), signature)
      } else {
        (List(), "")
      }
    } else {
      (List(), "")
    }
  listJars.foreach(jar => logInfo(s"Uploaded ${jar}"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitionsWithIndex { (partIndex, iter) =>
      val hasInput = iter.hasNext
      val res = if (!hasInput) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator.empty
      } else {
        if (ColumnarPluginConfig
              .getConf(sparkConf)
              .enableCodegenHashAggregate && groupingExpressions.nonEmpty) {
          val execTempDir = ColumnarPluginConfig.getTempFile
          val jarList = listJars
            .map(jarUrl => {
              logWarning(s"Get Codegened library Jar ${jarUrl}")
              UserAddedJarUtils.fetchJarFromSpark(
                jarUrl,
                execTempDir,
                s"spark-columnar-plugin-codegen-precompile-${signature}.jar",
                sparkConf)
              s"${execTempDir}/spark-columnar-plugin-codegen-precompile-${signature}.jar"
            })
          val aggregation = ColumnarGroupbyHashAggregation.create(
            groupingExpressions,
            child.output,
            aggregateExpressions,
            aggregateAttributes,
            resultExpressions,
            output,
            jarList,
            numInputBatches,
            numOutputBatches,
            numOutputRows,
            aggTime,
            totalTime,
            sparkConf)
          TaskContext
            .get()
            .addTaskCompletionListener[Unit](_ => {
              aggregation.close()
            })
          new CloseableColumnBatchIterator(aggregation.createIterator(iter))
        } else {
          var aggregation = ColumnarAggregation.create(
            partIndex,
            groupingExpressions,
            child.output,
            aggregateExpressions,
            aggregateAttributes,
            resultExpressions,
            output,
            numInputBatches,
            numOutputBatches,
            numOutputRows,
            aggTime,
            totalTime,
            sparkConf)
          TaskContext
            .get()
            .addTaskCompletionListener[Unit](_ => {
              aggregation.close()
            })
          new CloseableColumnBatchIterator(aggregation.createIterator(iter))
        }
      }
      res
    }
  }

}
