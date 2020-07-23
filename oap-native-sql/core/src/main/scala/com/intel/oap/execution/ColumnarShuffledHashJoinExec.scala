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

import java.util.concurrent.TimeUnit._

import com.intel.oap.vectorized._
import com.intel.oap.ColumnarPluginConfig

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{Utils, UserAddedJarUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import scala.collection.mutable.ListBuffer
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._

import io.netty.buffer.ArrowBuf
import com.google.common.collect.Lists;

import com.intel.oap.expression._
import com.intel.oap.vectorized.ExpressionEvaluator
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
class ColumnarShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
    extends ShuffledHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left, right) {

  val sparkConf = sparkContext.getConf
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_hashjoin"),
    "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "join time"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def supportsColumnar = true

  val numOutputRows = longMetric("numOutputRows")
  val totalTime = longMetric("totalTime")
  val joinTime = longMetric("joinTime")
  val buildTime = longMetric("buildTime")
  val resultSchema = this.schema

  //TODO() Disable code generation
  //override def supportCodegen: Boolean = false

  val signature =
    if (resultSchema.size > 0 && !leftKeys
          .filter(expr => bindReference(expr, left.output, true).isInstanceOf[BoundReference])
          .isEmpty && !rightKeys
          .filter(expr => bindReference(expr, right.output, true).isInstanceOf[BoundReference])
          .isEmpty) {

      ColumnarShuffledHashJoin.prebuild(
        leftKeys,
        rightKeys,
        resultSchema,
        joinType,
        buildSide,
        condition,
        left,
        right,
        buildTime,
        joinTime,
        totalTime,
        numOutputRows,
        sparkConf)
    } else {
      ""
    }
  val listJars = if (signature != "") {
    if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
      val tempDir = ColumnarPluginConfig.getRandomTempDir
      val jarFileName =
        s"${tempDir}/tmp/spark-columnar-plugin-codegen-precompile-${signature}.jar"
      sparkContext.addJar(jarFileName)
    }
    sparkContext.listJars.filter(path => path.contains(s"${signature}.jar"))
  } else {
    List()
  }
  listJars.foreach(jar => logInfo(s"Uploaded ${jar}"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) =>
        //val hashed = buildHashedRelation(buildIter)
        //join(streamIter, hashed, numOutputRows)
        ColumnarPluginConfig.getConf(sparkConf)
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

        val vjoin = ColumnarShuffledHashJoin.create(
          leftKeys,
          rightKeys,
          resultSchema,
          joinType,
          buildSide,
          condition,
          left,
          right,
          jarList,
          buildTime,
          joinTime,
          totalTime,
          numOutputRows,
          sparkConf)
        TaskContext
          .get()
          .addTaskCompletionListener[Unit](_ => {
            vjoin.close()
          })
        val vjoinResult = vjoin.columnarJoin(streamIter, buildIter)
        new CloseableColumnBatchIterator(vjoinResult)
    }
  }
}
