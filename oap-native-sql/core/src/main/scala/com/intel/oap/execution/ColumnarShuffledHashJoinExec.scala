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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
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
    "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "join time"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def supportsColumnar = true

  //TODO() Disable code generation
  //override def supportCodegen: Boolean = false

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val joinTime = longMetric("joinTime")
    val buildTime = longMetric("buildTime")
    val resultSchema = this.schema
    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) =>
        //val hashed = buildHashedRelation(buildIter)
        //join(streamIter, hashed, numOutputRows)
        val vjoin = ColumnarShuffledHashJoin.create(
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
          numOutputRows,
          sparkConf)
        val vjoinResult = vjoin.columnarInnerJoin(streamIter, buildIter)
        TaskContext
          .get()
          .addTaskCompletionListener[Unit](_ => {
            vjoin.close()
          })
        new CloseableColumnBatchIterator(vjoinResult)
    }
  }
}
