package com.intel.sparkColumnarPlugin.execution

import java.util.concurrent.TimeUnit._

import com.intel.sparkColumnarPlugin.vectorized._

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

import com.intel.sparkColumnarPlugin.expression._
import com.intel.sparkColumnarPlugin.vectorized.ExpressionEvaluator
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
    right: SparkPlan) extends ShuffledHashJoinExec(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right) {

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
    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) { (streamIter, buildIter) =>
      //val hashed = buildHashedRelation(buildIter)
      //join(streamIter, hashed, numOutputRows)
      val vjoin = ColumnarShuffledHashJoin.create(leftKeys, rightKeys, resultSchema, joinType, buildSide, condition, left, right, buildTime, joinTime, numOutputRows)
      val vjoinResult = vjoin.columnarInnerJoin(streamIter, buildIter)
      TaskContext.get().addTaskCompletionListener[Unit](_ => {
        vjoin.close()
      })
      new CloseableColumnBatchIterator(vjoinResult)
    }
  }
}
