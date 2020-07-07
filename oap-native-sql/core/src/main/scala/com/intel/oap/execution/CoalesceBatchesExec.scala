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

import java.util.concurrent.TimeUnit

import com.intel.oap.vectorized.ArrowWritableColumnVector
import org.apache.arrow.vector.util.VectorBatchAppender
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ListBuffer

case class CoalesceBatchesExec(child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "CoalesceBatches"

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "concatTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "concat batch time total"),
    "collectTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "collect batch time total"),
    "avgCoalescedNumRows" -> SQLMetrics
      .createAverageMetric(sparkContext, "avg coalesced batch num rows"))
  // TODO: peak device memory total

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    import CoalesceBatchesExec._

    val recordsPerBatch = conf.arrowMaxRecordsPerBatch
    val numInputRows = longMetric("numInputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputBatches = longMetric("numOutputBatches")
    val concatTime = longMetric("concatTime")
    val collectTime = longMetric("collectTime")
    val avgCoalescedNumRows = longMetric("avgCoalescedNumRows")

    child.executeColumnar().mapPartitions { iter =>
      if (iter.hasNext) {
        new Iterator[ColumnarBatch] {
          var target: ColumnarBatch = _
          var numBatchesTotal: Long = _
          var numRowsTotal: Long = _

          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            closePrevious()
            if (numBatchesTotal > 0) {
              avgCoalescedNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
            }
          }

          private def closePrevious(): Unit = {
            if (target != null) {
              target.close()
              target = null
            }
          }

          override def hasNext: Boolean = {
            iter.hasNext
          }

          override def next(): ColumnarBatch = {
            closePrevious()
            var rowCount = 0
            val batchesToAppend = ListBuffer[ColumnarBatch]()

            val beforeCollect = System.nanoTime
            target = iter.next()
            target.retain()
            rowCount += target.numRows

            while (iter.hasNext && rowCount < recordsPerBatch) {
              val delta = iter.next()
              delta.retain()
              rowCount += delta.numRows
              batchesToAppend += delta
            }

            val beforeConcat = System.nanoTime
            collectTime += beforeConcat - beforeCollect

            coalesce(target, batchesToAppend.toList)
            target.setNumRows(rowCount)

            concatTime += System.nanoTime - beforeConcat
            numInputRows += rowCount
            numInputBatches += (1 + batchesToAppend.length)
            numOutputBatches += 1

            // used for calculating avgCoalescedNumRows
            numRowsTotal += rowCount
            numBatchesTotal += 1

            batchesToAppend.foreach(cb => cb.close())

            target
          }
        }
      } else {
        Iterator.empty
      }
    }
  }
}

object CoalesceBatchesExec {
  implicit class ArrowColumnarBatchRetainer(val cb: ColumnarBatch) {
    def retain(): Unit = {
      (0 until cb.numCols).toList.foreach(i =>
        cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
    }
  }

  def coalesce(targetBatch: ColumnarBatch, batchesToAppend: List[ColumnarBatch]): Unit = {
    (0 until targetBatch.numCols).toList.foreach { i =>
      val targetVector =
        targetBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector
      val vectorsToAppend = batchesToAppend.map { cb =>
        cb.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector
      }
      VectorBatchAppender.batchAppend(targetVector, vectorsToAppend: _*)
    }
  }
}
