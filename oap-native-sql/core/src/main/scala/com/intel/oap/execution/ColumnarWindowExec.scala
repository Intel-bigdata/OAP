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

import com.google.flatbuffers.FlatBufferBuilder
import com.intel.oap.expression.{CodeGeneration, ConverterUtils}
import com.intel.oap.vectorized.{ArrowWritableColumnVector, CloseableColumnBatchIterator, ExpressionEvaluator}
import org.apache.arrow.gandiva.expression.TreeBuilder
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, Cast, Descending, Expression, NamedExpression, Rank, SortOrder, WindowExpression, WindowFunction}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Sum}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class ColumnarWindowExec(windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan) extends WindowExec(windowExpression,
  partitionSpec, orderSpec, child) {

  override def supportsColumnar = true

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  // We no longer require for sorted input for columnar window
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "totalTime" -> SQLMetrics
        .createTimingMetric(sparkContext, "totaltime_window"))

  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")
  val numInputBatches = longMetric("numInputBatches")
  val totalTime = longMetric("totalTime")

  val sparkConf = sparkContext.getConf


  val windowFunctions: Seq[(String, Expression)] = windowExpression
      .map(e => e.asInstanceOf[Alias])
      .map(a => a.child.asInstanceOf[WindowExpression])
      .map(w => w.windowFunction)
      .map {
        case a: AggregateExpression => a.aggregateFunction
        case b: WindowFunction => b
        case f =>
          throw new UnsupportedOperationException("unsupported window function type: " +
              f)
      }
      .map { f =>
        val name = f match {
          case _: Sum => "sum"
          case _: Average => "avg"
          case _: Rank =>
            val desc: Option[Boolean] = orderSpec.foldLeft[Option[Boolean]](None) {
              (desc, s) =>
                val currentDesc = s.direction match {
                  case Ascending => false
                  case Descending => true
                  case _ => throw new IllegalStateException
                }
                if (desc.isEmpty) {
                  Some(currentDesc)
                } else if (currentDesc == desc.get) {
                  Some(currentDesc)
                } else {
                  throw new UnsupportedOperationException("Rank: clashed rank order found")
                }
            }
            desc match {
              case Some(true) => "rank_desc"
              case Some(false) => "rank_asc"
              case None => "rank_asc"
            }
          case f => throw new UnsupportedOperationException("unsupported window function: " + f)
        }
        (name, f)
      }

  if (windowFunctions.isEmpty) {
    throw new UnsupportedOperationException("zero window functions" +
        "specified in window")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitionsWithIndex { (partIndex, iter) =>
      if (!iter.hasNext) {
        Iterator.empty
      } else {
        val prev1 = System.nanoTime()
        val gWindowFunctions = windowFunctions.map { case (n, f) =>
          TreeBuilder.makeFunction(n,
            f.children
                .map(e =>
                  e match {
                    case a: AttributeReference =>
                      TreeBuilder.makeField(
                        Field.nullable(a.name,
                          CodeGeneration.getResultType(a.dataType)))
                    case c: Cast =>
                      TreeBuilder.makeField(
                        Field.nullable(c.child.asInstanceOf[AttributeReference].name,
                          CodeGeneration.getResultType(c.dataType))
                      )
                  }).toList.asJava,
            NoneType.NONE_TYPE)
        }
        val groupingExpressions = partitionSpec.map(e => e.asInstanceOf[AttributeReference])

        val gPartitionSpec = TreeBuilder.makeFunction("partitionSpec",
          groupingExpressions.map(e => TreeBuilder.makeField(
            Field.nullable(e.name,
              CodeGeneration.getResultType(e.dataType)))).toList.asJava,
          NoneType.NONE_TYPE)
        // Workaround:
        // Gandiva doesn't support serializing Struct type so far. Use a fake Binary type instead.
        val returnType = ArrowType.Binary.INSTANCE
        val fieldType = new FieldType(false, returnType, null)
        val resultField = new Field("window_res", fieldType,
          windowFunctions.map { case (_, f) =>
            CodeGeneration.getResultType(f.dataType)
          }.zipWithIndex.map { case (t, i) =>
            Field.nullable(s"window_res_" + i, t)
          }.asJava)

        val window = TreeBuilder.makeFunction("window",
          (gWindowFunctions.toList ++ List(gPartitionSpec)).asJava, returnType)

        val evaluator = new ExpressionEvaluator()
        val resultSchema = new Schema(resultField.getChildren)
        val arrowSchema = ArrowUtils.toArrowSchema(child.schema, SQLConf.get.sessionLocalTimeZone)
        evaluator.build(arrowSchema,
          List(TreeBuilder.makeExpression(window,
            resultField)).asJava, resultSchema, true)
        val inputCache = new ListBuffer[ColumnarBatch]()
        val buildCost = System.nanoTime() - prev1
        totalTime += TimeUnit.NANOSECONDS.toMillis(buildCost)
        iter.foreach(c => {
          numInputBatches += 1
          val prev2 = System.nanoTime()
          inputCache += c
          (0 until c.numCols()).map(c.column)
              .foreach(_.asInstanceOf[ArrowWritableColumnVector].retain())
          val recordBatch = ConverterUtils.createArrowRecordBatch(c)
          try {
            evaluator.evaluate(recordBatch)
          } finally {
            recordBatch.close()
          }
          val evaluationCost = System.nanoTime() - prev2
          totalTime += TimeUnit.NANOSECONDS.toMillis(evaluationCost)
        })

        val prev3 = System.nanoTime()
        val batches = evaluator.finish()
        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => evaluator.close())
        val windowFinishCost = System.nanoTime() - prev3
        totalTime += TimeUnit.NANOSECONDS.toMillis(windowFinishCost)
        val itr = batches.zipWithIndex.map { case (recordBatch, i) => {
          val prev4 = System.nanoTime()
          val length = recordBatch.getLength
          val vectors = try {
             ArrowWritableColumnVector.loadColumns(length, resultSchema, recordBatch)
          } finally {
            recordBatch.close()
          }
          val correspondingInputBatch = inputCache(i)
          val batch = new ColumnarBatch(
            (0 until correspondingInputBatch.numCols()).map(i => correspondingInputBatch.column(i)).toArray
                ++ vectors, correspondingInputBatch.numRows())
          val emitCost = System.nanoTime() - prev4
          totalTime += TimeUnit.NANOSECONDS.toMillis(emitCost)
          numOutputRows += batch.numRows()
          numOutputBatches += 1
          batch
        }}.toIterator
        new CloseableColumnBatchIterator(itr)
      }
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarWindowExec]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarWindowExec =>
      (that canEqual this) && (that eq this)
    case _ => false
  }

  override def hashCode(): Int = System.identityHashCode(this)

  private object NoneType {
    val NONE_TYPE = new NoneType
  }

  private class NoneType extends ArrowType {
    override def getTypeID: ArrowType.ArrowTypeID = {
      return ArrowTypeID.NONE
    }

    override def getType(builder: FlatBufferBuilder): Int = {
      throw new UnsupportedOperationException()
    }

    override def toString: String = {
      return "NONE"
    }

    override def accept[T](visitor: ArrowType.ArrowTypeVisitor[T]): T = {
      throw new UnsupportedOperationException()
    }

    override def isComplex: Boolean = false
  }
}
