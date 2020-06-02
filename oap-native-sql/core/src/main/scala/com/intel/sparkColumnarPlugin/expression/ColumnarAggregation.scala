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

package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf
import java.util.ArrayList
import java.util.Collections
import java.util.concurrent.TimeUnit._
import util.control.Breaks._

import com.intel.sparkColumnarPlugin.vectorized.ArrowWritableColumnVector
import org.apache.spark.sql.util.ArrowUtils
import com.intel.sparkColumnarPlugin.vectorized.ExpressionEvaluator
import com.intel.sparkColumnarPlugin.vectorized.BatchIterator

import com.google.common.collect.Lists
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.TaskContext

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

class ColumnarAggregation(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    originalInputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    output: Seq[Attribute],
    numInputBatches: SQLMetric,
    numOutputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    aggrTime: SQLMetric,
    elapseTime: SQLMetric)
    extends Logging {
  // build gandiva projection here.
  var elapseTime_make: Long = 0
  var rowId: Int = 0
  var processedNumRows: Int = 0
  var result_iterator: BatchIterator = _

  logInfo(
    s"\ngroupingExpressions: $groupingExpressions,\noriginalInputAttributes: $originalInputAttributes,\naggregateExpressions: $aggregateExpressions,\naggregateAttributes: $aggregateAttributes,\nresultExpressions: $resultExpressions, \noutput: $output")

  var resultTotalRows: Int = 0
  val mode = if (aggregateExpressions.size > 0) {
    aggregateExpressions(0).mode
  } else {
    null
  }

  //////////////// Project original input to aggregateExpression input //////////////////
  // 1. map original input to grouping input
  var groupingExpressionsProjector = ColumnarProjection.create(originalInputAttributes, groupingExpressions)
  val groupingOrdinalList = groupingExpressionsProjector.getOrdinalList
  val groupingAttributes = groupingExpressionsProjector.output
  logInfo(s"groupingAttributes is ${groupingAttributes},\ngroupingOrdinalList is ${groupingOrdinalList}")

  // 2. create grouping native Expression
  val groupingFieldList = groupingAttributes.map(attr => {
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })
  val groupingNativeExpression: List[ColumnarAggregateExpressionBase] = groupingFieldList.map(field => {
    new ColumnarUniqueAggregateExpression(List(field)).asInstanceOf[ColumnarAggregateExpressionBase]
  })

  // 3. map original input to aggregate input
  var beforeAggregateProjector: ColumnarProjection = _
  var projectOrdinalList : List[Int] = _
  var aggregateInputAttributes : List[AttributeReference] = _

  if (mode == null) {
    projectOrdinalList = List[Int]()
    aggregateInputAttributes =  List[AttributeReference]()
  } else {
    mode match {
      case Partial => { 
        beforeAggregateProjector =
          ColumnarProjection.create(
            originalInputAttributes,
            aggregateExpressions.flatMap(_.aggregateFunction.children), skipLiteral = true, renameResult = true)
        projectOrdinalList = beforeAggregateProjector.getOrdinalList
        aggregateInputAttributes = beforeAggregateProjector.output
      }
      case Final => {
        val ordinal_attr_list = originalInputAttributes.toList.zipWithIndex
          .filter{case(expr, i) => !groupingOrdinalList.contains(i)}
          .map{case(expr, i) => {
            (i, ConverterUtils.getAttrFromExpr(expr))
          }}
        projectOrdinalList = ordinal_attr_list.map(_._1)
        aggregateInputAttributes = ordinal_attr_list.map(_._2)
      }
      case _ =>
        throw new UnsupportedOperationException("doesn't support this mode")
    }
  }
  logInfo(s"aggregateProjectorOutputAttributes is ${aggregateInputAttributes},\nprojectOrdinalList is ${projectOrdinalList}")

  // 4. create aggregate native Expression
  val aggregateFieldList = aggregateInputAttributes.map(attr => {
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })
  var field_id = 0
  val aggregateNativeExpressions: List[ColumnarAggregateExpressionBase] =
    aggregateExpressions.toList.map(expr => {
      val res = new ColumnarAggregateExpression(
        expr.aggregateFunction,
        expr.mode,
        expr.isDistinct,
        expr.resultId)
      val arg_size = res.requiredColNum
      val res_size = res.expectedResColNum
      val fieldList = ListBuffer[Field]()
      for (i <- 0 until arg_size) {
        fieldList += aggregateFieldList(field_id)
        field_id += 1
      }
      res.setInputFields(fieldList.toList)
      res
    })

  // 5. create nativeAggregate evaluator
  val allNativeExpressions = groupingNativeExpression ::: aggregateNativeExpressions
  val allAggregateInputFieldList = groupingFieldList ::: aggregateFieldList
  val allAggregateResultAttributes = groupingAttributes ::: aggregateAttributes.toList
  val aggregateResultFieldList = allAggregateResultAttributes.map(attr => {
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })
  /* declare dummy resultType and resultField to generate Gandiva expression
   * Both won't be actually used.*/
  val resultType = CodeGeneration.getResultType()
  val resultField = Field.nullable(s"dummy_res", resultType)

  val expressionTree: List[ExpressionTree] = allNativeExpressions.map( expr => {
    val node =
      expr.doColumnarCodeGen_ext((groupingFieldList, allAggregateInputFieldList, resultType, resultField))
    TreeBuilder.makeExpression(node, resultField)
  })
  val aggregateInputSchema = new Schema(allAggregateInputFieldList.asJava)
  val aggregateResultSchema = new Schema(aggregateResultFieldList.asJava)
  var aggregator = new ExpressionEvaluator()
  aggregator.build(aggregateInputSchema, expressionTree.asJava, true)
  aggregator.setReturnFields(aggregateResultSchema)
  logInfo(s"native aggregate input is $aggregateInputSchema,\noutput is $aggregateResultSchema")

  // 6. map grouping and aggregate result to FinalResult
  var aggregateToResultProjector = ColumnarProjection.create(allAggregateResultAttributes, resultExpressions, skipLiteral = false, renameResult = true)
  val aggregateToResultOrdinalList = aggregateToResultProjector.getOrdinalList
  val resultAttributes = aggregateToResultProjector.output
  val resultArrowSchema = new Schema(resultAttributes.map(attr => {
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  }).asJava)
  logInfo(s"resultAttributes is ${resultAttributes},\naggregateToResultOrdinalList is ${aggregateToResultOrdinalList}")
//////////////////////////////////////////////////////////////////////////////////////////////////
  def close(): Unit = {
    if (aggregator != null) {
      aggregator.close()
      aggregator = null
    }
    if (result_iterator != null) {
      result_iterator.close()
      result_iterator = null
    }
  }

  def updateAggregationResult(columnarBatch: ColumnarBatch): Unit = {
    val numRows = columnarBatch.numRows
    val groupingProjectCols = groupingOrdinalList.map(i => {
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].retain()
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector]
    })
    val aggregateProjectCols = projectOrdinalList.map(i => {
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].retain()
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector]
    })

    val groupingAggregateCols : List[ArrowWritableColumnVector] = if (groupingExpressionsProjector.needEvaluate) {
      val res = groupingExpressionsProjector.evaluate(numRows, groupingProjectCols.map(_.getValueVector()))
      groupingProjectCols.foreach(_.close())
      res
    } else {
      groupingProjectCols
    }

    val aggregateCols : List[ArrowWritableColumnVector]= if (beforeAggregateProjector != null && beforeAggregateProjector.needEvaluate) {
      val res = beforeAggregateProjector.evaluate(numRows, aggregateProjectCols.map(_.getValueVector()))
      aggregateProjectCols.foreach(_.close())
      res
    } else {
      aggregateProjectCols
    }

    val combinedAggregateCols = groupingAggregateCols ::: aggregateCols
    val inputAggrRecordBatch: ArrowRecordBatch =
      ConverterUtils.createArrowRecordBatch(numRows, combinedAggregateCols.map(_.getValueVector()))
    aggregator.evaluate(inputAggrRecordBatch)
    ConverterUtils.releaseArrowRecordBatch(inputAggrRecordBatch)
    groupingAggregateCols.foreach(_.close())
    aggregateCols.foreach(_.close())
  }

  def getAggregationResult(resultIter: BatchIterator): ColumnarBatch = {
    val resultStructType = ArrowUtils.fromArrowSchema(resultArrowSchema)
    if (processedNumRows == 0) {
      val resultColumnVectors =
        ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
      return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
    } else {
      val finalResultRecordBatch = if (resultIter != null) {
        resultIter.next()
      } else {
        aggregator.finish()(0)
      }

      if (finalResultRecordBatch == null) {
        val resultColumnVectors =
          ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
        return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
      }
      val resultLength = finalResultRecordBatch.getLength

      val aggrExprResultColumnVectorList = ConverterUtils.fromArrowRecordBatch(aggregateResultSchema, finalResultRecordBatch)
      ConverterUtils.releaseArrowRecordBatch(finalResultRecordBatch)
      val resultInputCols = aggregateToResultOrdinalList.map(i => {
        aggrExprResultColumnVectorList(i).asInstanceOf[ArrowWritableColumnVector].retain()
        aggrExprResultColumnVectorList(i)
      })
      val resultColumnVectorList = if (aggregateToResultProjector.needEvaluate) {
        val res = aggregateToResultProjector.evaluate(resultLength, resultInputCols.map(_.getValueVector()))
        //for (i <- 0 until resultLength)
        //  logInfo(s"aggregateToResultProjector, input is ${resultInputCols.map(v => v.getUTF8String(i))}, output is ${res.map(v => v.getUTF8String(i))}")
        resultInputCols.foreach(_.close())
        res
      } else {
        resultInputCols
      }
      aggrExprResultColumnVectorList.foreach(_.close())
      //logInfo(s"AggregationResult first row is ${resultColumnVectorList.map(v => v.getUTF8String(0))}")
      new ColumnarBatch(resultColumnVectorList.map(v => v.asInstanceOf[ColumnVector]).toArray, resultLength)
    }
  }

  def createIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var cb: ColumnarBatch = null
      var nextCalled = false
      var resultColumnarBatch: ColumnarBatch = null
      var data_loaded = false
      var nextBatch = true

      override def hasNext: Boolean = {
        if (nextCalled == false && resultColumnarBatch != null) {
          return true
        }
        if ( !nextBatch ) {
          return false
        }

        nextCalled = false
        if (data_loaded == false) {
          val beforeAgg = System.nanoTime()
          while (cbIterator.hasNext) {
            cb = cbIterator.next()
  
            if (cb.numRows > 0) {
              updateAggregationResult(cb)
              processedNumRows += cb.numRows
            }
            numInputBatches += 1
          }
          val elapse = System.nanoTime() - beforeAgg
          elapseTime += NANOSECONDS.toMillis(elapse)
          if (groupingFieldList.size > 0) {
            result_iterator = aggregator.finishByIterator()
          }
          data_loaded = true
        }
        val beforeEval = System.nanoTime()
        resultColumnarBatch = getAggregationResult(result_iterator)
        val eval_elapse = System.nanoTime() - beforeEval
        aggrTime += NANOSECONDS.toMillis(eval_elapse)
        if (resultColumnarBatch.numRows == 0) {
          resultColumnarBatch.close()
          logInfo(s"Aggregation completed, total output ${numOutputRows} rows, ${numOutputBatches} batches")
          return false
        }
        numOutputBatches += 1
        numOutputRows += resultColumnarBatch.numRows
        if (result_iterator == null) {
          nextBatch = false
        }
        true
      }

      override def next(): ColumnarBatch = {
        if (resultColumnarBatch == null) {
          throw new UnsupportedOperationException(s"next() called, while there is no next")
        }
        nextCalled = true
        val numCols = resultColumnarBatch.numCols
        //logInfo(s"result has ${resultColumnarBatch.numRows}, first row is ${(0 until numCols).map(resultColumnarBatch.column(_).getUTF8String(0))}")
        resultColumnarBatch
      }
    }// iterator
  }

}

object ColumnarAggregation {
  var columnarAggregation: ColumnarAggregation = _
  def create(
      partIndex: Int,
      groupingExpressions: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      resultExpressions: Seq[NamedExpression],
      output: Seq[Attribute],
      numInputBatches: SQLMetric,
      numOutputBatches: SQLMetric,
      numOutputRows: SQLMetric,
      aggrTime: SQLMetric,
      elapseTime: SQLMetric): ColumnarAggregation = synchronized {
    columnarAggregation = new ColumnarAggregation(
      partIndex,
      groupingExpressions,
      originalInputAttributes,
      aggregateExpressions,
      aggregateAttributes,
      resultExpressions,
      output,
      numInputBatches,
      numOutputBatches,
      numOutputRows,
      aggrTime,
      elapseTime)
    columnarAggregation
  }

  def close(): Unit = {
    if (columnarAggregation != null) {
      columnarAggregation.close()
      columnarAggregation = null
    }
  }
}
