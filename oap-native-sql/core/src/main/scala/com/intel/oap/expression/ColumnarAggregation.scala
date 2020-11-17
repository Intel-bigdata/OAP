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

package com.intel.oap.expression

import io.netty.buffer.ArrowBuf
import java.util.ArrayList
import java.util.Collections
import java.util.concurrent.TimeUnit._
import util.control.Breaks._

import com.intel.oap.ColumnarPluginConfig
import com.intel.oap.vectorized.ArrowWritableColumnVector
import org.apache.spark.sql.util.ArrowUtils
import com.intel.oap.vectorized.ExpressionEvaluator
import com.intel.oap.vectorized.BatchIterator

import com.google.common.collect.Lists
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.spark.SparkConf
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
    elapseTime: SQLMetric,
    sparkConf: SparkConf)
    extends Logging {
  // build gandiva projection here.
  ColumnarPluginConfig.getConf(sparkConf)
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
  val aggregateAttributeList : Seq[Attribute] = aggregateAttributes
  val beforeAggregateExprListBuffer = ListBuffer[Expression]()
  val projectOrdinalListBuffer = ListBuffer[Int]()
  val aggregateFieldListBuffer = ListBuffer[Field]()
  var aggregateResultAttributesBuffer = getAttrForAggregateExpr(aggregateExpressions)
  // we need to remove ordinal used in partial mode expression
  val nonPartialProjectOrdinalList = (0 until originalInputAttributes.size).toList.filter(i => !groupingOrdinalList.contains(i)).to[ListBuffer]
  aggregateExpressions.zipWithIndex.foreach{case(expr, index) => expr.mode match {
    case Partial => {
      val internalExpressionList = expr.aggregateFunction.children
      val ordinalList = ColumnarProjection.binding(originalInputAttributes, internalExpressionList, index, skipLiteral = true)
      ordinalList.foreach{i => {
        nonPartialProjectOrdinalList -= i
      }}
    }
    case _ => {}
  }}
  var non_partial_field_id = 0
  var res_field_id = 0
  val (aggregateNativeExpressions, beforeAggregateProjector) = if (mode == null) {
    (List[ColumnarAggregateExpressionBase](), null)
  } else {
    // we need to filter all Partial mode aggregation
    val columnarExprList = aggregateExpressions.toList.zipWithIndex.map{case(expr, index) => expr.mode match {
      case Partial => {
        val res = new ColumnarAggregateExpression(
          expr.aggregateFunction,
          expr.mode,
          expr.isDistinct,
          expr.resultId)
        val arg_size = res.requiredColNum
        val internalExpressionList = expr.aggregateFunction.children
        val ordinalList = ColumnarProjection.binding(originalInputAttributes, internalExpressionList, index, skipLiteral = true)
        val fieldList = if (arg_size > 0) {
          internalExpressionList.map(projectExpr => {
            val attr = ConverterUtils.getResultAttrFromExpr(projectExpr, s"res_$index")
            if (attr.dataType.isInstanceOf[DecimalType])
              throw new UnsupportedOperationException(s"Decimal type is not supported in ColumnarAggregation.")
            Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
          })
        } else {
          List[Field]()
        }
        ordinalList.foreach{i => {
          nonPartialProjectOrdinalList -= i
          projectOrdinalListBuffer += i
        }}
        fieldList.foreach{field => {
          aggregateFieldListBuffer += field
        }}
        expr.aggregateFunction.children.foreach(e => beforeAggregateExprListBuffer += e)
        res.setInputFields(fieldList.toList)
        res
      }
      case _ => {
        val res = new ColumnarAggregateExpression(
          expr.aggregateFunction,
          expr.mode,
          expr.isDistinct,
          expr.resultId)
        val arg_size = res.requiredColNum
        val ordinalList = (non_partial_field_id until (non_partial_field_id + arg_size)).map(i => nonPartialProjectOrdinalList(i))
        non_partial_field_id += arg_size
        val fieldList = ordinalList.map(i => {
          val attr = originalInputAttributes(i)
          if (attr.dataType.isInstanceOf[DecimalType])
            throw new UnsupportedOperationException(s"Decimal type is not supported in ColumnarAggregation.")
          Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
        })
        ordinalList.foreach{i => {
          beforeAggregateExprListBuffer += originalInputAttributes(i)
          projectOrdinalListBuffer += i
        }}
        fieldList.foreach{field => {
          aggregateFieldListBuffer += field
        }}
        res.setInputFields(fieldList.toList)
        res
      }
    }}
    val projector = ColumnarProjection.create(
        originalInputAttributes, beforeAggregateExprListBuffer.toList, skipLiteral = true, renameResult = true)
    (columnarExprList, projector)
  }

  // 4. create aggregate native Expression

  val aggregateOrdinalList = projectOrdinalListBuffer.toList
  var projectOrdinalList = aggregateOrdinalList.distinct
  val aggregateFieldList = aggregateFieldListBuffer.toList

  // 5. create nativeAggregate evaluator
  val allNativeExpressions = groupingNativeExpression ::: aggregateNativeExpressions
  val allAggregateInputFieldList = groupingFieldList ::: aggregateFieldList
  val allAggregateResultAttributes = groupingAttributes ::: aggregateResultAttributesBuffer.toList
  val aggregateResultFieldList = allAggregateResultAttributes.map(attr => {
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })
  /* declare dummy resultType and resultField to generate Gandiva expression
   * Both won't be actually used.*/
  val resultType = CodeGeneration.getResultType()
  val resultField = Field.nullable(s"dummy_res", resultType)

  val expressionTree: List[ExpressionTree] = allNativeExpressions.map(expr => {
    val node =
      expr.doColumnarCodeGen_ext((groupingFieldList, allAggregateInputFieldList, resultType, resultField))
    TreeBuilder.makeExpression(node, resultField)
  })
  val aggregateInputSchema = new Schema(allAggregateInputFieldList.asJava)
  val aggregateResultSchema = new Schema(aggregateResultFieldList.asJava)

  var aggregator: ExpressionEvaluator = _
  if (allAggregateInputFieldList.size > 0) {
    aggregator = new ExpressionEvaluator()
    aggregator.build(aggregateInputSchema, expressionTree.asJava, true)
    aggregator.setReturnFields(aggregateResultSchema)
    logInfo(s"native aggregate input is $aggregateInputSchema,\noutput is $aggregateResultSchema")
  } else {
    null
  }
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

    elapseTime.merge(aggrTime)
  }

  def getAttrForAggregateExpr(aggregateExpressions: Seq[AggregateExpression]): ListBuffer[Attribute] = {
    var aggregateAttr = new ListBuffer[Attribute]()
    val size = aggregateExpressions.size
    var res_index = 0
    for (expIdx <- 0 until size) {
      val exp: AggregateExpression = aggregateExpressions(expIdx)
      val mode = exp.mode
      val aggregateFunc = exp.aggregateFunction
      aggregateFunc match {
        case Average(_) => mode match {
          case Partial => {
            val avg = aggregateFunc.asInstanceOf[Average]
            val aggBufferAttr = avg.inputAggBufferAttributes
            for (index <- 0 until aggBufferAttr.size) {
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
              aggregateAttr += attr
            }
            res_index += 2
          }
          case PartialMerge => {
            val avg = aggregateFunc.asInstanceOf[Average]
            val aggBufferAttr = avg.inputAggBufferAttributes
            for (index <- 0 until aggBufferAttr.size) {
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
              aggregateAttr += attr
            }
            res_index += 1
          }
          case Final => {
            aggregateAttr += aggregateAttributeList(res_index)
            res_index += 1
          }
        }
        case Sum(_) => mode match {
          case Partial | PartialMerge => {
            val sum = aggregateFunc.asInstanceOf[Sum]
            val aggBufferAttr = sum.inputAggBufferAttributes
            val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
            aggregateAttr += attr
            res_index += 1
          }
          case _ => {
            aggregateAttr += aggregateAttributeList(res_index)
            res_index += 1
          }
        }
        case Count(_) => mode match {
          case Partial | PartialMerge => {
            val count = aggregateFunc.asInstanceOf[Count]
            val aggBufferAttr = count.inputAggBufferAttributes
            val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
            aggregateAttr += attr
            res_index += 1
          }
          case _ => {
            aggregateAttr += aggregateAttributeList(res_index)
            res_index += 1
          }
        }
        case Max(_) => mode match {
          case Partial | PartialMerge => {
            val max = aggregateFunc.asInstanceOf[Max]
            val aggBufferAttr = max.inputAggBufferAttributes
            val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
            aggregateAttr += attr
            res_index += 1
          }
          case _ => {
            aggregateAttr += aggregateAttributeList(res_index)
            res_index += 1
          }
        }
        case Min(_) => mode match {
          case Partial | PartialMerge => {
            val min = aggregateFunc.asInstanceOf[Min]
            val aggBufferAttr = min.inputAggBufferAttributes
            val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
            aggregateAttr += attr
            res_index += 1
          }
          case _ => {
            aggregateAttr += aggregateAttributeList(res_index)
            res_index += 1
          }
        }
        case StddevSamp(_) => mode match {
          case Partial => {
            val stddevSamp = aggregateFunc.asInstanceOf[StddevSamp]
            val aggBufferAttr = stddevSamp.inputAggBufferAttributes
            for (index <- 0 until aggBufferAttr.size) {
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
              aggregateAttr += attr
            }
            res_index += 3
          }
          case PartialMerge => {
            throw new UnsupportedOperationException("stddev_samp PartialMerge is not supported.")
          }
          case Final => {
            aggregateAttr += aggregateAttributeList(res_index)
            res_index += 1
          }
        }
        case other =>
          throw new UnsupportedOperationException(s"not currently supported: $other.")
      }
    }
    aggregateAttr
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
    val aggregateFullCols = aggregateOrdinalList.map(i => {
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
      aggregateFullCols.foreach(_.close())
      res
    } else {
      aggregateProjectCols.foreach(_.close())
      aggregateFullCols
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
    } else if (aggregator == null){
      //TODO: add an special case when this hash aggr is doing countLiteral only
      val doCountLiteral: Boolean = expressionTree.map(expr => s"${expr.toProtobuf}").filter(_.contains("countLiteral")).size == 1
      if (doCountLiteral) {
        val resultColumnVectors =
          ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
        (resultColumnVectors zip resultAttributes).foreach{case(v, attr) => {
          val numRows = attr.dataType match {
            case t: IntegerType =>
              processedNumRows.asInstanceOf[Number].intValue
            case t: LongType =>
              processedNumRows.asInstanceOf[Number].longValue
          }
          v.put(0, numRows)
        }}
        return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 1)
      } else {
        val resultColumnVectors =
          ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
        return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
      }
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
      var eval_elapse: Long = 0
      var noNext: Boolean = false

      override def hasNext: Boolean = {
        if (noNext) return false
        if (nextCalled == false && resultColumnarBatch != null) {
          return true
        }
        if (!nextBatch) {
          noNext = true
          return false
        }

        nextCalled = false
        if (data_loaded == false) {
          while (cbIterator.hasNext) {
            cb = cbIterator.next()
  
            if (cb.numRows > 0) {
              val beforeEval = System.nanoTime()
              if (aggregator != null) {
                updateAggregationResult(cb)
              }
              eval_elapse += System.nanoTime() - beforeEval
              processedNumRows += cb.numRows
            }
            numInputBatches += 1
          }
          if (groupingFieldList.size > 0) {
            val beforeFinish = System.nanoTime()
            result_iterator = aggregator.finishByIterator()
            eval_elapse += System.nanoTime() - beforeFinish
          }
          data_loaded = true
          aggrTime += NANOSECONDS.toMillis(eval_elapse)
        }
        val beforeResultFetch = System.nanoTime()
        resultColumnarBatch = getAggregationResult(result_iterator)
        aggrTime += NANOSECONDS.toMillis(System.nanoTime() - beforeResultFetch)
        if (resultColumnarBatch.numRows == 0) {
          resultColumnarBatch.close()
          logInfo(s"Aggregation completed, total output ${numOutputRows} rows, ${numOutputBatches} batches")
          noNext = true
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
      elapseTime: SQLMetric,
      sparkConf: SparkConf): ColumnarAggregation = synchronized {
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
      elapseTime,
      sparkConf)
    columnarAggregation
  }

  def close(): Unit = {
    if (columnarAggregation != null) {
      columnarAggregation.close()
      columnarAggregation = null
    }
  }
}
