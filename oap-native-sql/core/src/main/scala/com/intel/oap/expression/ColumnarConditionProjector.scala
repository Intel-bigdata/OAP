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

import java.util.Collections
import java.util.concurrent.TimeUnit

import com.google.common.collect.Lists
import com.intel.oap.vectorized.ArrowWritableColumnVector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType

import io.netty.buffer.ArrowBuf
import scala.collection.JavaConverters._

class ColumnarConditionProjector(
  condExpr: Expression,
  projectList: Seq[Expression],
  originalInputAttributes: Seq[Attribute],
  numInputBatches: SQLMetric,
  numOutputBatches: SQLMetric,
  numOutputRows: SQLMetric,
  procTime: SQLMetric)
  extends Logging {
  logInfo(s"originalInputAttributes is ${originalInputAttributes}, \nCondition is ${condExpr}, \nProjection is ${projectList}")
  var proc_time :Long = 0
  var elapseTime_make: Long = 0
  val start_make: Long = System.nanoTime()
  var skip = false
  var selectionBuffer : ArrowBuf = null

  val conditionInputList : java.util.List[Field] = Lists.newArrayList()
  val condPrepareList: (TreeNode, ArrowType) = if (condExpr != null) {
    val columnarCondExpr: Expression = ColumnarExpressionConverter.replaceWithColumnarExpression(condExpr, originalInputAttributes)
    val (cond, resultType) =
      columnarCondExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(conditionInputList)
    (cond, resultType)
  } else {
    null
  }
  //Collections.sort(conditionFieldList, (l: Field, r: Field) => { l.getName.compareTo(r.getName)})
  val conditionFieldList = conditionInputList.asScala.toList.distinct.asJava;
  val conditionOrdinalList: List[Int] = conditionFieldList.asScala.toList.map(field => {
    field.getName.replace("c_", "").toInt
  })

  var projectInputList : java.util.List[Field] = Lists.newArrayList()
  var projPrepareList : Seq[(ExpressionTree, ArrowType)] = null
  if (projectList != null) {
    val columnarProjExprs: Seq[Expression] = projectList.map(expr => {
      ColumnarExpressionConverter.replaceWithColumnarExpression(expr, originalInputAttributes)
    })
    projPrepareList = columnarProjExprs.map(columnarExpr => {
      val (node, resultType) =
        columnarExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(projectInputList)
      val result = Field.nullable("result", resultType)
      (TreeBuilder.makeExpression(node, result), resultType)
    })
  }
  var projectFieldList = projectInputList.asScala.toList.distinct.asJava;

  if (projectFieldList.size == 0) { 
    if (conditionFieldList.size > 0) {
      projectFieldList = originalInputAttributes.zipWithIndex.toList.map{case (attr, i) => 
        Field.nullable(s"c_${i}", CodeGeneration.getResultType(attr.dataType))
      }.asJava
    } else {
      logInfo(s"skip this conditionprojection")
      skip = true
    }
  }
  val projectOrdinalList: List[Int] = projectFieldList.asScala.toList.map(field => {
    field.getName.replace("c_", "").toInt
  })

  val projectResultFieldList = if (projPrepareList != null) {
    projPrepareList.map(expr => Field.nullable(s"result", expr._2)).toList.asJava
  } else {
    projPrepareList = 
      projectFieldList.asScala.map(field => {
        (TreeBuilder.makeExpression(TreeBuilder.makeField(field), field), field.getType)
      })
    projectFieldList
  }

  val conditionArrowSchema = new Schema(conditionFieldList)
  val projectionArrowSchema = new Schema(projectFieldList)
  val projectionSchema = ArrowUtils.fromArrowSchema(projectionArrowSchema)
  val resultArrowSchema = new Schema(projectResultFieldList)
  val resultSchema = ArrowUtils.fromArrowSchema(resultArrowSchema)
  logInfo(s"conditionArrowSchema is ${conditionArrowSchema}, conditionOrdinalList is ${conditionOrdinalList}, \nprojectionArrowSchema is ${projectionArrowSchema}, projectionOrinalList is ${projectOrdinalList}, \nresult schema is ${resultArrowSchema}")

  val conditioner = if (skip == false && condPrepareList != null) {
    createFilter(conditionArrowSchema, condPrepareList)
  } else {
    null
  }
  val withCond: Boolean = if (conditioner != null) {
    true
  } else {
    false
  }
  val projector = if (skip == false) {
    createProjector(projectionArrowSchema, projPrepareList, withCond)
  } else {
    null
  }

  elapseTime_make = System.nanoTime() - start_make
  logInfo(s"Gandiva make total ${TimeUnit.NANOSECONDS.toMillis(elapseTime_make)} ms.")

  val allocator = ArrowWritableColumnVector.getNewAllocator

  def createFilter(arrowSchema: Schema, prepareList: (TreeNode, ArrowType)): Filter =
    synchronized {
      if (conditioner != null) {
        return conditioner
      }
      Filter.make(arrowSchema, TreeBuilder.makeCondition(prepareList._1))
    }

  def createProjector(
      arrowSchema: Schema,
      prepareList: Seq[(ExpressionTree, ArrowType)],
      withCond: Boolean): Projector = synchronized {
    if (projector != null) {
      return projector
    }
    val fieldNodesList = prepareList.map(_._1).toList.asJava
    if (withCond) {
      Projector.make(arrowSchema, fieldNodesList, SelectionVectorType.SV_INT16)
    } else {
      Projector.make(arrowSchema, fieldNodesList)
    }
  }

  def createStructType(arrowSchema: Schema): StructType = {
    ArrowUtils.fromArrowSchema(arrowSchema)
  }

  def close(): Unit = {
    if (selectionBuffer != null) {
      selectionBuffer.close()
      selectionBuffer = null
    }
    allocator.close()
    if (conditioner != null) {
      conditioner.close()
    }
    if (projector != null) {
      projector.close()
    }
    procTime.set(proc_time)
  }

  def createIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      private var columnarBatch: ColumnarBatch = null
      private var resColumnarBatch: ColumnarBatch = null
      private var nextCalled = false

      override def hasNext: Boolean = {
        if (!nextCalled && resColumnarBatch != null) {
          return true
        }
        nextCalled = false
        var beforeEval: Long = 0
        var afterEval: Long = 0
        var numRows = 0
        var input : ArrowRecordBatch = null
        var selectionVector : SelectionVectorInt16 = null
        while (numRows == 0) {

          if (cbIterator.hasNext) {
            columnarBatch = cbIterator.next()
            numInputBatches += 1
          } else {
            resColumnarBatch = null
            logInfo(s"has no next, return false")
            return false
          }
          beforeEval = System.nanoTime()
          numRows = columnarBatch.numRows()
          if (numRows > 0) {
            if (skip == true){
              logInfo("Use original ColumnarBatch")
              resColumnarBatch = columnarBatch
              (0 until resColumnarBatch.numCols).toList.foreach(i => resColumnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
              return true
            } 
            if (conditioner != null) {
              // do conditioner here
              numRows = columnarBatch.numRows
              if (selectionBuffer != null) {
                selectionBuffer.close()
                selectionBuffer = null
              }
              selectionBuffer = allocator.buffer(numRows * 2)
              selectionVector = new SelectionVectorInt16(selectionBuffer)
              val cols = conditionOrdinalList.map(i => {
                columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
              })
              afterEval = System.nanoTime()
              proc_time += ((System.nanoTime() - beforeEval) / (1000 * 1000))
              input = ConverterUtils.createArrowRecordBatch(numRows, cols)
              conditioner.evaluate(input, selectionVector)
              ConverterUtils.releaseArrowRecordBatch(input)
              numRows = selectionVector.getRecordCount()
              if (projectList == null && numRows == columnarBatch.numRows()) {
                logInfo("No projection and conditioned row number is as same as original row number. Directly use original ColumnarBatch")
                resColumnarBatch = columnarBatch
                (0 until resColumnarBatch.numCols).toList.foreach(i => resColumnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
                return true
              }
            }
          }
          if (numRows == 0) {
            logInfo(s"Got empty ColumnarBatch from child or after filter")
          }
        }
  
        // for now, we either filter one columnarBatch who has valid rows or we only need to do project
        // either scenario we will need to output one columnarBatch.
        beforeEval = System.nanoTime()
        val resultColumnVectors = ArrowWritableColumnVector.allocateColumns(numRows, resultSchema).toArray
        val outputVectors = resultColumnVectors.map(columnVector => {
          columnVector.getValueVector()
        }).toList.asJava
  
        val cols = projectOrdinalList.map(i => {
          columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
        })
        input = ConverterUtils.createArrowRecordBatch(columnarBatch.numRows, cols)
        if(conditioner != null) {
          projector.evaluate(input, selectionVector, outputVectors);
        } else {
          projector.evaluate(input, outputVectors);
        }
  
        ConverterUtils.releaseArrowRecordBatch(input)
        val outputBatch = new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), numRows)
        proc_time += ((System.nanoTime() - beforeEval) / (1000 * 1000))
        resColumnarBatch = outputBatch
        true
      }

      override def next(): ColumnarBatch = {
        nextCalled = true
        if (resColumnarBatch == null) {
          throw new UnsupportedOperationException("Iterator has no next columnar batch or it hasn't been called by hasNext.")
        }
        numOutputBatches += 1
        numOutputRows += resColumnarBatch.numRows
        val numCols = resColumnarBatch.numCols
        //logInfo(s"result has ${resColumnarBatch.numRows}, first row is ${(0 until numCols).map(resColumnarBatch.column(_).getUTF8String(0))}")
        resColumnarBatch
      }

    }// end of Iterator
  }// end of createIterator

}// end of class

object ColumnarConditionProjector {
  def create(
    condition: Expression,
    projectList: Seq[Expression],
    inputSchema: Seq[Attribute],
    numInputBatches: SQLMetric,
    numOutputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    procTime: SQLMetric): ColumnarConditionProjector = synchronized {
    new ColumnarConditionProjector(
      condition,
      projectList,
      inputSchema,
      numInputBatches,
      numOutputBatches,
      numOutputRows,
      procTime)
  }
}
