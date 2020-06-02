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
import java.util._
import java.util.concurrent.TimeUnit

import com.google.common.collect.Lists
import com.intel.sparkColumnarPlugin.vectorized.ArrowWritableColumnVector

import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.ValueVector

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer

class ColumnarFilter (
  originalInputAttributes: Seq[Attribute],
  expr: Expression) extends AutoCloseable with Logging {
  // build gandiva projection here.
  //////////////// Project original input to aggregate input //////////////////
  var conditionInputList : java.util.List[Field] = Lists.newArrayList()
  var resultNumRows : Int = 0
  val columnarExpression: Expression =
    ColumnarExpressionConverter.replaceWithColumnarExpression(expr, originalInputAttributes)
  val (node, resultType) =
    columnarExpression.asInstanceOf[ColumnarExpression].doColumnarCodeGen(conditionInputList)

  val conditionFieldList = conditionInputList.asScala.toList.distinct.asJava;
  val conditionOrdinalList: List[Int] = conditionFieldList.asScala.toList.map(field => {
    field.getName.replace("c_", "").toInt
  })
  val conditionArrowSchema = new Schema(conditionFieldList)
  val projectFieldList = originalInputAttributes.toList.map(attr => {
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })
  val projectArrowSchema = new Schema(projectFieldList.asJava)
  val projectSchema = ArrowUtils.fromArrowSchema(projectArrowSchema)
  logInfo(s"conditionSchema is ${conditionArrowSchema}, projectSchema is ${projectArrowSchema}")

  val allocator = ArrowWritableColumnVector.getNewAllocator
  var selectionBuffer : ArrowBuf = null

  val filter = Filter.make(conditionArrowSchema, TreeBuilder.makeCondition(node))
  val projectionNodeList = projectFieldList.map(field => {
    TreeBuilder.makeExpression(TreeBuilder.makeField(field), field)
  })
  val projector = Projector.make(projectArrowSchema, projectionNodeList.asJava, SelectionVectorType.SV_INT16)

  def getOrdinalList(): List[Int] = {
    conditionOrdinalList 
  }

  def getResultNumRows = resultNumRows

  private def evaluate(inputRecordBatch: ArrowRecordBatch): SelectionVectorInt16 = {
    if (selectionBuffer != null) {
      selectionBuffer.close()
      selectionBuffer = null
    }
    selectionBuffer = allocator.buffer(inputRecordBatch.getLength * 2)
    val selectionVector = new SelectionVectorInt16(selectionBuffer)
    filter.evaluate(inputRecordBatch, selectionVector)
    selectionVector
  }

  def evaluate(numRows: Int, inputColumnVector: List[ValueVector]): SelectionVectorInt16 = {
    val inputRecordBatch: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(numRows, inputColumnVector)
    val selectionVector = evaluate(inputRecordBatch)
    ConverterUtils.releaseArrowRecordBatch(inputRecordBatch)
    selectionVector
  }

  def process(numRows: Int, conditionInput: List[ValueVector], projectInput: List[ValueVector]): List[ArrowWritableColumnVector] = {
    val selectionVector = evaluate(numRows, conditionInput)
    resultNumRows = selectionVector.getRecordCount

    val inputRecordBatch: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(numRows, projectInput)
    val outputVectors = ArrowWritableColumnVector.allocateColumns(resultNumRows, projectSchema)
    val valueVectors = outputVectors.map(columnVector => columnVector.getValueVector()).toList
    projector.evaluate(inputRecordBatch, selectionVector, valueVectors.asJava)
    ConverterUtils.releaseArrowRecordBatch(inputRecordBatch)
    outputVectors.toList
  }

  override def close(): Unit = {
    if (selectionBuffer != null) {
      selectionBuffer.close()
      selectionBuffer = null
    }
    filter.close()
  }
}

object ColumnarFilter extends Logging {
  def create(
    originalInputAttributes: Seq[Attribute],
    expr: Expression)
    : ColumnarFilter = {
    new ColumnarFilter(originalInputAttributes, expr)
  }
}
