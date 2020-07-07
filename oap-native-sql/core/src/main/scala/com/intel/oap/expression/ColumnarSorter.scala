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

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit._
import com.google.common.collect.Lists

import com.intel.oap.ColumnarPluginConfig
import com.intel.oap.vectorized.ArrowWritableColumnVector
import com.intel.oap.vectorized.ExpressionEvaluator
import com.intel.oap.vectorized.BatchIterator

import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.TaskContext

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer

class ColumnarSorter(
    sortOrder: Seq[SortOrder],
    outputAsColumnar: Boolean,
    outputAttributes: Seq[Attribute],
    sortTime: SQLMetric,
    outputBatches: SQLMetric,
    outputRows: SQLMetric,
    shuffleTime: SQLMetric,
    elapse: SQLMetric,
    sparkConf: SparkConf)
    extends Logging {

  logInfo(s"ColumnarSorter sortOrder is ${sortOrder}, outputAttributes is ${outputAttributes}")
  ColumnarPluginConfig.getConf(sparkConf)
  /////////////// Prepare ColumnarSorter //////////////
  var processedNumRows: Long = 0
  var sort_elapse: Long = 0
  var shuffle_elapse: Long = 0
  var total_elapse: Long = 0
  val inputBatchHolder = new ListBuffer[ColumnarBatch]()
  var allocator = ArrowWritableColumnVector.getNewAllocator
  var nextVector: FieldVector = null
  val keyFieldList: List[Field] = sortOrder.toList.map(sort => {
    val attr = ConverterUtils.getAttrFromExpr(sort.child)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  });
  val outputFieldList: List[Field] = outputAttributes.toList.map(expr => {
    val attr = ConverterUtils.getAttrFromExpr(expr)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })
  val inputFieldList: List[Field] = outputFieldList

  val sortFuncName = if (sortOrder.head.isAscending) {
    "sortArraysToIndicesNullsFirstAsc"
  } else {
    "sortArraysToIndicesNullsFirstDesc"
  }

///////////////// Prepare Schema ////////////////
  logInfo(s"inputFieldList is ${inputFieldList}, outputFieldList is ${outputFieldList}")
  val arrowSchema = new Schema(inputFieldList.asJava)
  val resultSchema = StructType(
    outputAttributes
      .map(expr => {
        val attr = ConverterUtils.getAttrFromExpr(expr)
        StructField(s"${attr.name}", attr.dataType, true)
      })
      .toArray)
  val output_arrow_schema = new Schema(outputFieldList.asJava)
  val indicesSchema = new Schema(
    Lists.newArrayList(Field.nullable("indices", new ArrowType.FixedSizeBinary(16))))
///////////////// prepare sort expression ////////////////
  val retType = Field.nullable("res", new ArrowType.Int(32, true))
  val sort_node = TreeBuilder.makeFunction(
    sortFuncName,
    keyFieldList.map(keyField => TreeBuilder.makeField(keyField)).asJava,
    new ArrowType.Int(32, true))
  val sort_expr = TreeBuilder.makeExpression(sort_node, retType)
  var sort_iterator: BatchIterator = _
  var sorter = new ExpressionEvaluator()
  sorter.build(
    arrowSchema,
    Lists.newArrayList(sort_expr),
    indicesSchema,
    true /*return at finish*/ )
  /////////////////////////////////////////////////////

  def close(): Unit = {
    logInfo(s"Sort Closed, ${processedNumRows} rows, output ${outputRows} rows")
    if (nextVector != null) {
      nextVector.close()
    }
    elapse.set(NANOSECONDS.toMillis(total_elapse))
    sortTime.set(NANOSECONDS.toMillis(sort_elapse))
    shuffleTime.set(NANOSECONDS.toMillis(shuffle_elapse))
    inputBatchHolder.foreach(cb => cb.close())
    if (sorter != null) {
      sorter.close()
      sorter = null
    }
    if (sort_iterator != null) {
      sort_iterator.close()
      sort_iterator = null
    }
    if (allocator != null) {
      allocator.close()
      allocator = null
    }
  }

  def updateSorterResult(input: ColumnarBatch): Unit = {
    inputBatchHolder += input
    val input_batch = ConverterUtils.createArrowRecordBatch(input)
    (0 until input.numCols).toList.foreach(i =>
      input.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
    val beforeSort = System.nanoTime()
    sorter.evaluate(input_batch)
    sort_elapse += System.nanoTime() - beforeSort
    total_elapse += System.nanoTime() - beforeSort
    ConverterUtils.releaseArrowRecordBatch(input_batch)
  }

  def getSorterResult(resultBatch: ArrowRecordBatch): ColumnarBatch = {
    if (resultBatch == null) {
      val resultColumnVectors =
        ArrowWritableColumnVector.allocateColumns(0, resultSchema).toArray
      new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
    } else {
      val resultColumnVectorList =
        ConverterUtils.fromArrowRecordBatch(output_arrow_schema, resultBatch)
      val length = resultBatch.getLength()
      ConverterUtils.releaseArrowRecordBatch(resultBatch)
      new ColumnarBatch(resultColumnVectorList.map(v => v.asInstanceOf[ColumnVector]), length)
    }
  }

  def createColumnarIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var cb: ColumnarBatch = null
      var nextBatch: ArrowRecordBatch = null
      var batchIterator: BatchIterator = null

      override def hasNext: Boolean = {
        if (sort_iterator == null) {
          while (cbIterator.hasNext) {
            cb = cbIterator.next()

            if (cb.numRows > 0) {
              updateSorterResult(cb)
              processedNumRows += cb.numRows
            }
          }

          val beforeSort = System.nanoTime()
          sort_iterator = sorter.finishByIterator();
          sort_elapse += System.nanoTime() - beforeSort
          total_elapse += System.nanoTime() - beforeSort
        }

        val beforeShuffle = System.nanoTime()
        nextBatch = sort_iterator.next()
        shuffle_elapse += System.nanoTime() - beforeShuffle
        total_elapse += System.nanoTime() - beforeShuffle

        if (nextBatch == null) {
          return false
        } else {
          return true
        }
      }

      override def next(): ColumnarBatch = {
        outputBatches += 1
        outputRows += nextBatch.getLength()
        getSorterResult(nextBatch)
      }
    }
  }
}

object ColumnarSorter {
  def create(
      sortOrder: Seq[SortOrder],
      outputAsColumnar: Boolean,
      outputAttributes: Seq[Attribute],
      sortTime: SQLMetric,
      outputBatches: SQLMetric,
      outputRows: SQLMetric,
      shuffleTime: SQLMetric,
      elapse: SQLMetric,
      sparkConf: SparkConf): ColumnarSorter = synchronized {
    new ColumnarSorter(
      sortOrder,
      outputAsColumnar,
      outputAttributes,
      sortTime,
      outputBatches,
      outputRows,
      shuffleTime,
      elapse,
      sparkConf)
  }

}
