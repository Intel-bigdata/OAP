package com.intel.sparkColumnarPlugin.expression

import java.util.concurrent.TimeUnit._
import com.google.common.collect.Lists

import com.intel.sparkColumnarPlugin.vectorized.ArrowWritableColumnVector
import com.intel.sparkColumnarPlugin.vectorized.ExpressionEvaluator
import com.intel.sparkColumnarPlugin.vectorized.BatchIterator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.TaskContext

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
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
    outputAttributes: Seq[Attribute],
    sortTime: SQLMetric,
    outputBatches: SQLMetric,
    outputRows: SQLMetric)
    extends Logging {

  logInfo(s"ColumnarSorter sortOrder is ${sortOrder}, outputAttributes is ${outputAttributes}")
  if (sortOrder.length != 1) {
    throw new UnsupportedOperationException("ColumnarSorter does not support multiple key sort yet.")
  }
  /////////////// Prepare ColumnarSorter //////////////
  var numInputBatches: Long = 0
  var processedNumRows: Long = 0
  var numOutputBatches: Long = 0
  var numOutputRows: Long = 0
  var finish_elapse : Long = 0
  var elapse: Long = 0
  var hasNextCalledTimes: Long = 0
  val inputBatchHolder = new ListBuffer[ArrowRecordBatch]() 
  val keyField: Field = {
    val attr = ConverterUtils.getAttrFromExpr(sortOrder.head.child)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  }
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

  val gandivaExpressionTree: List[ExpressionTree] = inputFieldList.map( inputField => {
    val funcName = "action_dono"
    val resultType = inputField.getType()
    val sortNode = TreeBuilder.makeFunction(
        sortFuncName,
        Lists.newArrayList(TreeBuilder.makeField(keyField)),
        keyField.getType())
    val shuffleNode = TreeBuilder.makeFunction(
        "shuffleArrayList",
        (sortNode :: inputFieldList.map({field => TreeBuilder.makeField(field)})).asJava,
        resultType)
    val funcNode = TreeBuilder.makeFunction(
        funcName,
        Lists.newArrayList(shuffleNode, TreeBuilder.makeField(inputField)),
        resultType)

    TreeBuilder.makeExpression(funcNode, inputField)
  })

  var sorter = new ExpressionEvaluator()
  val arrowSchema = new Schema(inputFieldList.asJava)

  val resultSchema = StructType(outputAttributes.map(expr => {
    val attr = ConverterUtils.getAttrFromExpr(expr)
    StructField(s"${attr.name}", attr.dataType, true)
  }).toArray)
  val recordBatchSchema = new Schema(outputFieldList.asJava)

  logInfo(s"inputFieldList is ${inputFieldList}, outputFieldList is ${outputFieldList}")
  sorter.build(arrowSchema, gandivaExpressionTree.asJava, true/*return at finish*/)

  /////////////////////////////////////////////////////
  
  def close(): Unit = {
    logInfo("ColumnarSorter ExpressionEvaluator closed");
    logInfo(s"Sort Completed, total processed ${numInputBatches} batches, ${processedNumRows} rows, output ${numOutputBatches} batches and ${numOutputRows} rows, took ${NANOSECONDS.toMillis(elapse)} ms handling one file(including fetching + processing), ${NANOSECONDS.toMillis(finish_elapse)} ms doing finish process, hasNext called ${hasNextCalledTimes} times.")
    sorter.close()
    sorter = null
  }

  def updateSorterResult(input: ColumnarBatch): Unit = {
    val input_batch = ConverterUtils.createArrowRecordBatch(input)
    inputBatchHolder += input_batch
    sorter.evaluate(input_batch)
  }

  def getSorterResultByIterator(): BatchIterator = {
    if (processedNumRows == 0) {
      return new BatchIterator();
    }
    return sorter.finishByIterator();
  }

  def getSorterResult(resultBatch: ArrowRecordBatch): ColumnarBatch = {
    if (resultBatch == null) {
      val resultColumnVectors =
        ArrowWritableColumnVector.allocateColumns(0, resultSchema).toArray
      new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
    } else {
      val resultColumnVectorList = ConverterUtils.fromArrowRecordBatch(
        recordBatchSchema, resultBatch)

      new ColumnarBatch(resultColumnVectorList.map(v => v.asInstanceOf[ColumnVector]), resultBatch.getLength())
    }
  }

  def createIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var cb: ColumnarBatch = null
      var nextBatch: ArrowRecordBatch = null
      var batchIterator: BatchIterator = null
      var eval_elapse : Long = 0

      override def hasNext: Boolean = {
        hasNextCalledTimes += 1
        if (batchIterator == null) {
          val beforeSort = System.nanoTime()
          while (cbIterator.hasNext) {
            if (cb != null) {
              cb.close()
              cb = null
            }
            cb = cbIterator.next()

            val beforeEval = System.nanoTime()
            if (cb.numRows > 0) {
              updateSorterResult(cb)
              processedNumRows += cb.numRows
              numInputBatches += 1
            }
            eval_elapse += System.nanoTime() - beforeEval
          }
          if (cb != null) {
            cb.close()
            cb = null
          }
          elapse = System.nanoTime() - beforeSort
          sortTime.set(NANOSECONDS.toMillis(elapse))
          batchIterator = getSorterResultByIterator()
        }

        val beforeFinish = System.nanoTime()
        nextBatch = batchIterator.next()
        finish_elapse += System.nanoTime() - beforeFinish

        if (nextBatch == null) {
          ConverterUtils.releaseArrowRecordBatchList(inputBatchHolder.toArray)
          return false
        } else {
          outputBatches += 1
          outputRows += nextBatch.getLength()
          numOutputBatches += 1
          numOutputRows += nextBatch.getLength()
          return true
        }
      }

      override def next(): ColumnarBatch = {
        val outputBatch = getSorterResult(nextBatch)
        ConverterUtils.releaseArrowRecordBatch(nextBatch)

        outputBatch
      }
    }
  }

}

object ColumnarSorter {
  def create(
      sortOrder: Seq[SortOrder],
      outputAttributes: Seq[Attribute],
      sortTime: SQLMetric,
      outputBatches: SQLMetric,
      outputRows: SQLMetric): ColumnarSorter = synchronized {
    new ColumnarSorter(
      sortOrder,
      outputAttributes,
      sortTime,
      outputBatches,
      outputRows)
  }

}
