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

import java.util.concurrent.atomic.AtomicLong
import io.netty.buffer.ArrowBuf

import com.intel.sparkColumnarPlugin.vectorized.ArrowWritableColumnVector

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object ConverterUtils extends Logging {
  def createArrowRecordBatch(columnarBatch: ColumnarBatch): ArrowRecordBatch = {
    val fieldNodes = new ListBuffer[ArrowFieldNode]()
    val inputData = new ListBuffer[ArrowBuf]()
    val numRowsInBatch = columnarBatch.numRows()
    for (i <- 0 until columnarBatch.numCols()) {
      val inputVector =
        columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
      fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      //FIXME for projection + in test
      //fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      inputData += inputVector.getValidityBuffer()
      if (inputVector.isInstanceOf[VarCharVector]) {
        inputData += inputVector.getOffsetBuffer()
      }
      inputData += inputVector.getDataBuffer()
      //FIXME for projection + in test
      //inputData += inputVector.getValidityBuffer()
    }
    new ArrowRecordBatch(numRowsInBatch, fieldNodes.toList.asJava, inputData.toList.asJava)
  }

  def createArrowRecordBatch(numRowsInBatch: Int, cols: List[ValueVector]): ArrowRecordBatch = {
    val fieldNodes = new ListBuffer[ArrowFieldNode]()
    val inputData = new ListBuffer[ArrowBuf]()
    cols.foreach(inputVector => {
      fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      inputData += inputVector.getValidityBuffer()
      if (inputVector.isInstanceOf[VarCharVector]) {
        inputData += inputVector.getOffsetBuffer()
      }
      inputData += inputVector.getDataBuffer()
    })
    new ArrowRecordBatch(numRowsInBatch, fieldNodes.toList.asJava, inputData.toList.asJava)
  }

  def fromArrowRecordBatch(recordBatchSchema: Schema, recordBatch: ArrowRecordBatch): Array[ArrowWritableColumnVector] = {
    val numRows = recordBatch.getLength()
    ArrowWritableColumnVector.loadColumns(numRows, recordBatchSchema, recordBatch)
  }

  def releaseArrowRecordBatch(recordBatch: ArrowRecordBatch): Unit = {
    if (recordBatch != null) {
      recordBatch.close()
    }
  }

  def releaseArrowRecordBatchList(recordBatchList: Array[ArrowRecordBatch]): Unit = {
    recordBatchList.foreach({ recordBatch =>
      if (recordBatch != null)
        releaseArrowRecordBatch(recordBatch)
    })
  }

  def getAttrFromExpr(fieldExpr: Expression, skipAlias: Boolean = false): AttributeReference = {
    fieldExpr match {
      case a: Cast =>
        getAttrFromExpr(a.child)
      case a: AggregateExpression =>
        getAttrFromExpr(a.aggregateFunction.children(0))
      case a: AttributeReference =>
        a
      case a: Alias =>
        if (skipAlias && a.child.isInstanceOf[AttributeReference])
          getAttrFromExpr(a.child)
        else
          a.toAttribute.asInstanceOf[AttributeReference]
      case a: KnownFloatingPointNormalized =>
        logInfo(s"$a")
        getAttrFromExpr(a.child)
      case a: NormalizeNaNAndZero =>
        getAttrFromExpr(a.child)
      case c: Coalesce =>
        getAttrFromExpr(c.children(0))
      case i: IsNull =>
        getAttrFromExpr(i.child)
      case other =>
        throw new UnsupportedOperationException(s"makeStructField is unable to parse from $other (${other.getClass}).")
    }
  }

  def getResultAttrFromExpr(fieldExpr: Expression, name: String = "None"): AttributeReference = {
    fieldExpr match {
      case a: Cast =>
        getResultAttrFromExpr(a.child, name)
      case a: AttributeReference =>
        if (name != "None") {
          new AttributeReference(name, a.dataType, a.nullable)()
        } else {
          a
        }
      case a: Alias =>
        //TODO: a walkaround since we didn't support cast yet
        if (a.child.isInstanceOf[Cast]) {
          val tmp = if (name != "None") {
            new Alias(a.child.asInstanceOf[Cast].child, name)(a.exprId, a.qualifier, a.explicitMetadata)
          } else {
            new Alias(a.child.asInstanceOf[Cast].child, a.name)(a.exprId, a.qualifier, a.explicitMetadata)
          }
          tmp.toAttribute.asInstanceOf[AttributeReference]
        } else {
          if (name != "None") {
            val tmp = a.toAttribute.asInstanceOf[AttributeReference]
            new AttributeReference(name, tmp.dataType, tmp.nullable)()
          } else {
            a.toAttribute.asInstanceOf[AttributeReference]
          }
        }
      case other =>
        val a = if (name != "None") {
          new Alias(other, name)()
        } else {
          new Alias(other, "res")()
        }
        a.toAttribute.asInstanceOf[AttributeReference]
    }
  }

  def ifEquals(left: Seq[AttributeReference], right: Seq[NamedExpression]): Boolean = {
    if (left.size != right.size) return false
    for (i <- 0 until left.size) {
      if (left(i).exprId != right(i).exprId) return false
    }
    true
  }

  def combineArrowRecordBatch(rb1: ArrowRecordBatch, rb2: ArrowRecordBatch): ArrowRecordBatch = {
     val numRows = rb1.getLength()
     val rb1_nodes = rb1.getNodes()
     val rb2_nodes = rb2.getNodes()
     val rb1_bufferlist = rb1.getBuffers()
     val rb2_bufferlist = rb2.getBuffers()

     val combined_nodes = rb1_nodes.addAll(rb2_nodes)
     val combined_bufferlist = rb1_bufferlist.addAll(rb2_bufferlist)
     new ArrowRecordBatch(numRows, rb1_nodes, rb1_bufferlist)
  }

  override def toString(): String = {
    s"ConverterUtils"
  }

}

