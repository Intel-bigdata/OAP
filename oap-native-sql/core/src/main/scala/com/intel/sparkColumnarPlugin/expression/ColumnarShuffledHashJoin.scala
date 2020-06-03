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

import java.util.concurrent.TimeUnit._

import com.intel.sparkColumnarPlugin.vectorized.ArrowWritableColumnVector

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}

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

import org.apache.spark.sql.types.{DataType, StructType}
import com.intel.sparkColumnarPlugin.vectorized.ExpressionEvaluator
import com.intel.sparkColumnarPlugin.vectorized.BatchIterator

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
class ColumnarShuffledHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    resultSchema: StructType,
    joinType: JoinType,
    buildSide: BuildSide,
    conditionOption: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    buildTime: SQLMetric,
    joinTime: SQLMetric,
    totalOutputNumRows: SQLMetric
    ) extends Logging{

    var build_cb : ColumnarBatch = null
    var last_cb: ColumnarBatch = null

    val inputBatchHolder = new ListBuffer[ColumnarBatch]()
    // TODO
    val l_input_schema: List[Attribute] = left.output.toList
    val r_input_schema: List[Attribute] = right.output.toList
    logInfo(s"\nleft_schema is ${l_input_schema}, right_schema is ${r_input_schema}, \nleftKeys is ${leftKeys}, rightKeys is ${rightKeys}, \nresultSchema is ${resultSchema}, \njoinType is ${joinType}, buildSide is ${buildSide}, condition is ${conditionOption}")

    val l_input_field_list: List[Field] = l_input_schema.toList.map(attr => {
      Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    })
    val r_input_field_list: List[Field] = r_input_schema.toList.map(attr => {
      Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    })

    val resultFieldList = resultSchema.map(field => {
      Field.nullable(field.name, CodeGeneration.getResultType(field.dataType))
    }).toList

    val leftKeyAttributes = leftKeys.toList.map(expr => {
      ConverterUtils.getAttrFromExpr(expr).asInstanceOf[Expression]
    })
    val rightKeyAttributes = rightKeys.toList.map(expr => {
      ConverterUtils.getAttrFromExpr(expr).asInstanceOf[Expression]
    })

    val lkeyFieldList: List[Field] = leftKeyAttributes.toList.map(expr => {
        val attr = ConverterUtils.getAttrFromExpr(expr)
        Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(expr.dataType))
    })

    val rkeyFieldList: List[Field] = rightKeyAttributes.toList.map(expr => {
        val attr = ConverterUtils.getAttrFromExpr(expr)
        Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(expr.dataType))
    })
    
    val (build_key_field_list, stream_key_field_list, stream_key_ordinal_list, build_input_field_list, stream_input_field_list)
      = buildSide match {
      case BuildLeft =>
        val stream_key_expr_list = bindReferences(rightKeyAttributes, r_input_schema) 
        (lkeyFieldList, rkeyFieldList, stream_key_expr_list.toList.map(_.asInstanceOf[BoundReference].ordinal), l_input_field_list, r_input_field_list)

      case BuildRight =>
        val stream_key_expr_list = bindReferences(leftKeyAttributes, l_input_schema) 
        (rkeyFieldList, lkeyFieldList, stream_key_expr_list.toList.map(_.asInstanceOf[BoundReference].ordinal), r_input_field_list, l_input_field_list)

    }

    val (probe_func_name, build_output_field_list, stream_output_field_list) = joinType match {
      case _: InnerLike =>
        ("conditionedProbeArraysInner", build_input_field_list, stream_input_field_list)
      case LeftSemi =>
        ("conditionedProbeArraysSemi", List[Field](), stream_input_field_list)
      case LeftOuter =>
        ("conditionedProbeArraysOuter", build_input_field_list, stream_input_field_list)
      case RightOuter =>
        ("conditionedProbeArraysOuter", build_input_field_list, stream_input_field_list)
      case LeftAnti =>
        ("conditionedProbeArraysAnti", List[Field](), stream_input_field_list)
      case _ =>
        throw new UnsupportedOperationException(s"Join Type ${joinType} is not supported yet.")
    }

    val build_input_arrow_schema: Schema = new Schema(build_input_field_list.asJava)

    val stream_input_arrow_schema: Schema = new Schema(stream_input_field_list.asJava)

    val build_output_arrow_schema: Schema = new Schema(build_output_field_list.asJava)
    val stream_output_arrow_schema: Schema = new Schema(stream_output_field_list.asJava)

    val stream_key_arrow_schema: Schema = new Schema(stream_key_field_list.asJava)
    var output_arrow_schema: Schema = _


    logInfo(s"\nbuild_key_field_list is ${build_key_field_list}, stream_key_field_list is ${stream_key_field_list}, stream_key_ordinal_list is ${stream_key_ordinal_list}, \nbuild_input_field_list is ${build_input_field_list}, stream_input_field_list is ${stream_input_field_list}, \nbuild_output_field_list is ${build_output_field_list}, stream_output_field_list is ${stream_output_field_list}")

    /////////////////////////////// Create Prober /////////////////////////////
    // Prober is used to insert left table primary key into hashMap
    // Then use iterator to probe right table primary key from hashmap
    // to get corresponding indices of left table
    //
    val condition = conditionOption match {
      case Some(c) =>
        c
      case None =>
        null
    }
    val (conditionInputFieldList, conditionOutputFieldList) = buildSide match {
      case BuildLeft =>
        (build_input_field_list, build_output_field_list ::: stream_output_field_list)
      case BuildRight =>
        (build_input_field_list, stream_output_field_list ::: build_output_field_list)
    }
    val conditionArrowSchema = new Schema(conditionInputFieldList.asJava) 
    output_arrow_schema = new Schema(conditionOutputFieldList.asJava)
    var conditionInputList : java.util.List[Field] = Lists.newArrayList()
    val build_args_node = TreeBuilder.makeFunction(
      "codegen_left_schema",
      build_input_field_list.map(field => {
        TreeBuilder.makeField(field)
      }).asJava, 
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val stream_args_node = TreeBuilder.makeFunction(
      "codegen_right_schema",
      stream_input_field_list.map(field => {
        TreeBuilder.makeField(field)
      }).asJava, 
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val build_keys_node = TreeBuilder.makeFunction(
      "codegen_left_key_schema",
      build_key_field_list.map(field => {
        TreeBuilder.makeField(field)
      }).asJava, 
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val stream_keys_node = TreeBuilder.makeFunction(
      "codegen_right_key_schema",
      stream_key_field_list.map(field => {
        TreeBuilder.makeField(field)
      }).asJava, 
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val condition_expression_node_list : java.util.List[org.apache.arrow.gandiva.expression.TreeNode] =
      if (condition != null) {
        val columnarExpression: Expression =
          ColumnarExpressionConverter.replaceWithColumnarExpression(condition)
        val (condition_expression_node, resultType) =
          columnarExpression.asInstanceOf[ColumnarExpression].doColumnarCodeGen(conditionInputList)
        Lists.newArrayList(build_keys_node, stream_keys_node, condition_expression_node)
      } else {
        Lists.newArrayList(build_keys_node, stream_keys_node)
      }
    val retType = Field.nullable("res", new ArrowType.Int(32, true))

    // Make Expresion for conditionedProbe
    var prober = new ExpressionEvaluator()
    val condition_probe_node = TreeBuilder.makeFunction(
      probe_func_name, 
      condition_expression_node_list,
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val codegen_probe_node = TreeBuilder.makeFunction(
      "codegen_withTwoInputs",
      Lists.newArrayList(condition_probe_node, build_args_node, stream_args_node),
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val condition_probe_expr = TreeBuilder.makeExpression(
      codegen_probe_node, 
      retType) 

    prober.build(build_input_arrow_schema, Lists.newArrayList(condition_probe_expr), true)

    /////////////////////////////// Create Shuffler /////////////////////////////
    // Shuffler will use input indices array to shuffle current table
    // output a new table with new sequence.
    //
    var build_shuffler = new ExpressionEvaluator()
    var probe_iterator: BatchIterator = _
    var build_shuffle_iterator: BatchIterator = _

    // Make Expresion for conditionedShuffle
    val condition_shuffle_node = TreeBuilder.makeFunction(
      "conditionedShuffleArrayList", 
      Lists.newArrayList(),
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val codegen_shuffle_node = TreeBuilder.makeFunction(
      "codegen_withTwoInputs",
      Lists.newArrayList(condition_shuffle_node, build_args_node, stream_args_node),
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val condition_shuffle_expr = TreeBuilder.makeExpression(
      codegen_shuffle_node, 
      retType) 
    build_shuffler.build(conditionArrowSchema, Lists.newArrayList(condition_shuffle_expr), output_arrow_schema, true)


  def columnarInnerJoin(
    streamIter: Iterator[ColumnarBatch],
    buildIter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {

    val beforeBuild = System.nanoTime()

    while (buildIter.hasNext) {
      if (build_cb != null) {
        build_cb = null
      }
      build_cb = buildIter.next()
      val build_rb = ConverterUtils.createArrowRecordBatch(build_cb)
      (0 until build_cb.numCols).toList.foreach(i => build_cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
      inputBatchHolder += build_cb
      prober.evaluate(build_rb)
      build_shuffler.evaluate(build_rb)
      ConverterUtils.releaseArrowRecordBatch(build_rb)
    }
    if (build_cb != null) {
      build_cb = null
    } else {
      val res = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          false
        }

        override def next(): ColumnarBatch = {
          val resultColumnVectors = ArrowWritableColumnVector
            .allocateColumns(0, resultSchema)
            .toArray
          new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
        }
      }
      return res
    }

    // there will be different when condition is null or not null
    probe_iterator = prober.finishByIterator()
    build_shuffler.setDependency(probe_iterator)
    build_shuffle_iterator = build_shuffler.finishByIterator()
    buildTime += NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)
    
    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        if(streamIter.hasNext) {
          true
        } else {
          inputBatchHolder.foreach(cb => cb.close())
          false
        }
      }

      override def next(): ColumnarBatch = {
        val cb = streamIter.next()
        last_cb = cb
        val beforeJoin = System.nanoTime()
        val stream_rb: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(cb)
        probe_iterator.processAndCacheOne(stream_input_arrow_schema, stream_rb)
        val output_rb = build_shuffle_iterator.process(stream_input_arrow_schema, stream_rb)
  
        ConverterUtils.releaseArrowRecordBatch(stream_rb)
        joinTime += NANOSECONDS.toMillis(System.nanoTime() - beforeJoin)
        if (output_rb == null) {
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(0, resultSchema).toArray
          new ColumnarBatch(resultColumnVectors.map(v => v.asInstanceOf[ColumnVector]).toArray, 0)
        } else {
          val outputNumRows = output_rb.getLength
          val output = ConverterUtils.fromArrowRecordBatch(output_arrow_schema, output_rb)
          ConverterUtils.releaseArrowRecordBatch(output_rb)
          totalOutputNumRows += outputNumRows 
          new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]).toArray, outputNumRows)
        }  
      }
    }
  }

  def close(): Unit = {
    if (build_shuffler != null) {
      build_shuffler.close()
      build_shuffler = null
    }
    if (build_shuffle_iterator != null) {
      build_shuffle_iterator.close()
      build_shuffle_iterator = null
    }
    if (prober != null) {
      prober.close()
      prober = null
    }
    if (probe_iterator != null) {
      probe_iterator.close()
      probe_iterator = null
    }
  }
}

object ColumnarShuffledHashJoin {
  var columnarShuffedHahsJoin: ColumnarShuffledHashJoin = _
  def create(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    resultSchema: StructType,
    joinType: JoinType,
    buildSide: BuildSide, 
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    buildTime: SQLMetric,
    joinTime: SQLMetric,
    numOutputRows: SQLMetric
    ): ColumnarShuffledHashJoin = synchronized {
    columnarShuffedHahsJoin = new ColumnarShuffledHashJoin(leftKeys, rightKeys, resultSchema, joinType, buildSide, condition, left, right, buildTime, joinTime, numOutputRows)
    columnarShuffedHahsJoin
  }

  def close(): Unit = {
    if (columnarShuffedHahsJoin != null) {
      columnarShuffedHahsJoin.close()
    }
  }
}
