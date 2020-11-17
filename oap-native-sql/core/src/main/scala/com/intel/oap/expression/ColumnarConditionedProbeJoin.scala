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

import java.util.concurrent.TimeUnit._

import com.intel.oap.ColumnarPluginConfig
import com.intel.oap.vectorized.ArrowWritableColumnVector
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}

import scala.collection.JavaConverters._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.mutable.ListBuffer
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._
import io.netty.buffer.ArrowBuf
import com.google.common.collect.Lists
import org.apache.spark.sql.types.{DataType, DecimalType, StructType}
import com.intel.oap.vectorized.ExpressionEvaluator
import com.intel.oap.vectorized.BatchIterator

object ColumnarConditionedProbeJoin extends Logging {
  def prepareHashBuildFunction(
      buildKeys: Seq[Expression],
      buildInputAttributes: Seq[Attribute],
      builder_type: Int = 0,
      is_broadcast: Boolean = false): TreeNode = {
    val buildInputFieldList: List[Field] = buildInputAttributes.toList.map(attr => {
      if (attr.dataType.isInstanceOf[DecimalType])
        throw new UnsupportedOperationException(
          s"Decimal type is not supported in ColumnarShuffledHashJoin.")
      Field
        .nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    })
    val buildKeysFunctionList: List[TreeNode] = buildKeys.toList.map(expr => {
      val (nativeNode, returnType) = if (!is_broadcast) {
        ConverterUtils.getColumnarFuncNode(expr)
      } else {
        ConverterUtils.getColumnarFuncNode(expr, buildInputAttributes)
      }
      if (s"${nativeNode.toProtobuf}".contains("none#")) {
        throw new UnsupportedOperationException(
          s"Unsupport to generate native expression from replaceable expression.")
      }
      nativeNode
    })
    val build_keys_node = TreeBuilder.makeFunction(
      "hash_key_schema",
      buildKeysFunctionList.asJava,
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
    val builder_type_node = TreeBuilder.makeLiteral(builder_type.asInstanceOf[Integer])
    val build_keys_config_node = TreeBuilder.makeFunction(
      "build_keys_config_node",
      Lists.newArrayList(builder_type_node),
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
    // Make Expresion for conditionedProbe
    val hash_relation_kernel = TreeBuilder.makeFunction(
      "HashRelation",
      Lists.newArrayList(build_keys_node, build_keys_config_node),
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
    TreeBuilder.makeFunction(
      s"standalone",
      Lists.newArrayList(hash_relation_kernel),
      new ArrowType.Int(32, true))
  }

  def prepareKernelFunction(
      buildKeys: Seq[Expression],
      streamKeys: Seq[Expression],
      buildInputAttributes: Seq[Attribute],
      streamInputAttributes: Seq[Attribute],
      output: Seq[Attribute],
      joinType: JoinType,
      buildSide: BuildSide,
      conditionOption: Option[Expression],
      builder_type: Int = 0): TreeNode = {
    val buildInputFieldList: List[Field] = buildInputAttributes.toList.map(attr => {
      if (attr.dataType.isInstanceOf[DecimalType])
        throw new UnsupportedOperationException(
          s"Decimal type is not supported in ColumnarShuffledHashJoin.")
      Field
        .nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    })
    val streamInputFieldList: List[Field] = streamInputAttributes.toList.map(attr => {
      if (attr.dataType.isInstanceOf[DecimalType])
        throw new UnsupportedOperationException(
          s"Decimal type is not supported in ColumnarShuffledHashJoin.")
      Field
        .nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    })

    val buildKeysFunctionList: List[TreeNode] = buildKeys.toList.map(expr => {
      val (nativeNode, returnType) = ConverterUtils.getColumnarFuncNode(expr)
      if (s"${nativeNode.toProtobuf}".contains("none#")) {
        throw new UnsupportedOperationException(
          s"Unsupport to generate native expression from replaceable expression.")
      }
      nativeNode
    })

    val streamKeysFunctionList: List[TreeNode] = streamKeys.toList.map(expr => {
      val (nativeNode, returnType) = ConverterUtils.getColumnarFuncNode(expr)
      if (s"${nativeNode.toProtobuf}".contains("none#")) {
        throw new UnsupportedOperationException(
          s"Unsupport to generate native expression from replaceable expression.")
      }
      nativeNode
    })

    val resultFunctionList: List[TreeNode] = output.toList.map(field => {
      val field_node = Field.nullable(
        s"${field.name}#${field.exprId.id}",
        CodeGeneration.getResultType(field.dataType))
      TreeBuilder.makeField(field_node)
    })

    var existsField: Field = null
    var existsIndex: Int = -1
    val probeFuncName = joinType match {
      case _: InnerLike =>
        "conditionedProbeArraysInner"
      case LeftSemi =>
        "conditionedProbeArraysSemi"
      case LeftOuter =>
        "conditionedProbeArraysOuter"
      case RightOuter =>
        "conditionedProbeArraysOuter"
      case LeftAnti =>
        "conditionedProbeArraysAnti"
      case j: ExistenceJoin =>
        "conditionedProbeArraysExistence"
      case _ =>
        throw new UnsupportedOperationException(s"Join Type ${joinType} is not supported yet.")
    }

    val condition = conditionOption.getOrElse(null)
    var conditionInputList: java.util.List[Field] = Lists.newArrayList()
    val build_args_node = TreeBuilder.makeFunction(
      "codegen_left_schema",
      buildInputFieldList.map(field => { TreeBuilder.makeField(field) }).asJava,
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
    val stream_args_node = TreeBuilder.makeFunction(
      "codegen_right_schema",
      streamInputFieldList.map(field => { TreeBuilder.makeField(field) }).asJava,
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
    val build_keys_node = TreeBuilder.makeFunction(
      "codegen_left_key_schema",
      buildKeysFunctionList.asJava,
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
    val stream_keys_node = TreeBuilder.makeFunction(
      "codegen_right_key_schema",
      streamKeysFunctionList.asJava,
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
    val result_node = TreeBuilder.makeFunction(
      "codegen_result_schema",
      resultFunctionList.asJava,
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
    val build_keys_config_node = TreeBuilder.makeFunction(
      "build_keys_config_node",
      Lists.newArrayList(TreeBuilder.makeLiteral(builder_type.asInstanceOf[Integer])),
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
    val condition_expression_node_list: java.util.List[TreeNode] =
      if (condition != null) {
        val columnarExpression: Expression =
          ColumnarExpressionConverter.replaceWithColumnarExpression(condition)
        val (condition_expression_node, resultType) =
          columnarExpression
            .asInstanceOf[ColumnarExpression]
            .doColumnarCodeGen(conditionInputList)
        Lists.newArrayList(
          build_args_node,
          stream_args_node,
          build_keys_node,
          stream_keys_node,
          result_node,
          build_keys_config_node,
          condition_expression_node)
      } else {
        Lists.newArrayList(
          build_args_node,
          stream_args_node,
          build_keys_node,
          stream_keys_node,
          result_node,
          build_keys_config_node)
      }
    val retType = Field.nullable("res", new ArrowType.Int(32, true))

    // Make Expresion for conditionedProbe
    TreeBuilder.makeFunction(
      probeFuncName,
      condition_expression_node_list,
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
  }

}
