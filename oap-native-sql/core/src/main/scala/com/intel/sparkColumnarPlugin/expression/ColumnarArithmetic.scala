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

import com.google.common.collect.Lists

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * A version of add that supports columnar processing for longs.
 */
class ColumnarAdd(left: Expression, right: Expression, original: Expression)
    extends Add(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = CodeGeneration.getResultType(left_type, right_type)
    if (!left_type.equals(resultType)) {
      val func_name = CodeGeneration.getCastFuncName(resultType)
      left_node =
        TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), resultType),
    }
    if (!right_type.equals(resultType)) {
      val func_name = CodeGeneration.getCastFuncName(resultType)
      right_node =
        TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), resultType),
    }

    //logInfo(s"(TreeBuilder.makeFunction(add, Lists.newArrayList($left_node, $right_node), $resultType), $resultType)")
    (
      TreeBuilder.makeFunction("add", Lists.newArrayList(left_node, right_node), resultType),
      resultType)
  }
}

class ColumnarSubtract(left: Expression, right: Expression, original: Expression)
    extends Subtract(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = CodeGeneration.getResultType(left_type, right_type)
    if (!left_type.equals(resultType)) {
      val func_name = CodeGeneration.getCastFuncName(resultType)
      left_node =
        TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), resultType),
    }
    if (!right_type.equals(resultType)) {
      val func_name = CodeGeneration.getCastFuncName(resultType)
      right_node =
        TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), resultType),
    }
    //logInfo(s"(TreeBuilder.makeFunction(multiply, Lists.newArrayList($left_node, $right_node), $resultType), $resultType)")
    (
      TreeBuilder.makeFunction("subtract", Lists.newArrayList(left_node, right_node), resultType),
      resultType)
  }
}

class ColumnarMultiply(left: Expression, right: Expression, original: Expression)
    extends Multiply(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = CodeGeneration.getResultType(left_type, right_type)
    if (!left_type.equals(resultType)) {
      val func_name = CodeGeneration.getCastFuncName(resultType)
      left_node =
        TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), resultType),
    }
    if (!right_type.equals(resultType)) {
      val func_name = CodeGeneration.getCastFuncName(resultType)
      right_node =
        TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), resultType),
    }

    //logInfo(s"(TreeBuilder.makeFunction(multiply, Lists.newArrayList($left_node, $right_node), $resultType), $resultType)")
    (
      TreeBuilder.makeFunction("multiply", Lists.newArrayList(left_node, right_node), resultType),
      resultType)
  }
}

class ColumnarDivide(left: Expression, right: Expression, original: Expression)
    extends Divide(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = CodeGeneration.getResultType(left_type, right_type)
    if (!left_type.equals(resultType)) {
      val func_name = CodeGeneration.getCastFuncName(resultType)
      left_node =
        TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), resultType),
    }
    if (!right_type.equals(resultType)) {
      val func_name = CodeGeneration.getCastFuncName(resultType)
      right_node =
        TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), resultType),
    }
    //logInfo(s"(TreeBuilder.makeFunction(multiply, Lists.newArrayList($left_node, $right_node), $resultType), $resultType)")
    (
      TreeBuilder.makeFunction("divide", Lists.newArrayList(left_node, right_node), resultType),
      resultType)
  }
}

object ColumnarBinaryArithmetic {

  def create(left: Expression, right: Expression, original: Expression): Expression =
    original match {
      case a: Add =>
        new ColumnarAdd(left, right, a)
      case s: Subtract =>
        new ColumnarSubtract(left, right, s)
      case m: Multiply =>
        new ColumnarMultiply(left, right, m)
      case d: Divide =>
        new ColumnarDivide(left, right, d)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
}
