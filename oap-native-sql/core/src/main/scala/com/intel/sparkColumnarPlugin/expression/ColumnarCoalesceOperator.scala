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
import com.google.common.collect.Sets
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
 * An expression that is evaluated to the first non-null input.
 * {{{
 *   coalesce(1, 2) => 1
 *   coalesce(null, 1, 2) => 1
 *   coalesce(null, null, 2) => 2
 * }}}
**/
//TODO(): coalesce(null, null, null) => null

class ColumnarCoalesce(exps: Seq[Expression], original: Expression)
    extends Coalesce(exps: Seq[Expression])
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val iter: Iterator[Expression] = exps.iterator
    val exp = exps.head

    val (exp_node, expType): (TreeNode, ArrowType) =
      exp.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val isnotnullNode =
      TreeBuilder.makeFunction("isnotnull", Lists.newArrayList(exp_node), new ArrowType.Bool())

    val funcNode = TreeBuilder.makeIf(isnotnullNode, exp_node, innerIf(args, exps, iter), expType)
    (funcNode, expType)
  }

  def innerIf(args: java.lang.Object, exps: Seq[Expression], iter: Iterator[Expression]): TreeNode = {
    if (!iter.hasNext) {
      // Return the last element no matter if it is null
      val (exp_node, expType): (TreeNode, ArrowType) =
        exps.last.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val isnotnullNode =
        TreeBuilder.makeFunction("isnotnull", Lists.newArrayList(exp_node), new ArrowType.Bool())
      val funcNode = TreeBuilder.makeIf(isnotnullNode, exp_node, exp_node, expType)
      funcNode
    } else {
      val exp = iter.next()
      val (exp_node, expType): (TreeNode, ArrowType) =
        exp.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val isnotnullNode =
        TreeBuilder.makeFunction("isnotnull", Lists.newArrayList(exp_node), new ArrowType.Bool())
      val funcNode = TreeBuilder.makeIf(isnotnullNode, exp_node, innerIf(args, exps, iter), expType)
      funcNode
    }
  }
}

object ColumnarCoalesceOperator {

  def create(exps: Seq[Expression], original: Expression): Expression = original match {
    case c: Coalesce =>
      new ColumnarCoalesce(exps, original)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
