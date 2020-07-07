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

class ColumnarIf(predicate: Expression, trueValue: Expression,
                 falseValue: Expression, original: Expression)
    extends If(predicate: Expression, trueValue: Expression, falseValue: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
      val (predicate_node, predicateType): (TreeNode, ArrowType) =
      predicate.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (true_node, trueType): (TreeNode, ArrowType) =
        trueValue.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (false_node, falseType): (TreeNode, ArrowType) =
        falseValue.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

      val funcNode = TreeBuilder.makeIf(predicate_node, true_node, false_node, trueType)
      (funcNode, trueType)
  }
}

object ColumnarIfOperator {

  def create(predicate: Expression, trueValue: Expression,
             falseValue: Expression, original: Expression): Expression = original match {
    case i: If =>
      new ColumnarIf(predicate, trueValue, falseValue, original)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
