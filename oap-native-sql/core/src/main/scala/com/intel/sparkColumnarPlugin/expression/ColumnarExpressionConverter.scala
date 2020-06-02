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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
object ColumnarExpressionConverter extends Logging {

  var check_if_no_calculation = true

  def replaceWithColumnarExpression(expr: Expression, attributeSeq: Seq[Attribute] = null): Expression = expr match {
    case a: Alias =>
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      new ColumnarAlias(replaceWithColumnarExpression(a.child, attributeSeq), a.name)(
        a.exprId,
        a.qualifier,
        a.explicitMetadata)
    case a: AttributeReference =>
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      if (attributeSeq != null) {
        val bindReference = BindReferences.bindReference(expr, attributeSeq, true)
        if (bindReference == expr) {
          return new ColumnarAttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId, a.qualifier) 
        }
        val b = bindReference.asInstanceOf[BoundReference]
        new ColumnarBoundReference(b.ordinal, b.dataType, b.nullable)
      } else {
        return new ColumnarAttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId, a.qualifier) 
      }
    case lit: Literal =>
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      new ColumnarLiteral(lit)
    case binArith: BinaryArithmetic =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarBinaryArithmetic.create(
        replaceWithColumnarExpression(binArith.left, attributeSeq),
        replaceWithColumnarExpression(binArith.right, attributeSeq),
        expr)
    case b: BoundReference =>
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      new ColumnarBoundReference(b.ordinal, b.dataType, b.nullable)
    case b: BinaryOperator =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarBinaryOperator.create(
        replaceWithColumnarExpression(b.left, attributeSeq),
        replaceWithColumnarExpression(b.right, attributeSeq),
        expr)
    case sp: StringPredicate =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarBinaryOperator.create(
        replaceWithColumnarExpression(sp.left, attributeSeq),
        replaceWithColumnarExpression(sp.right, attributeSeq),
        expr)
    case sr: StringRegexExpression =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarBinaryOperator.create(
        replaceWithColumnarExpression(sr.left, attributeSeq),
        replaceWithColumnarExpression(sr.right, attributeSeq),
        expr)
    case i: If =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarIfOperator.create(
        replaceWithColumnarExpression(i.predicate, attributeSeq),
        replaceWithColumnarExpression(i.trueValue, attributeSeq),
        replaceWithColumnarExpression(i.falseValue, attributeSeq),
        expr)
    case cw: CaseWhen =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      val colBranches = cw.branches.map{ expr => {
          (replaceWithColumnarExpression(expr._1, attributeSeq), replaceWithColumnarExpression(expr._2, attributeSeq))
        }
      }
      val colElseValue = cw.elseValue.map { expr => {
          replaceWithColumnarExpression(expr, attributeSeq)
        }
      }

      logInfo(s"col_branches: $colBranches")
      logInfo(s"col_else: $colElseValue")
      ColumnarCaseWhenOperator.create(
        colBranches,
        colElseValue,
        expr)
    case c: Coalesce =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      val exps = c.children.map{ expr =>
        replaceWithColumnarExpression(expr, attributeSeq)
      }
      ColumnarCoalesceOperator.create(
        exps,
        expr)
    case i: In =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarInOperator.create(
        replaceWithColumnarExpression(i.value, attributeSeq),
        i.list,
        expr)
    case i: InSet =>
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      check_if_no_calculation = false
      ColumnarInSetOperator.create(
        replaceWithColumnarExpression(i.child, attributeSeq),
        i.hset,
        expr)
    case ss: Substring =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarTernaryOperator.create(
        replaceWithColumnarExpression(ss.str, attributeSeq),
        replaceWithColumnarExpression(ss.pos),
        replaceWithColumnarExpression(ss.len),
        expr)
    case u: UnaryExpression =>
      if (!u.isInstanceOf[Cast]) {
        check_if_no_calculation = false
      }
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarUnaryOperator.create(replaceWithColumnarExpression(u.child, attributeSeq), expr)
    case s: org.apache.spark.sql.execution.ScalarSubquery =>
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      new ColumnarScalarSubquery(s)
    case expr =>
      logWarning(s"${expr.getClass} ${expr} is not currently supported.")
      expr
  }

  def ifNoCalculation = check_if_no_calculation

  def reset(): Unit = {
    check_if_no_calculation = true
  }

}
