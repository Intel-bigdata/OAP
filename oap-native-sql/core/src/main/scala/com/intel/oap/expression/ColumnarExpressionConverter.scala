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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
object ColumnarExpressionConverter extends Logging {

  var check_if_no_calculation = true

  def replaceWithColumnarExpression(
      expr: Expression,
      attributeSeq: Seq[Attribute] = null,
      expIdx: Int = -1,
      convertBoundRefToAttrRef: Boolean = false): Expression =
    expr match {
      case a: Alias =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        new ColumnarAlias(
          replaceWithColumnarExpression(
            a.child,
            attributeSeq,
            expIdx,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          a.name)(a.exprId, a.qualifier, a.explicitMetadata)
      case a: AttributeReference =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        if (attributeSeq != null) {
          val bindReference =
            BindReferences.bindReference(expr, attributeSeq, true)
          if (bindReference == expr) {
            if (expIdx == -1) {
              return new ColumnarAttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
                a.exprId,
                a.qualifier)
            } else {
              return new ColumnarBoundReference(expIdx, a.dataType, a.nullable)
            }
          }
          val b = bindReference.asInstanceOf[BoundReference]
          new ColumnarBoundReference(b.ordinal, b.dataType, b.nullable)
        } else {
          return new ColumnarAttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
            a.exprId,
            a.qualifier)
        }
      case lit: Literal =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        new ColumnarLiteral(lit)
      case binArith: BinaryArithmetic =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryArithmetic.create(
          replaceWithColumnarExpression(
            binArith.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            binArith.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case b: BoundReference =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        if (convertBoundRefToAttrRef && attributeSeq != null) {
          val a = attributeSeq(b.ordinal)
          new ColumnarAttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
            a.exprId,
            a.qualifier)
        } else {
          new ColumnarBoundReference(b.ordinal, b.dataType, b.nullable)
        }
      case b: BinaryOperator =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryOperator.create(
          replaceWithColumnarExpression(
            b.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            b.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case b: ShiftLeft =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryOperator.create(
          replaceWithColumnarExpression(
            b.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            b.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case b: ShiftRight =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryOperator.create(
          replaceWithColumnarExpression(
            b.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            b.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case sp: StringPredicate =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryOperator.create(
          replaceWithColumnarExpression(
            sp.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            sp.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case sr: StringRegexExpression =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryOperator.create(
          replaceWithColumnarExpression(
            sr.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            sr.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case i: If =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarIfOperator.create(
          replaceWithColumnarExpression(
            i.predicate,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            i.trueValue,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            i.falseValue,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case cw: CaseWhen =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        val colBranches = cw.branches.map { expr =>
          {
            (
              replaceWithColumnarExpression(
                expr._1,
                attributeSeq,
                convertBoundRefToAttrRef = convertBoundRefToAttrRef),
              replaceWithColumnarExpression(
                expr._2,
                attributeSeq,
                convertBoundRefToAttrRef = convertBoundRefToAttrRef))
          }
        }
        val colElseValue = cw.elseValue.map { expr =>
          {
            replaceWithColumnarExpression(
              expr,
              attributeSeq,
              convertBoundRefToAttrRef = convertBoundRefToAttrRef)
          }
        }

        logInfo(s"col_branches: $colBranches")
        logInfo(s"col_else: $colElseValue")
        val rename = if (expIdx == -1) false else true
        ColumnarCaseWhenOperator.create(colBranches, colElseValue, expr, rename)
      case c: Coalesce =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        val exps = c.children.map { expr =>
          replaceWithColumnarExpression(
            expr,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef)
        }
        ColumnarCoalesceOperator.create(exps, expr)
      case i: In =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarInOperator.create(
          replaceWithColumnarExpression(
            i.value,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          i.list,
          expr)
      case i: InSet =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        check_if_no_calculation = false
        ColumnarInSetOperator.create(
          replaceWithColumnarExpression(
            i.child,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          i.hset,
          expr)
      case ss: Substring =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarTernaryOperator.create(
          replaceWithColumnarExpression(
            ss.str,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            ss.pos,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            ss.len,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case u: UnaryExpression =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarUnaryOperator.create(
          replaceWithColumnarExpression(
            u.child,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case oaps: com.intel.oap.expression.ColumnarScalarSubquery =>
        oaps
      case s: org.apache.spark.sql.execution.ScalarSubquery =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        new ColumnarScalarSubquery(s)
      case c: Concat =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        val exps = c.children.map { expr =>
          replaceWithColumnarExpression(
            expr,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef)
        }
        ColumnarConcatOperator.create(exps, expr)
      case r: Round =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarRoundOperator.create(
          replaceWithColumnarExpression(
            r.child,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            r.scale,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case b: BinaryExpression =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryExpression.create(
          replaceWithColumnarExpression(
            b.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            b.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case expr =>
        throw new UnsupportedOperationException(
          s" --> ${expr.getClass} | ${expr} is not currently supported.")
    }

  def ifNoCalculation = check_if_no_calculation

  def reset(): Unit = {
    check_if_no_calculation = true
  }

}
