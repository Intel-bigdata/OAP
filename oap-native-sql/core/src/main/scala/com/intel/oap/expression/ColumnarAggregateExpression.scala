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

import scala.collection.immutable.List
import scala.collection.JavaConverters._
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

trait ColumnarAggregateExpressionBase extends ColumnarExpression with Logging {
  def requiredColNum: Int
  def expectedResColNum: Int
  def setInputFields(fieldList: List[Field]): Unit = {}
  def doColumnarCodeGen_ext(args: Object): TreeNode = {
    throw new UnsupportedOperationException(s"ColumnarAggregateExpressionBase doColumnarCodeGen_ext is a abstract function.")
  }
}

class ColumnarUniqueAggregateExpression(aggrFieldList: List[Field]) extends ColumnarAggregateExpressionBase with Logging {

  override def requiredColNum: Int = 1
  override def expectedResColNum: Int = 1
  override def doColumnarCodeGen_ext(args: Object): TreeNode = {
    val (keyFieldList, inputFieldList, resultType, resultField) =
      args.asInstanceOf[(List[Field], List[Field], ArrowType, Field)]
    val funcName = "action_unique"
    val inputNode = {
      // if keyList has keys, we need to do groupby by these keys.
      var inputFieldNode = 
        keyFieldList.map({field => TreeBuilder.makeField(field)}).asJava
      val encodeNode = TreeBuilder.makeFunction(
        "encodeArray",
        inputFieldNode,
        resultType/*this arg won't be used*/)
      inputFieldNode = 
        (encodeNode :: inputFieldList.map({field => TreeBuilder.makeField(field)})).asJava
      val groupByFuncNode = TreeBuilder.makeFunction(
        "splitArrayListWithAction",
        inputFieldNode,
        resultType/*this arg won't be used*/)
      List(groupByFuncNode) ::: aggrFieldList.map(field => TreeBuilder.makeField(field))
    }
    logInfo(s"${funcName}(${inputNode})")
    TreeBuilder.makeFunction(
        funcName,
        inputNode.asJava,
        resultType)
  }
}

class ColumnarAggregateExpression(
    aggregateFunction: AggregateFunction,
    mode: AggregateMode,
    isDistinct: Boolean,
    resultId: ExprId)
    extends AggregateExpression(aggregateFunction, mode, isDistinct, None, resultId)
    with ColumnarAggregateExpressionBase
    with Logging {

  var aggrFieldList: List[Field] = _
  val (funcName, argSize, resSize) = mode match {
    case Partial | PartialMerge =>
      aggregateFunction.prettyName match {
        case "avg" => ("sum_count", 1, 2)
        case "count" => {
          if (aggregateFunction.children(0).isInstanceOf[Literal]) {
            (s"countLiteral_${aggregateFunction.children(0)}", 0, 1)
          } else {
            ("count", 1, 1)
          }
        }
        case other => (aggregateFunction.prettyName, 1, 1)
      }
    case Final =>
      aggregateFunction.prettyName match {
        case "count" => ("sum", 1, 1)
        case "avg" => ("avgByCount", 2, 1)
        case other => (aggregateFunction.prettyName, 1, 1)
      }
    case _ =>
      throw new UnsupportedOperationException("doesn't support this mode")
  }

  val finalFuncName = funcName match {
    case "count" => "sum"
    case other => other
  }
  logInfo(s"funcName is $funcName, mode is $mode, argSize is $argSize, resSize is ${resSize}")

  override def requiredColNum: Int = argSize
  override def expectedResColNum: Int = resSize
  override def setInputFields(fieldList: List[Field]): Unit = {
    aggrFieldList = fieldList
  }

  override def doColumnarCodeGen_ext(args: Object): TreeNode = {
    val (keyFieldList, inputFieldList, resultType, resultField) =
      args.asInstanceOf[(List[Field], List[Field], ArrowType, Field)]
    if (keyFieldList.isEmpty != true) {
      // if keyList has keys, we need to do groupby by these keys.
      var inputFieldNode = 
        keyFieldList.map({field => TreeBuilder.makeField(field)}).asJava
      val encodeNode = TreeBuilder.makeFunction(
        "encodeArray",
        inputFieldNode,
        resultType/*this arg won't be used*/)
      inputFieldNode = 
        (encodeNode :: inputFieldList.map({field => TreeBuilder.makeField(field)})).asJava
      val groupByFuncNode = TreeBuilder.makeFunction(
        "splitArrayListWithAction",
        inputFieldNode,
        resultType/*this arg won't be used*/)
      val inputAggrFieldNode = 
        List(groupByFuncNode) ::: aggrFieldList.map(field => TreeBuilder.makeField(field))
      val aggregateFuncName = "action_" + funcName
      logInfo(s"${aggregateFuncName}(${inputAggrFieldNode})")
      TreeBuilder.makeFunction(
        aggregateFuncName,
        inputAggrFieldNode.asJava,
        resultType)
    } else {
      val inputFieldNode = 
        aggrFieldList.map(field => TreeBuilder.makeField(field))
      logInfo(s"${funcName}(${inputFieldNode})")
      TreeBuilder.makeFunction(
        funcName,
        inputFieldNode.asJava,
        resultType)
    }
  }
}
