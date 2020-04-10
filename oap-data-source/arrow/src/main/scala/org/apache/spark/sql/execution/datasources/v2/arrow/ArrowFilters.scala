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

package org.apache.spark.sql.execution.datasources.v2.arrow

import org.apache.arrow.dataset.DatasetTypes
import org.apache.arrow.dataset.DatasetTypes.TreeNode
import org.apache.arrow.dataset.filter.FilterImpl
import org.apache.spark.sql.sources._

object ArrowFilters {

  def translateFilters(pushedFilters: Seq[Filter]): org.apache.arrow.dataset.filter.Filter = {
    val node = pushedFilters
      .flatMap(translateFilter)
      .reduceOption((t1: TreeNode, t2: TreeNode) => {
        DatasetTypes.TreeNode.newBuilder.setAndNode(
          DatasetTypes.AndNode.newBuilder()
            .setLeftArg(t1)
            .setRightArg(t2)
            .build()).build()
      })
    if (node.isDefined) {
      new FilterImpl(DatasetTypes.Condition.newBuilder()
        .setRoot(node.get).build)
    } else {
      org.apache.arrow.dataset.filter.Filter.EMPTY
    }
  }

  private def translateValue(value: Any): TreeNode = {
    value match {
      case v: Integer => DatasetTypes.TreeNode.newBuilder.setIntNode(
        DatasetTypes.IntNode.newBuilder.setValue(v).build)
        .build
      case v: Long => DatasetTypes.TreeNode.newBuilder.setLongNode(
        DatasetTypes.LongNode.newBuilder.setValue(v).build)
        .build
      case v: Float => DatasetTypes.TreeNode.newBuilder.setFloatNode(
        DatasetTypes.FloatNode.newBuilder.setValue(v).build)
        .build
      case v: Double => DatasetTypes.TreeNode.newBuilder.setDoubleNode(
        DatasetTypes.DoubleNode.newBuilder.setValue(v).build)
        .build
      case v: Boolean => DatasetTypes.TreeNode.newBuilder.setBooleanNode(
        DatasetTypes.BooleanNode.newBuilder.setValue(v).build)
        .build
      case _ => throw new UnsupportedOperationException // fixme complete this
    }
  }

  private def translateFilter(pushedFilter: Filter): Option[TreeNode] = {
    pushedFilter match {
      case EqualTo(attribute, value) =>
        createComparisonNode("equal", attribute, value)
      case GreaterThan(attribute, value) =>
        createComparisonNode("greaterThan", attribute, value)
      case GreaterThanOrEqual(attribute, value) =>
        createComparisonNode("greaterThanOrEqual", attribute, value)
      case LessThan(attribute, value) =>
        createComparisonNode("lessThan", attribute, value)
      case LessThanOrEqual(attribute, value) =>
        createComparisonNode("lessThanOrEqual", attribute, value)
      case Not(child) =>
        createNotNode(child)
      case And(left, right) =>
        createAndNode(left, right)
      case Or(left, right) =>
        createOrNode(left, right)
      case IsNotNull(attribute) =>
        createIsNotNullNode(attribute)
      case IsNull(attribute) =>
        createIsNullNode(attribute)
      case _ => None // fixme complete this
    }
  }

  private def createComparisonNode(opName: String,
                                   attribute: String, value: Any): Option[TreeNode] = {
    Some(DatasetTypes.TreeNode.newBuilder.setCpNode(
      DatasetTypes.ComparisonNode.newBuilder
        .setOpName(opName) // todo make op names enumerable
        .setLeftArg(
          DatasetTypes.TreeNode.newBuilder.setFieldNode(
            DatasetTypes.FieldNode.newBuilder.setName(attribute).build)
            .build)
        .setRightArg(translateValue(value))
        .build)
      .build)
  }

  def createNotNode(child: Filter): Option[TreeNode] = {
    val translatedChild = translateFilter(child)
    if (translatedChild.isEmpty) {
      return None
    }
    Some(DatasetTypes.TreeNode.newBuilder
      .setNotNode(DatasetTypes.NotNode.newBuilder.setArgs(translatedChild.get).build()).build())
  }

  def createIsNotNullNode(attribute: String): Option[TreeNode] = {
    Some(DatasetTypes.TreeNode.newBuilder
      .setIsValidNode(
        DatasetTypes.IsValidNode.newBuilder.setArgs(
          DatasetTypes.TreeNode.newBuilder.setFieldNode(
            DatasetTypes.FieldNode.newBuilder.setName(attribute).build)
            .build).build()).build())
  }

  def createIsNullNode(attribute: String): Option[TreeNode] = {
    Some(DatasetTypes.TreeNode.newBuilder
      .setNotNode(
        DatasetTypes.NotNode.newBuilder.setArgs(
          DatasetTypes.TreeNode.newBuilder
            .setIsValidNode(
              DatasetTypes.IsValidNode.newBuilder.setArgs(
                DatasetTypes.TreeNode.newBuilder.setFieldNode(
                  DatasetTypes.FieldNode.newBuilder.setName(attribute).build)
                  .build)
                .build()).build()).build()).build())
  }

  def createAndNode(left: Filter, right: Filter): Option[TreeNode] = {
    val translatedLeft = translateFilter(left)
    val translatedRight = translateFilter(right)
    if (translatedLeft.isEmpty || translatedRight.isEmpty) {
      return None
    }
    Some(DatasetTypes.TreeNode.newBuilder
      .setAndNode(DatasetTypes.AndNode.newBuilder
        .setLeftArg(translatedLeft.get)
        .setRightArg(translatedRight.get)
        .build())
      .build())
  }

  def createOrNode(left: Filter, right: Filter): Option[TreeNode] = {
    val translatedLeft = translateFilter(left)
    val translatedRight = translateFilter(right)
    if (translatedLeft.isEmpty || translatedRight.isEmpty) {
      return None
    }
    Some(DatasetTypes.TreeNode.newBuilder
      .setOrNode(DatasetTypes.OrNode.newBuilder
        .setLeftArg(translatedLeft.get)
        .setRightArg(translatedRight.get)
        .build())
      .build())
  }
}
