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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Final, Partial}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.oap.OapAggregationFileScanExec

object OapAggUtils {
  private def createAggregate(
      requiredChildDistributionExpressions: Option[Seq[Expression]] = None,
      groupingExpressions: Seq[NamedExpression] = Nil,
      aggregateExpressions: Seq[AggregateExpression] = Nil,
      aggregateAttributes: Seq[Attribute] = Nil,
      initialInputBufferOffset: Int = 0,
      resultExpressions: Seq[NamedExpression] = Nil,
      child: SparkPlan): SparkPlan = {
    if (requiredChildDistributionExpressions.isDefined) {
      // final aggregate, fall back to Spark HashAggregateExec.
      HashAggregateExec(
        requiredChildDistributionExpressions = requiredChildDistributionExpressions,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child)
    } else {
      // Apply partial aggregate optimizations.
      OapAggregateExec(
        requiredChildDistributionExpressions = None,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child)
    }
  }

  def planAggregateWithoutDistinct(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {
    val useHash = HashAggregateExec.supportsAggregate(
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))

    if (!child.isInstanceOf[OapAggregationFileScanExec] || !useHash) {
      // Child can not leverage oap optimization reading.
      Nil
    } else {
      // 1. Create an Aggregate Operator for partial aggregations.
      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val partialAggregateExpressions = aggregateExpressions.map(_.copy(mode = Partial))
      val partialAggregateAttributes =
        partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
      val partialResultExpressions =
        groupingAttributes ++
          partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

      val partialAggregate = createAggregate(
        requiredChildDistributionExpressions = None,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = partialAggregateExpressions,
        aggregateAttributes = partialAggregateAttributes,
        initialInputBufferOffset = 0,
        resultExpressions = partialResultExpressions,
        child = child)

      // 2. Create an Aggregate Operator for final aggregations.
      val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

      val finalAggregate = createAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes,
        initialInputBufferOffset = groupingExpressions.length,
        resultExpressions = resultExpressions,
        child = partialAggregate)

      finalAggregate :: Nil
    }
  }
}
