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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, ExpressionSet, IntegerLiteral}
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat

private[sql] trait OAPStrategies {

  object SortPushDownStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ReturnAnswer(rootPlan) => rootPlan match {
        case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
          child match {
            case PhysicalOperation(projectList, filters,
            LogicalRelation(file: HadoopFsRelation, _, _)) =>
              val filterAttributes = AttributeSet(ExpressionSet(filters))
              val orderAttributes = AttributeSet(ExpressionSet(order.map{_.child}))
              /* Prerequisite: File format is OAP & has index on a specific column A.
               * Now support:
               *  1. filter + order by w/ limit
               *      SELECT x FROM xx WHERE Column-A filter ORDER BY Column-A LIMIT N
               *  2. order by w/ limit Only
               *      SELECT x FROM xx ORDER BY Column-A LIMIT N
               * Future support:
               *  1. query without limit
               *  2. multi filters + order by w/ limit
               *      SELECT x FROM xx WHERE Column-(A, B, C, etc) filter ORDER BY Column-A LIMIT N
               */
              if ((file.fileFormat.isInstanceOf[OapFileFormat] &&
                  file.fileFormat.initialize(file.sparkSession, file.options, file.location)
                  .asInstanceOf[OapFileFormat].hasAvailableIndex(orderAttributes)) &&
                  (orderAttributes.size == 1 &&
                   (filterAttributes.isEmpty || filterAttributes == orderAttributes))) {
                  SortPushDownAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
              } else Nil
            case _ =>
              Nil
          }
        case other =>
          planLater(other) :: Nil
      }
      case _ => Nil
    }
  }

  // TODO: Add more OAP specific strategies
}
