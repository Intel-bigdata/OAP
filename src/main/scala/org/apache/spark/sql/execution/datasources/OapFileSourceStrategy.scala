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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{execution, Strategy}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.internal.oap.OapConf

/**
 * OapFileSourceStrategy use to intercept [[FileSourceStrategy]]
 */
object OapFileSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {

    /**
     * After call [[FileSourceStrategy]], the result [[SparkPlan]] should the follow 4 scenarios:
     *  1. [[ProjectExec]] -> [[FilterExec]] -> [[FileSourceScanExec]]
     *  2. [[ProjectExec]] -> [[FileSourceScanExec]]
     *  3. [[FilterExec]] -> [[FileSourceScanExec]]
     *  4. [[FileSourceScanExec]]
     * Classified discussion the 4 scenarios and assemble a new [[SparkPlan]] if can optimized.
     */
    def tryOptimize(head: SparkPlan): SparkPlan = {
      head match {
        // ProjectExec -> FilterExec -> FileSourceScanExec
        case ProjectExec(projectList, FilterExec(condition,
          FileSourceScanExec(relation, output, outputSchema, partitionFilters,
          dataFilters, tableIdentifier))) =>

          val tableEnbale =
            relation.sparkSession.conf.get(OapConf.OAP_CACHE_TABLE_LISTS_ENABLE)
          val cacheTablelists: String =
            relation.sparkSession.conf.get(OapConf.OAP_CACHE_TABLE_LISTS)
          // scalastyle:off println
          logInfo("tableIdentifier:" + tableIdentifier)
          logInfo("tableEnbale:" + tableEnbale)
          var canCache = true
          if(tableEnbale) {
            canCache = false
            tableIdentifier match {
              case Some(table) => logInfo("tableIdentifier.unquotedString:" + table.unquotedString)
                if (cacheTablelists.contains(table.unquotedString)) {
                  logInfo(" contains")
                  canCache = true
                }
                else logInfo("cacheTablelists no contains")
              case None => logInfo("Relation has no table")
            }
          }

          val (hadoopFsRelation, isOptimized) = HadoopFsRelationOptimizer.tryOptimize(
            relation, partitionFilters, dataFilters, outputSchema)
          if (isOptimized && canCache) {
            val scan = FileSourceScanExec(hadoopFsRelation, output, outputSchema,
              partitionFilters, dataFilters, tableIdentifier)
            execution.ProjectExec(projectList, execution.FilterExec(condition, scan))
          } else {
            head
          }
        // ProjectExec -> FileSourceScanExec
        case ProjectExec(projectList,
          FileSourceScanExec(relation, output, outputSchema, partitionFilters,
          dataFilters, tableIdentifier)) =>

          val tableEnbale =
            relation.sparkSession.conf.get(OapConf.OAP_CACHE_TABLE_LISTS_ENABLE)
          val cacheTablelists: String =
            relation.sparkSession.conf.get(OapConf.OAP_CACHE_TABLE_LISTS)
          // scalastyle:off println
          logInfo("tableIdentifier:" + tableIdentifier)
          logInfo("tableEnbale:" + tableEnbale)
          var canCache = true
          if(tableEnbale) {
            canCache = false
            tableIdentifier match {
              case Some(table) => logInfo("tableIdentifier.unquotedString:" + table.unquotedString)
                if (cacheTablelists.contains(table.unquotedString)) {
                  logInfo(" contains")
                  canCache = true
                }
                else logInfo("cacheTablelists no contains")
              case None => logInfo("Relation has no table")
            }
          }

          val (hadoopFsRelation, isOptimized) = HadoopFsRelationOptimizer.tryOptimize(
            relation, partitionFilters, dataFilters, outputSchema)
          if (isOptimized && canCache) {
            val scan = FileSourceScanExec(hadoopFsRelation, output, outputSchema,
              partitionFilters, dataFilters, tableIdentifier)
            execution.ProjectExec(projectList, scan)
          } else {
            head
          }
        // FilterExec -> FileSourceScanExec
        case FilterExec(condition, FileSourceScanExec(relation, output, outputSchema,
          partitionFilters, dataFilters, tableIdentifier)) =>

          val tableEnbale =
            relation.sparkSession.conf.get(OapConf.OAP_CACHE_TABLE_LISTS_ENABLE)
          val cacheTablelists: String =
            relation.sparkSession.conf.get(OapConf.OAP_CACHE_TABLE_LISTS)
          // scalastyle:off println
          logInfo("tableIdentifier:" + tableIdentifier)
          logInfo("tableEnbale:" + tableEnbale)
          var canCache = true
          if(tableEnbale) {
            canCache = false
            tableIdentifier match {
              case Some(table) => logInfo("tableIdentifier.unquotedString:" + table.unquotedString)
                if (cacheTablelists.contains(table.unquotedString)) {
                  logInfo(" contains")
                  canCache = true
                }
                else logInfo("cacheTablelists no contains")
              case None => logInfo("Relation has no table")
            }
          }

          val (hadoopFsRelation, isOptimized) = HadoopFsRelationOptimizer.tryOptimize(
            relation, partitionFilters, dataFilters, outputSchema)
          if (isOptimized && canCache) {
            val scan = FileSourceScanExec(hadoopFsRelation, output, outputSchema,
              partitionFilters, dataFilters, tableIdentifier)
            execution.FilterExec(condition, scan)
          } else {
            head
          }
        // FileSourceScanExec
        case FileSourceScanExec(relation, output, outputSchema, partitionFilters,
          dataFilters, tableIdentifier) =>

          val tableEnbale =
            relation.sparkSession.conf.get(OapConf.OAP_CACHE_TABLE_LISTS_ENABLE)
          val cacheTablelists: String =
            relation.sparkSession.conf.get(OapConf.OAP_CACHE_TABLE_LISTS)
          // scalastyle:off println
          logInfo("tableIdentifier:" + tableIdentifier)
          logInfo("tableEnbale:" + tableEnbale)
          var canCache = true
          if(tableEnbale) {
            canCache = false
            tableIdentifier match {
              case Some(table) => logInfo("tableIdentifier.unquotedString:" + table.unquotedString)
                if (cacheTablelists.contains(table.unquotedString)) {
                  logInfo(" contains")
                  canCache = true
                }
                else logInfo("cacheTablelists no contains")
              case None => logInfo("Relation has no table")
            }
          }

          val (hadoopFsRelation, isOptimized) = HadoopFsRelationOptimizer.tryOptimize(
            relation, partitionFilters, dataFilters, outputSchema)
          if (isOptimized && canCache) {
            FileSourceScanExec(hadoopFsRelation, output, outputSchema,
              partitionFilters, dataFilters, tableIdentifier)
          } else {
            head
          }
        case _ => throw new OapException(s"Unsupport plan mode $head")
      }
    }

    plan match {
      case PhysicalOperation(_, _, LogicalRelation(_: HadoopFsRelation, _, _, _)) =>
        FileSourceStrategy(plan).headOption match {
          case Some(head) => tryOptimize(head) :: Nil
          case _ => Nil
        }
      case _ => Nil
    }
  }
}
