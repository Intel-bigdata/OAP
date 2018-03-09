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

package org.apache.spark.sql.execution.datasources.oap

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{execution, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.{logical, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.OapAggUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.joins.BuildRight
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.util.Utils

trait OapStrategies extends Logging {

  def oapStrategies: Seq[Strategy] = {
    // BtreeIndex applicable strategies.
    OapSortLimitStrategy ::
    // BitMapIndex applicable strategies.
    OapSemiJoinStrategy ::
    // No requirement.
    OapGroupAggregateStrategy :: Nil
  }

  /**
   * Plans special cases of orderby+limit operators.
   * If OAP database already has index on a specific column, we
   * can push this sort and limit condition down to file scan RDD,
   * i.e. before this strategy applies, the child (could be a deep
   * child) the FileScanRDD gives full ROW scan and do lots sort and
   * limit in upper tree level. But after it applies, FileScanRDD
   * gives sorted (because of OAP index) and limited ROWs to upper
   * level, and then do only few sort and limit operation.
   *
   * Limitations:
   * Only 2 use scenarios so far.
   *   1.filter + order by with limit on same/single column
   *     SELECT x FROM xx WHERE filter(A) ORDER BY Column-A LIMIT N
   *   2. order by a single column with limit Only
   *     SELECT x FROM xx ORDER BY Column-A LIMIT N
   *
   * TODO:
   *   1. add more use scenarios in future.
   *   2. abstract common implementation between oap strategies.
   */
  object OapSortLimitStrategy extends Strategy with Logging {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ReturnAnswer(rootPlan) => rootPlan match {
        case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
          val childPlan = calcChildPlan(child, limit, order)
          TakeOrderedAndProjectExec(limit, order, child.output, childPlan) :: Nil
        case logical.Limit(
            IntegerLiteral(limit),
            logical.Project(projectList, logical.Sort(order, true, child))) =>
          val childPlan = calcChildPlan(child, limit, order)
          TakeOrderedAndProjectExec(limit, order, projectList, childPlan) :: Nil
        case _ =>
          Nil
      }
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
        val childPlan = calcChildPlan(child, limit, order)
        TakeOrderedAndProjectExec(limit, order, child.output, childPlan) :: Nil
      case logical.Limit(
          IntegerLiteral(limit),
          logical.Project(projectList, logical.Sort(order, true, child))) =>
        val childPlan = calcChildPlan(child, limit, order)
        TakeOrderedAndProjectExec(limit, order, projectList, childPlan) :: Nil
      case _ => Nil
    }

    def calcChildPlan(
        child: LogicalPlan,
        limit: Int,
        order: Seq[SortOrder]): SparkPlan = child match {
      case PhysicalOperation(projectList, filters,
        relation @ LogicalRelation(
          file @ HadoopFsRelation(_, _, _, _, _ : OapFileFormat, _), _, table)) =>
        val filterAttributes = AttributeSet(ExpressionSet(filters))
        val orderAttributes = AttributeSet(ExpressionSet(order.map(_.child)))
        if (orderAttributes.size == 1 && filterAttributes == orderAttributes) {
          val oapOption = new CaseInsensitiveMap(file.options +
            (OapFileFormat.OAP_QUERY_LIMIT_OPTION_KEY -> limit.toString) +
            (OapFileFormat.OAP_QUERY_ORDER_OPTION_KEY -> order.head.isAscending.toString))
          val indexRequirement = filters.map(_ => BTreeIndex())

          createOapFileScanPlan(
            projectList,
            filters,
            relation,
            file,
            table,
            oapOption,
            filters,
            indexRequirement) match {
            case Some(fastScan) => OapOrderLimitFileScanExec(limit, order, projectList, fastScan)
            case None => PlanLater(child)
          }
        } else {
          PlanLater(child)
        }
      case _ => PlanLater(child)
    }
  }

  // TODO: Add more OAP specific strategies
  /**
   * OAPSemiJoinStrategy optimizes SemiJoin.
   * SemiJoin can take assumption that each value in right
   * table is distinct, so we can take advantage of OAP index
   * that we tell index scan to return only 1 item from each
   * index entry.
   *
   * Limitation:
   * 1. Query & Filter column must have index.
   * TODO: choose any index if no filter.
   *
   */
  object OapSemiJoinStrategy extends Strategy with Logging {
    private def canBroadcast(plan: LogicalPlan): Boolean = {
      val conf = SparkSession.getActiveSession.get.sessionState.conf
      // We can take a much larger threshold here since if this optimization
      // is applicable, only distinct item will be broadcasted, those data
      // should much less than the origin table.
      plan.statistics.isBroadcastable ||
        plan.statistics.sizeInBytes <= conf.autoBroadcastJoinThreshold
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if joinType == LeftSemi && canBroadcast(right) =>
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left),
          calcChildPlan(right, rightKeys.map(SortOrder(_, Ascending)))))
      case _ => Nil
    }

    def calcChildPlan(
        child: LogicalPlan,
        order: Seq[SortOrder]): SparkPlan = child match {
      case PhysicalOperation(projectList, filters,
        relation @ LogicalRelation(
          file @ HadoopFsRelation(_, _, _, _, _ : OapFileFormat, _), _, table)) =>
        val filterAttributes = AttributeSet(ExpressionSet(filters))
        val orderAttributes = AttributeSet(ExpressionSet(order.map(_.child)))
        if (orderAttributes.size == 1 || filterAttributes == orderAttributes) {
          val oapOption = new CaseInsensitiveMap(file.options +
            (OapFileFormat.OAP_INDEX_SCAN_NUM_OPTION_KEY -> "1"))
          val indexRequirement = filters.map(_ => BitMapIndex())

          createOapFileScanPlan(
            projectList,
            filters,
            relation,
            file,
            table,
            oapOption,
            filters,
            indexRequirement) match {
            case Some(fastScan) => OapDistinctFileScanExec(scanNumber = 1, projectList, fastScan)
            case None => PlanLater(child)
          }
        } else {
          PlanLater(child)
        }
      case _ => PlanLater(child)
    }
  }

  /**
   * Optimized Aggregations w/ GROUP BY as a OAP Strategies.
   * Now the workable case is:
   *   SELECT [agg](columns)
   *   FROM table
   *   [WHERE filter on columns]
   *   GROUP BY one specific column
   */
  object OapGroupAggregateStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalAggregation(
      groupingExpressions, aggregateExpressions, resultExpressions, child) =>
        val (functionsWithDistinct, _) =
          aggregateExpressions.partition(_.isDistinct)
        if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our MultipleDistinctRewriter should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
            "Spark user mailing list.")
        }

        val aggregateOperator =
          if (aggregateExpressions.map(_.aggregateFunction).exists(!_.supportsPartial)) {
            if (functionsWithDistinct.nonEmpty) {
              sys.error("Distinct columns cannot exist in Aggregate operator containing " +
                "aggregate functions which don't support partial aggregation.")
            } else {
              // So far do not support non-partial aggregations.
              Nil
            }
          } else if (functionsWithDistinct.isEmpty) {
            // Support single group by only so far
            if (groupingExpressions.size == 1) {
              OapAggUtils.planAggregateWithoutDistinct(
                groupingExpressions,
                aggregateExpressions,
                resultExpressions,
                calcChildPlan(
                  groupingExpressions, aggregateExpressions, resultExpressions, child))
            } else Nil
          } else {
            // TODO: support distinct in future.
            Nil
          }
        aggregateOperator
      case _ => Nil
    }

    private def calcChildPlan(
        groupExpressions: Seq[NamedExpression],
        aggExpressions: Seq[AggregateExpression],
        resultExpressions: Seq[NamedExpression],
        child : LogicalPlan
    ) : SparkPlan = child match {
      case PhysicalOperation(projectList, filters,
        relation @ LogicalRelation(
          file @ HadoopFsRelation(_, _, _, _, _ : OapFileFormat, _), _, table)) =>
        val filterAttributes = AttributeSet(ExpressionSet(filters))
        val groupingAttributes = AttributeSet(groupExpressions.map(_.toAttribute))

        if (groupingAttributes.size == 1 && filterAttributes == groupingAttributes) {
          val oapOption = new CaseInsensitiveMap(file.options +
            (OapFileFormat.OAP_INDEX_GROUP_BY_OPTION_KEY -> "true"))

          createOapFileScanPlan(
            projectList, filters, relation, file, table, oapOption, filters, Nil) match {
            case Some(fastScan) => OapAggregationFileScanExec(aggExpressions, projectList, fastScan)
            case _ => PlanLater(child)
          }
        } else {
          PlanLater(child)
        }
      case _ => PlanLater(child)
    }
  }

  /**
   * Pretty much like FileSourceStrategy.apply() as the only difference is the sort
   * and limit config were pushed down to FileScanRDD's reader function.
   * TODO: remove OAP irrelevant code.
   */
  def createOapFileScanPlan(
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      l: LogicalPlan,
      _fsRelation: HadoopFsRelation,
      table: Option[CatalogTable],
      oapOption: Map[String, String],
      indexHint: Seq[Expression],
      indexRequirements: Seq[IndexType]): Option[SparkPlan] = {
    // If executor index selection (EIS) is enabled, oapStrategies are disabled.
    val conf = SparkSession.getActiveSession.get.sessionState.conf
    if (conf.getConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION)) {
      return None
    }

    // Filters on this relation fall into four categories based
    // on where we can use them to avoid
    // reading unneeded data:
    //  - partition keys only - used to prune directories to read
    //  - bucket keys only - optionally used to prune files to read
    //  - keys stored in the data only - optionally used to skip groups of data in files
    //  - filters that need to be evaluated again after the scan
    val filterSet = ExpressionSet(filters)

    // The attribute name of predicate could be different than the one in schema in case of
    // case insensitive, we should change them to match the one in schema, so we donot need to
    // worry about case sensitivity anymore.
    val normalizedFilters = filters.map { e =>
      e transform {
        case a: AttributeReference =>
          a.withName(l.output.find(_.semanticEquals(a)).get.name)
      }
    }

    val normalizedIndexHint = indexHint.map { e =>
      e transform {
        case a: AttributeReference =>
          a.withName(l.output.find(_.semanticEquals(a)).get.name)
      }
    }

    val partitionColumns =
      l.resolve(
        _fsRelation.partitionSchema, _fsRelation.sparkSession.sessionState.analyzer.resolver)
    val partitionSet = AttributeSet(partitionColumns)
    val partitionKeyFilters =
      ExpressionSet(normalizedFilters.filter(_.references.subsetOf(partitionSet)))
    logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

    val selectedPartitions = _fsRelation.location.listFiles(partitionKeyFilters.toSeq)

    _fsRelation.fileFormat match {
      case fileFormat: OapFileFormat =>
        fileFormat.initialize(_fsRelation.sparkSession, oapOption,
          selectedPartitions.flatMap(p => p.files))

        if (fileFormat.hasAvailableIndex(normalizedIndexHint, indexRequirements)) {
          val dataColumns = l.resolve(
              _fsRelation.dataSchema, _fsRelation.sparkSession.sessionState.analyzer.resolver)

          // Partition keys are not available in the statistics of the files.
          val dataFilters = normalizedIndexHint.filter(_.references.intersect(partitionSet).isEmpty)

          // Predicates with both partition keys and attributes need to be evaluated after the scan.
          val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
          logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

          val filterAttributes = AttributeSet(afterScanFilters)
          val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
          val requiredAttributes = AttributeSet(requiredExpressions)

          val readDataColumns =
            dataColumns
              .filter(requiredAttributes.contains)
              .filterNot(partitionColumns.contains)
          val outputSchema = readDataColumns.toStructType
          logInfo(s"Output Data Schema: ${outputSchema.simpleString(5)}")

          val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
          logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

          val outputAttributes = readDataColumns ++ partitionColumns

          val oapRelation = _fsRelation.copy(options = oapOption)(_fsRelation.sparkSession)
          val scan =
            new FileSourceScanExec(
              oapRelation,
              outputAttributes,
              outputSchema,
              partitionKeyFilters.toSeq,
              pushedDownFilters,
              table.map(_.identifier))

          val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
          val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
          val withProjections = if (projects == withFilter.output) {
            withFilter
          } else {
            execution.ProjectExec(projects, withFilter)
          }

          Option(withProjections)
        } else {
          None
        }
      case _ => None
    }
  }
}

/**
 * A simple wrapper SparkPlan exec base class to mark OAP related execution,
 * which can be easily viewed by sql explain.
 */
abstract class OapFileScanExec extends UnaryExecNode with CodegenSupport {
  /**
   * Here we can not tell its order because rows in each partition may not in order
   * because they could come from different files, which is because spark combines
   * files reading sometimes (depends on various conditions) for better performance.
   * Refer to fileScanRDD for the file reading combination.
   */
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def doExecute(): RDD[InternalRow] = child.execute()

  override def output: Seq[Attribute] = child.output

  def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(
      ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       ${consume(ctx, input)}
     """.stripMargin
  }
}

case class OapOrderLimitFileScanExec(
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan) extends OapFileScanExec {

  override def simpleString: String = {
    val orderByString = Utils.truncatedString(sortOrder, "[", ",", "]")
    val outputString = Utils.truncatedString(output, "[", ",", "]")

    s"OapOrderLimitFileScanExec(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }
}

case class OapDistinctFileScanExec(
    scanNumber: Int,
    projectList: Seq[NamedExpression],
    child: SparkPlan) extends OapFileScanExec {

  override def simpleString: String = {
    val outputString = Utils.truncatedString(output, "[", ",", "]")

    s"OapDistinctFileScanExec(output=$outputString, scan [$scanNumber] row of each index)"
  }
}

case class OapAggregationFileScanExec(
    aggExpression: Seq[AggregateExpression],
    projectList: Seq[NamedExpression],
    child: SparkPlan) extends OapFileScanExec {

  override def simpleString: String = {
    val outputString = Utils.truncatedString(output, "[", ",", "]")
    val aggString = Utils.truncatedString(aggExpression, "[", ",", "]")

    s"OapAggregationFileScanExec(output=$outputString, Aggregation function=$aggString)"
  }
}
