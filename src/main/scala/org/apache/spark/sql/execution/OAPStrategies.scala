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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{expressions, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.DataSourceScanExec.{INPUT_PATHS, PUSHED_FILTERS}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.Strategy
import org.apache.spark.util.Utils

trait OAPStrategies {

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
   * TODO: add more use scenarios in future.
   */
  object SortPushDownStrategy extends Strategy with Logging {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ReturnAnswer(rootPlan) => rootPlan match {
        case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
          val childPlan = calcChildPlan(child, limit, order)
          execution.TakeOrderedAndProjectExec(limit, order, child.output, childPlan) :: Nil
        case logical.Limit(
            IntegerLiteral(limit),
            logical.Project(projectList, logical.Sort(order, true, child))) =>
          val childPlan = calcChildPlan(child, limit, order)
          execution.TakeOrderedAndProjectExec(limit, order, projectList, childPlan) :: Nil
        case _ =>
          Nil
      }
      case _ => Nil
    }

    def calcChildPlan(child : LogicalPlan, limit : Int, order : Seq[SortOrder])
                      : SparkPlan = child match {
      case PhysicalOperation(
            projectList, filters, relation@LogicalRelation(file: HadoopFsRelation, _, table)) =>
        val filterAttributes = AttributeSet(ExpressionSet(filters))
        val orderAttributes = AttributeSet(ExpressionSet(order.map(_.child)))
        if ((orderAttributes.size == 1 &&
            (filterAttributes.isEmpty || filterAttributes == orderAttributes)) &&
            (file.fileFormat.isInstanceOf[OapFileFormat] &&
              file.fileFormat.initialize(file.sparkSession, file.options, file.location)
              .asInstanceOf[OapFileFormat].hasAvailableIndex(orderAttributes))) {
          PushDownSortToOAPFileScanExec(limit, order, projectList,
            TakeOrderedOAPFileScan(projectList, filters, relation, file, table, limit, order.head))
        }
        else PlanLater(child)
      case _ => PlanLater(child)
    }


    /**
     * Pretty much like FileSourceStrategy.apply() as the only difference is the sort
     * and limit config were pushed down to FileScanRDD's reader function.
     * TODO: remove OAP irrelevant code.
     */
    def TakeOrderedOAPFileScan(
                                projects: Seq[NamedExpression], filters: Seq[Expression],
                                l: LogicalPlan, files : HadoopFsRelation,
                                table: Option[TableIdentifier],
                                limit : Int, order : SortOrder): SparkPlan = {
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

        val partitionColumns =
          l.resolve(files.partitionSchema, files.sparkSession.sessionState.analyzer.resolver)
        val partitionSet = AttributeSet(partitionColumns)
        val partitionKeyFilters =
          ExpressionSet(normalizedFilters.filter(_.references.subsetOf(partitionSet)))
        logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

        val dataColumns =
          l.resolve(files.dataSchema, files.sparkSession.sessionState.analyzer.resolver)

        // Partition keys are not available in the statistics of the files.
        val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

        // Predicates with both partition keys and attributes need to be evaluated after the scan.
        val afterScanFilters = filterSet -- partitionKeyFilters
        logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

        val selectedPartitions = files.location.listFiles(partitionKeyFilters.toSeq)

        val filterAttributes = AttributeSet(afterScanFilters)
        val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
        val requiredAttributes = AttributeSet(requiredExpressions)

        val readDataColumns =
          dataColumns
            .filter(requiredAttributes.contains)
            .filterNot(partitionColumns.contains)
        val prunedDataSchema = readDataColumns.toStructType
        logInfo(s"Pruned Data Schema: ${prunedDataSchema.simpleString(5)}")

        val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
        logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

        val readFile = files.fileFormat.asInstanceOf[OapFileFormat].buildReaderWithPartitionValues(
          sparkSession = files.sparkSession,
          dataSchema = files.dataSchema,
          partitionSchema = files.partitionSchema,
          requiredSchema = prunedDataSchema,
          filters = pushedDownFilters,
          order.isAscending,
          limit,
          options = files.options,
          hadoopConf = files.sparkSession.sessionState.newHadoopConfWithOptions(files.options))
        logInfo(s"Pushed Order By $order, Limit $limit.")

        val plannedPartitions = {
          val defaultMaxSplitBytes = files.sparkSession.sessionState.conf.filesMaxPartitionBytes
          val openCostInBytes = files.sparkSession.sessionState.conf.filesOpenCostInBytes
          val defaultParallelism = files.sparkSession.sparkContext.defaultParallelism
          val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
          val bytesPerCore = totalBytes / defaultParallelism
          val maxSplitBytes = Math.min(defaultMaxSplitBytes,
            Math.max(openCostInBytes, bytesPerCore))
          logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
            s"open cost is considered as scanning $openCostInBytes bytes.")

          val splitFiles = selectedPartitions.flatMap { partition =>
            partition.files.flatMap { file =>
              assert(!files.fileFormat.isSplitable(files.sparkSession, files.options, file.getPath))
              val blockLocations = OAPFileUtils.getBlockLocations(file)
              val hosts = OAPFileUtils.getBlockHosts(blockLocations, 0, file.getLen)
              Seq(PartitionedFile(
                partition.values, file.getPath.toUri.toString, 0, file.getLen, hosts))
            }
          }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

          val partitions = new ArrayBuffer[FilePartition]
          val currentFiles = new ArrayBuffer[PartitionedFile]
          var currentSize = 0L

          /** Add the given file to the current partition. */
          def addFile(file: PartitionedFile): Unit = {
            currentSize += file.length + openCostInBytes
            currentFiles.append(file)
          }

          /** Close the current partition and move to the next. */
          def closePartition(): Unit = {
            if (currentFiles.nonEmpty) {
              val newPartition =
                FilePartition(
                  partitions.size,
                  currentFiles.toArray.toSeq) // Copy to a new Array.
              partitions.append(newPartition)
            }
            currentFiles.clear()
            currentSize = 0
          }

          // Assign files to partitions using "First Fit Decreasing" (FFD)
          // TODO: consider adding a slop factor here?
          splitFiles.foreach { file =>
            if (currentSize + file.length > maxSplitBytes) {
              closePartition()
            }
            addFile(file)
          }
          closePartition()
          partitions
        }

        // These metadata values make scan plans uniquely identifiable for equality checking.
        val meta = Map(
          "PartitionFilters" -> partitionKeyFilters.mkString("[", ", ", "]"),
          "Format" -> files.fileFormat.toString,
          "ReadSchema" -> prunedDataSchema.simpleString,
          PUSHED_FILTERS -> pushedDownFilters.mkString("[", ", ", "]"),
          INPUT_PATHS -> files.location.paths.mkString(", "))

        val scan =
          DataSourceScanExec.create(
            readDataColumns ++ partitionColumns,
            new FileScanRDD(
              files.sparkSession,
              readFile,
              plannedPartitions),
            files,
            meta,
            table)

        val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
        val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
        val withProjections = if (projects == withFilter.output) {
          withFilter
        } else {
          execution.ProjectExec(projects, withFilter)
        }

        withProjections
    }
    // TODO: Add more OAP specific strategies
  }

  /**
   * A Util class to handle file block (hadoop/hdfs file) related information.
   */
  object OAPFileUtils {

    private[OAPStrategies] def getBlockLocations(file: FileStatus)
                                : Array[BlockLocation] = file match {
      case f: LocatedFileStatus => f.getBlockLocations
      case f => Array.empty[BlockLocation]
    }

    // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
    // pair that represents a segment of the same file, find out the block that contains the largest
    // fraction the segment, and returns location hosts of that block.
    // If no such block can be found, returns an empty array.
    private[OAPStrategies] def getBlockHosts(
                                              blockLocations: Array[BlockLocation],
                                              offset: Long, length: Long): Array[String] = {
      val candidates = blockLocations.map {
        // The fragment starts from a position within this block
        case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
          b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

        // The fragment ends at a position within this block
        case b if offset <= b.getOffset && offset + length < b.getLength =>
          b.getHosts -> (offset + length - b.getOffset).min(length)

        // The fragment fully contains this block
        case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
          b.getHosts -> b.getLength

        // The fragment doesn't intersect with this block
        case b =>
          b.getHosts -> 0L
      }.filter { case (hosts, size) =>
        size > 0L
      }

      if (candidates.isEmpty) {
        Array.empty[String]
      } else {
        val (hosts, _) = candidates.maxBy { case (_, size) => size }
        hosts
      }
    }
  }
}

/**
 * A simple wrapper Exec to mark push down execution, which
 * can be easily viewed by sql explain.
 */
case class PushDownSortToOAPFileScanExec(
                                          limit: Int,
                                          sortOrder: Seq[SortOrder],
                                          projectList: Seq[NamedExpression],
                                          child: SparkPlan) extends UnaryExecNode {

  /**
   * Here we can not tell its order because rows in each partition may not in order
   * because they could come from different files, which is because spark combines
   * files reading sometimes (depends on various conditions) for better performance.
   * Refer to fileScanRDD for the file reading combination.
   */
  override def outputOrdering: Seq[SortOrder] = Nil

  override def simpleString: String = {
    val orderByString = Utils.truncatedString(sortOrder, "[", ",", "]")
    val outputString = Utils.truncatedString(output, "[", ",", "]")

    s"PushDownSortToOAPFileScanExec(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }

  override protected def doExecute(): RDD[InternalRow] = child.execute()

  override def output: Seq[Attribute] = child.output
}
