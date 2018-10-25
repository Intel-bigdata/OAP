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
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf

/**
 * A strategy for planning scans over collections of files that might be partitioned or bucketed
 * by user specified columns.
 *
 * At a high level planning occurs in several phases:
 *  - Split filters by when they need to be evaluated.
 *  - Prune the schema of the data requested based on any projections present. Today this pruning
 *    is only done on top level columns, but formats should support pruning of nested columns as
 *    well.
 *  - Construct a reader function by passing filters and the schema into the FileFormat.
 *  - Using a partition pruning predicates, enumerate the list of files that should be read.
 *  - Split the files into tasks and construct a FileScanRDD.
 *  - Add any projection or filters that must be evaluated after the scan.
 *
 * Files are assigned into tasks using the following algorithm:
 *  - If the table is bucketed, group files by bucket id into the correct number of partitions.
 *  - If the table is not bucketed or bucketing is turned off:
 *   - If any file is larger than the threshold, split it into pieces based on that threshold
 *   - Sort the files by decreasing file size.
 *   - Assign the ordered files to buckets using the following algorithm.  If the current partition
 *     is under the threshold with the addition of the next file, add it.  If not, open a new bucket
 *     and add it.  Proceed to the next file.
 */
object FileSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters,
      l @ LogicalRelation(_fsRelation: HadoopFsRelation, _, table, _)) =>
      // Filters on this relation fall into four categories based on where we can use them to avoid
      // reading unneeded data:
      //  - partition keys only - used to prune directories to read
      //  - bucket keys only - optionally used to prune files to read
      //  - keys stored in the data only - optionally used to skip groups of data in files
      //  - filters that need to be evaluated again after the scan
      val filterSet = ExpressionSet(filters)

      // The attribute name of predicate could be different than the one in schema in case of
      // case insensitive, we should change them to match the one in schema, so we do not need to
      // worry about case sensitivity anymore.
      val normalizedFilters = filters.map { e =>
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

      val selectedPartitions = _fsRelation.location.listFiles(partitionKeyFilters.toSeq, Nil)

      val fsRelation: HadoopFsRelation = _fsRelation.fileFormat match {
        // TODO a better rule to check if we need to substitute the ParquetFileFormat
        // as OapFileFormat
        // add spark.sql.oap.parquet.enable config
        // if config true turn to OapFileFormat
        // else turn to ParquetFileFormat
        case a: ParquetFileFormat
          if _fsRelation.sparkSession.conf.get(OapConf.OAP_PARQUET_ENABLED) =>
          val oapFileFormat = new OapFileFormat
          oapFileFormat
            .init(_fsRelation.sparkSession,
              _fsRelation.options,
              selectedPartitions.flatMap(p => p.files))

          if (oapFileFormat.hasAvailableIndex(normalizedFilters)) {
            logInfo("hasAvailableIndex = true, will replace with OapFileFormat.")
            val parquetOptions: Map[String, String] =
              Map(SQLConf.PARQUET_BINARY_AS_STRING.key ->
                _fsRelation.sparkSession.sessionState.conf.isParquetBinaryAsString.toString,
                SQLConf.PARQUET_INT96_AS_TIMESTAMP.key ->
                  _fsRelation.sparkSession.sessionState.conf.isParquetINT96AsTimestamp.toString,
                SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key ->
                  _fsRelation.sparkSession.sessionState.conf.writeLegacyParquetFormat.toString,
                SQLConf.PARQUET_INT64_AS_TIMESTAMP_MILLIS.key ->
                  _fsRelation.sparkSession.sessionState.conf
                    .isParquetINT64AsTimestampMillis.toString) ++
                _fsRelation.options

            _fsRelation.copy(fileFormat = oapFileFormat,
              options = parquetOptions)(_fsRelation.sparkSession)

          } else {
            logInfo("hasAvailableIndex = false, will retain ParquetFileFormat.")
            _fsRelation
          }

        case a if (_fsRelation.sparkSession.conf.get(OapConf.OAP_ORC_ENABLED) &&
          (a.isInstanceOf[org.apache.spark.sql.hive.orc.OrcFileFormat] ||
            a.isInstanceOf[org.apache.spark.sql.execution.datasources.orc.OrcFileFormat])) =>
          val oapFileFormat = new OapFileFormat
          oapFileFormat
            .init(_fsRelation.sparkSession,
              _fsRelation.options,
              selectedPartitions.flatMap(p => p.files))

          if (oapFileFormat.hasAvailableIndex(normalizedFilters)) {
            logInfo("hasAvailableIndex = true, will replace with OapFileFormat.")
            // isOapOrcFileFormat is used to indicate to read orc data with oap index accelerated.
            val orcOptions: Map[String, String] =
              Map(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key ->
                _fsRelation.sparkSession.sessionState.conf.orcFilterPushDown.toString) ++
                Map("isOapOrcFileFormat" -> "true") ++
                _fsRelation.options

            _fsRelation.copy(fileFormat = oapFileFormat,
              options = orcOptions)(_fsRelation.sparkSession)

          } else {
            logInfo("hasAvailableIndex = false, will retain OrcFileFormat.")
            _fsRelation
          }

        case _: OapFileFormat =>
          _fsRelation.fileFormat.asInstanceOf[OapFileFormat].init(
            _fsRelation.sparkSession,
            _fsRelation.options,
            selectedPartitions.flatMap(p => p.files))
          _fsRelation

        case _: FileFormat =>
          _fsRelation
      }

      val dataColumns =
        l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

      // Partition keys are not available in the statistics of the files.
      val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

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

      val outputAttributes = readDataColumns ++ partitionColumns

      val scan =
        FileSourceScanExec(
          fsRelation,
          outputAttributes,
          outputSchema,
          partitionKeyFilters.toSeq,
          dataFilters,
          table.map(_.identifier))

      val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
      val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
      val withProjections = if (projects == withFilter.output) {
        withFilter
      } else {
        execution.ProjectExec(projects, withFilter)
      }

      withProjections :: Nil

    case _ => Nil
  }
}
