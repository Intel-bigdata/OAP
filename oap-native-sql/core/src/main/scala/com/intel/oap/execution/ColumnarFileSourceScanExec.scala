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
 
package com.intel.oap.execution

import com.intel.oap.ColumnarPluginConfig

import java.util.concurrent.TimeUnit._

import scala.collection.mutable.HashMap

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ColumnarParquetFileFormatHandler
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

class ColumnarFileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier])
    extends FileSourceScanExec(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    dataFilters,
    tableIdentifier) {
    
    val tmpDir = ColumnarPluginConfig.getConf(sparkContext.getConf).tmpFile

    override lazy val supportsColumnar: Boolean = true

    val driverMetrics: HashMap[String, Long] = HashMap.empty

    private def sendDriverMetrics(): Unit = {
      driverMetrics.foreach(e => metrics(e._1).add(e._2))
      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
        metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
    }

    @transient private lazy val selectedPartitions: Array[PartitionDirectory] = {
      val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
      val startTime = System.nanoTime()
      val ret = relation.location.listFiles(partitionFilters, dataFilters)
      driverMetrics("numFiles") = ret.map(_.files.size.toLong).sum
      val timeTakenMs = NANOSECONDS.toMillis(
        (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
      driverMetrics("metadataTime") = timeTakenMs
      ret
    }.toArray

    /* @transient
    private override lazy val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
    logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}") */

    protected def createColumnarBucketedReadRDD(
        bucketSpec: BucketSpec,
        readFile: (PartitionedFile) => Iterator[ColumnarBatch],
        selectedPartitions: Array[PartitionDirectory],
        fsRelation: HadoopFsRelation): RDD[ColumnarBatch] = {
      logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
      val filesGroupedToBuckets =
        selectedPartitions.flatMap { p =>
          p.files.map { f =>
            PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
          }
        }.groupBy { f =>
          BucketingUtils
            .getBucketId(new Path(f.filePath).getName)
            .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
        }

      val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
        val bucketSet = optionalBucketSet.get
        filesGroupedToBuckets.filter {
          f => bucketSet.get(f._1)
        }
      } else {
        filesGroupedToBuckets
      }

      val filePartitions = Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
        FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
      }

      new ColumnarFileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
    }

    protected def createColumnarNonBucketedReadRDD(
        readFile: (PartitionedFile) => Iterator[ColumnarBatch],
        selectedPartitions: Array[PartitionDirectory],
        fsRelation: HadoopFsRelation): RDD[ColumnarBatch] = {
      val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
      val maxSplitBytes =
        FilePartition.maxSplitBytes(fsRelation.sparkSession, selectedPartitions)
      logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
        s"open cost is considered as scanning $openCostInBytes bytes.")

      val splitFiles = selectedPartitions.flatMap { partition =>
        partition.files.flatMap { file =>
          // getPath() is very expensive so we only want to call it once in this block:
          val filePath = file.getPath
          val isSplitable = relation.fileFormat.isSplitable(
            relation.sparkSession, relation.options, filePath)
          PartitionedFileUtil.splitFiles(
            sparkSession = relation.sparkSession,
            file = file,
            filePath = filePath,
            isSplitable = isSplitable,
            maxSplitBytes = maxSplitBytes,
            partitionValues = partition.values
          )
        }
      }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

      val partitions =
        FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)

      new ColumnarFileScanRDD(fsRelation.sparkSession, readFile, partitions)
    }

    lazy val inputColumnarRDD: RDD[ColumnarBatch] = {
      val readFile: (PartitionedFile) => Iterator[ColumnarBatch] =
        relation.fileFormat match {
          case _: ParquetFileFormat =>
            logInfo(s"Input file format is parquet file. Columnar file scan supported.")
            ColumnarParquetFileFormatHandler.buildColumnarReaderWithPartitionValues(
              relation,
              relation.sparkSession,
              relation.dataSchema,
              relation.partitionSchema,
              requiredSchema,
              dataFilters,
              relation.options,
              relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options),
              tmpDir)
          case _: FileFormat =>
            throw new UnsupportedOperationException(s"InputColumnarRDD is not supported for $this")
        }

      val readRDD = relation.bucketSpec match {
        case Some(bucketing) if relation.sparkSession.sessionState.conf.bucketingEnabled =>
          createColumnarBucketedReadRDD(bucketing, readFile, selectedPartitions, relation)
        case _ =>
          createColumnarNonBucketedReadRDD(readFile, selectedPartitions, relation)
      }
      sendDriverMetrics()
      readRDD
    }

    override def doExecuteColumnar(): RDD[ColumnarBatch] = {
      val numOutputRows = longMetric("numOutputRows")
      inputColumnarRDD.map { batch =>
        numOutputRows += batch.numRows()
        batch
      }
    }
}
