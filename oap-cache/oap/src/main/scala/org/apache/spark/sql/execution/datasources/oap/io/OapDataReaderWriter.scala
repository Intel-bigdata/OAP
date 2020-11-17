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

package org.apache.spark.sql.execution.datasources.oap.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.mapred.OrcStruct
import org.apache.parquet.filter2.predicate.FilterPredicate

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.execution.datasources.oap.utils.FilterHelper
import org.apache.spark.sql.execution.datasources.orc.OrcDeserializer
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

private[oap] class OapDataReaderV1(
    pathStr: String,
    meta: DataSourceMeta,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filterScanners: Option[IndexScanners],
    requiredIds: Array[Int],
    pushed: Option[FilterPredicate],
    metrics: OapMetricsManager,
    conf: Configuration,
    enableVectorizedReader: Boolean = false,
    options: Map[String, String] = Map.empty,
    filters: Seq[Filter] = Seq.empty,
    context: Option[DataFileContext] = None) extends OapDataReader with Logging {

  import org.apache.spark.sql.execution.datasources.oap.INDEX_STAT._

  private var _rowsReadWhenHitIndex: Option[Long] = None
  private var _indexStat = MISS_INDEX

  override def rowsReadByIndex: Option[Long] = _rowsReadWhenHitIndex
  override def indexStat: INDEX_STAT = _indexStat

  def totalRows(): Long = _totalRows
  private var _totalRows: Long = 0
  private val path = new Path(pathStr)

  private val dataFileClassName = meta.dataReaderClassName

  def initialize(): OapCompletionIterator[Any] = {
    logDebug("Initializing OapDataReader...")
    // TODO how to save the additional FS operation to get the Split size
    val fileScanner = DataFile(pathStr, meta.schema, dataFileClassName, conf)
    if (meta.dataReaderClassName.equals(OapFileFormat.PARQUET_DATA_FILE_CLASSNAME)) {
      fileScanner.asInstanceOf[ParquetDataFile].setParquetVectorizedContext(
        context.asInstanceOf[Option[ParquetVectorizedContext]])
    } else if (meta.dataReaderClassName.equals(OapFileFormat.ORC_DATA_FILE_CLASSNAME)) {
      // For orc, the context will be used by both vectorization and non vectorization.
      fileScanner.asInstanceOf[OrcDataFile].setOrcDataFileContext(
        context.get.asInstanceOf[OrcDataFileContext])
    }

    def fullScan: OapCompletionIterator[Any] = {
      val start = if (log.isDebugEnabled) System.currentTimeMillis else 0
      val iter = fileScanner.iterator(requiredIds, filters)
      val end = if (log.isDebugEnabled) System.currentTimeMillis else 0

      _totalRows = fileScanner.totalRows()

      logDebug("Construct File Iterator: " + (end - start) + " ms")
      iter
    }

    filterScanners match {
      case Some(indexScanners) if indexScanners.isIndexFileBeneficial(path, conf) =>
        def getRowIds(options: Map[String, String]): Array[Int] = {
          indexScanners.initialize(path, conf)

          _totalRows = indexScanners.totalRows()

          // total Row count can be get from the index scanner
          val limit = options.getOrElse(OapFileFormat.OAP_QUERY_LIMIT_OPTION_KEY, "0").toInt
          val rowIds = if (limit > 0) {
            // Order limit scan options
            val isAscending = options.getOrElse(
              OapFileFormat.OAP_QUERY_ORDER_OPTION_KEY, "true").toBoolean
            val sameOrder = !((indexScanners.order == Ascending) ^ isAscending)

            if (sameOrder) {
              indexScanners.take(limit).toArray
            } else {
              indexScanners.toArray.reverse.take(limit)
            }
          } else {
            indexScanners.toArray
          }

          // Parquet reader does not support backward scan, so rowIds must be sorted.
          // Actually Orc readers support the backward scan, thus no need to sort row Ids.
          // But with the sorted row Ids, the adjacment rows will be scanned in the same batch.
          // This will reduce IO cost.
          if (meta.dataReaderClassName.equals(OapFileFormat.PARQUET_DATA_FILE_CLASSNAME) ||
            meta.dataReaderClassName.equals(OapFileFormat.ORC_DATA_FILE_CLASSNAME)) {
            rowIds.sorted
          } else {
            rowIds
          }
        }


        val start = if (log.isDebugEnabled) System.currentTimeMillis else 0
        val rows = getRowIds(options)
        val iter = fileScanner.iteratorWithRowIds(requiredIds, rows, filters)
        val end = if (log.isDebugEnabled) System.currentTimeMillis else 0

        _indexStat = HIT_INDEX
        _rowsReadWhenHitIndex = Some(rows.length)
        logDebug("Construct File Iterator: " + (end - start) + "ms")
        iter
      case Some(_) =>
        _indexStat = IGNORE_INDEX
        fullScan
      case _ =>
        fullScan
    }
  }

  override def read(file: PartitionedFile): Iterator[InternalRow] = {
    FilterHelper.setFilterIfExist(conf, pushed)

    val iter = initialize()
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
    val tot = totalRows()
    metrics.updateTotalRows(tot)
    metrics.updateIndexAndRowRead(this, tot)
    // if enableVectorizedReader == true , return iter directly because of partitionValues
    // already filled by VectorizedReader, else use original branch as Parquet or Orc.
    if (enableVectorizedReader) {
      iter.asInstanceOf[Iterator[InternalRow]]
    } else {
      // Parquet and Oap are the same if the vectorization is off.
      val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
      val joinedRow = new JoinedRow()
      val appendPartitionColumns =
        GenerateUnsafeProjection.generate(fullSchema, fullSchema)
      meta.dataReaderClassName match {
        case dataReader if dataReader.equals(OapFileFormat.PARQUET_DATA_FILE_CLASSNAME) =>
          iter.asInstanceOf[Iterator[InternalRow]].map(d => {
            appendPartitionColumns(joinedRow(d, file.partitionValues))
          })
        case dataReader if dataReader.equals(OapFileFormat.ORC_DATA_FILE_CLASSNAME) =>
          val orcDataFileContext = context.get.asInstanceOf[OrcDataFileContext]
          val deserializer = new OrcDeserializer(orcDataFileContext.dataSchema, requiredSchema,
            orcDataFileContext.requestedColIds)
          iter.asInstanceOf[Iterator[OrcStruct]].map(value => {
            appendPartitionColumns(joinedRow(deserializer.deserialize(value),
              file.partitionValues))})
      }
    }
  }
}
