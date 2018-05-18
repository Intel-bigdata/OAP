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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.execution.datasources.oap.{DataSourceMeta, OapFileFormat}
import org.apache.spark.sql.execution.datasources.oap.index.IndexScanners
import org.apache.spark.sql.execution.datasources.oap.utils.OapIndexInfoStatusSerDe
import org.apache.spark.sql.oap.listener.SparkListenerOapIndexInfoUpdate
import org.apache.spark.sql.sources.Filter
import org.apache.spark.util.TimeStampedHashMap

private[oap] case class OapIndexInfoStatus(path: String, useIndex: Boolean)

private[sql] object OapIndexInfo extends Logging {
  val partitionOapIndex = new TimeStampedHashMap[String, Boolean](updateTimeStampOnGet = true)

  def status: String = {
    val indexInfoStatusSeq = partitionOapIndex.map(kv => OapIndexInfoStatus(kv._1, kv._2)).toSeq
    val threshTime = System.currentTimeMillis()
    partitionOapIndex.clearOldValues(threshTime)
    logDebug("current partition files: \n" +
      indexInfoStatusSeq.map { indexInfoStatus =>
        "partition file: " + indexInfoStatus.path +
          " use index: " + indexInfoStatus.useIndex + "\n" }.mkString("\n"))
    val indexStatusRawData = OapIndexInfoStatusSerDe.serialize(indexInfoStatusSeq)
    indexStatusRawData
  }

  def update(indexInfo: SparkListenerOapIndexInfoUpdate): Unit = {
    val indexStatusRawData = OapIndexInfoStatusSerDe.deserialize(indexInfo.oapIndexInfo)
    indexStatusRawData.foreach {oapIndexInfo =>
      logInfo("\nhost " + indexInfo.hostName + " executor id: " + indexInfo.executorId +
        "\npartition file: " + oapIndexInfo.path + " use OAP index: " + oapIndexInfo.useIndex)}
  }
}

/**
 * Compared to [[OapDataReader]], this is a relatively low-level reader, which doesn't care about
 * things like skipByPartitionFile, etc.
 * Note that there is no abstract class like OapDataScanner due to the 'initialize' function's
 * definition should be arbitrary, for instance, whatever the args are, as long as to return an
 * Iterator.
 */
private[oap] class OapDataScannerV1(
    path: Path,
    meta: DataSourceMeta,
    filterScanners: Option[IndexScanners],
    requiredIds: Array[Int],
    context: Option[VectorizedContext] = None) extends Logging {

  import org.apache.spark.sql.execution.datasources.oap.INDEX_STAT._

  private var _rowsReadWhenHitIndex: Option[Long] = None
  private var _indexStat = MISS_INDEX

  def rowsReadByIndex: Option[Long] = _rowsReadWhenHitIndex
  def indexStat: INDEX_STAT = _indexStat

  def totalRows(): Long = _totalRows
  private var _totalRows: Long = 0

  def initialize(
      conf: Configuration,
      options: Map[String, String] = Map.empty,
      filters: Seq[Filter] = Nil): OapIterator[InternalRow] = {
    logDebug("Initializing OapDataScanner...")
    // TODO how to save the additional FS operation to get the Split size
    val fileScanner = DataFile(path.toString, meta.schema, meta.dataReaderClassName, conf)
    if (meta.dataReaderClassName.contains("ParquetDataFile")) {
      fileScanner.asInstanceOf[ParquetDataFile].setVectorizedContext(context)
    }

    def fullScan: OapIterator[InternalRow] = {
      val start = if (log.isDebugEnabled) System.currentTimeMillis else 0
      val iter = fileScanner.iterator(requiredIds, filters)
      val end = if (log.isDebugEnabled) System.currentTimeMillis else 0

      _totalRows = fileScanner.totalRows()

      logDebug("Construct File Iterator: " + (end - start) + " ms")
      iter
    }

    filterScanners match {
      case Some(indexScanners) if indexScanners.indexIsAvailable(path, conf) =>
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
          if (meta.dataReaderClassName.contains("ParquetDataFile")) {
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
}
