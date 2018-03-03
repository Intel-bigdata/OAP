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
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.io.api.Binary

import org.apache.spark.SparkConf
import org.apache.spark.executor.custom.CustomManager
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerOapIndexInfoUpdate
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.execution.datasources.oap.{DataSourceMeta, OapFileFormat}
import org.apache.spark.sql.execution.datasources.oap.filecache.DataFiberBuilder
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.execution.datasources.oap.utils.OapIndexInfoStatusSerDe
import org.apache.spark.sql.types._
import org.apache.spark.util.TimeStampedHashMap

class OapIndexHeartBeatMessager extends CustomManager with Logging {
  override def status(conf: SparkConf): String = {
    OapIndexInfo.status
  }
}

// TODO: [linhong] Let's remove the `isCompressed` argument
private[oap] class OapDataWriter(
    isCompressed: Boolean,
    out: FSDataOutputStream,
    schema: StructType,
    conf: Configuration) extends Logging {

  private val ROW_GROUP_SIZE =
    conf.get(OapFileFormat.ROW_GROUP_SIZE, OapFileFormat.DEFAULT_ROW_GROUP_SIZE).toInt
  logDebug(s"${OapFileFormat.ROW_GROUP_SIZE} setting to $ROW_GROUP_SIZE")

  private val COMPRESSION_CODEC = CompressionCodec.valueOf(
    conf.get(OapFileFormat.COMPRESSION, OapFileFormat.DEFAULT_COMPRESSION))
  logDebug(s"${OapFileFormat.COMPRESSION} setting to ${COMPRESSION_CODEC.name()}")

  private var rowCount: Int = 0
  private var rowGroupCount: Int = 0

  private val rowGroup: Array[DataFiberBuilder] =
    DataFiberBuilder.initializeFromSchema(schema, ROW_GROUP_SIZE)

  private val statisticsArray = ColumnStatistics.getStatsFromSchema(schema)

  private def updateStats(
      stats: ColumnStatistics.ParquetStatistics,
      row: InternalRow,
      index: Int,
      dataType: DataType): Unit = {
    dataType match {
      case BooleanType => stats.updateStats(row.getBoolean(index))
      case IntegerType => stats.updateStats(row.getInt(index))
      case ByteType => stats.updateStats(row.getByte(index))
      case DateType => stats.updateStats(row.getInt(index))
      case ShortType => stats.updateStats(row.getShort(index))
      case StringType => stats.updateStats(
        Binary.fromConstantByteArray(row.getString(index).getBytes))
      case BinaryType => stats.updateStats(
        Binary.fromConstantByteArray(row.getBinary(index)))
      case FloatType => stats.updateStats(row.getFloat(index))
      case DoubleType => stats.updateStats(row.getDouble(index))
      case LongType => stats.updateStats(row.getLong(index))
      case _ => sys.error(s"Not support data type: $dataType")
    }
  }

  private val fiberMeta = new OapDataFileHandle(
    rowCountInEachGroup = ROW_GROUP_SIZE,
    fieldCount = schema.length,
    codec = COMPRESSION_CODEC)

  private val codecFactory = new CodecFactory(conf)

  def write(row: InternalRow) {
    var idx = 0
    while (idx < rowGroup.length) {
      rowGroup(idx).append(row)
      if (!row.isNullAt(idx)) {
        updateStats(statisticsArray(idx), row, idx, schema(idx).dataType)
      }
      idx += 1
    }
    rowCount += 1
    if (rowCount % ROW_GROUP_SIZE == 0) {
      writeRowGroup()
    }
  }

  private def writeRowGroup(): Unit = {
    rowGroupCount += 1
    val compressor: BytesCompressor = codecFactory.getCompressor(COMPRESSION_CODEC)
    val fiberLens = new Array[Int](rowGroup.length)
    val fiberUncompressedLens = new Array[Int](rowGroup.length)
    var idx: Int = 0
    var totalDataSize = 0L
    val rowGroupMeta = new RowGroupMeta()

    rowGroupMeta.withNewStart(out.getPos)
      .withNewFiberLens(fiberLens)
      .withNewUncompressedFiberLens(fiberUncompressedLens)
    while (idx < rowGroup.length) {
      val fiberByteData = rowGroup(idx).build()
      val newUncompressedFiberData = fiberByteData.fiberData
      val newFiberData = compressor.compress(newUncompressedFiberData)
      totalDataSize += newFiberData.length
      fiberLens(idx) = newFiberData.length
      fiberUncompressedLens(idx) = newUncompressedFiberData.length
      out.write(newFiberData)
      rowGroup(idx).clear()
      idx += 1
    }

    fiberMeta.appendRowGroupMeta(rowGroupMeta.withNewEnd(out.getPos))
  }

  def close() {
    val remainingRowCount = rowCount % ROW_GROUP_SIZE
    if (remainingRowCount != 0) {
      // should be end of the insertion, put the row groups into the last row group
      writeRowGroup()
    }

    rowGroup.indices.foreach { i =>
      val dictByteData = rowGroup(i).buildDictionary
      val encoding = rowGroup(i).getEncoding
      val dictionaryDataLength = dictByteData.length
      val dictionaryIdSize = rowGroup(i).getDictionarySize
      if (dictionaryDataLength > 0) {
        out.write(dictByteData)
      }
      fiberMeta.appendColumnMeta(new ColumnMeta(encoding, dictionaryDataLength, dictionaryIdSize,
        ColumnStatistics(statisticsArray(i))))
    }

    // and update the group count and row count in the last group
    fiberMeta
      .withGroupCount(rowGroupCount)
      .withRowCountInLastGroup(
        if (remainingRowCount != 0 || rowCount == 0) remainingRowCount else ROW_GROUP_SIZE)

    fiberMeta.write(out)
    codecFactory.release()
    out.close()
  }
}

private[oap] case class OapIndexInfoStatus(path: String, useIndex: Boolean)

private[oap] object OapIndexInfo extends Logging {
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

private[oap] class OapDataReader(
  path: Path,
  meta: DataSourceMeta,
  filterScanners: Option[IndexScanners],
  requiredIds: Array[Int]) extends Logging {

  import org.apache.spark.sql.execution.datasources.oap.INDEX_STAT._

  private var _rowsReadWhenHitIndex: Option[Long] = None
  private var _indexStat = MISS_INDEX

  def rowsReadByIndex: Option[Long] = _rowsReadWhenHitIndex
  def indexStat: INDEX_STAT = _indexStat

  def initialize(
      conf: Configuration,
      options: Map[String, String] = Map.empty): OapIterator[InternalRow] = {
    logDebug("Initializing OapDataReader...")
    // TODO how to save the additional FS operation to get the Split size
    val fileScanner = DataFile(path.toString, meta.schema, meta.dataReaderClassName, conf)

    def fullScan: OapIterator[InternalRow] = {
      val start = if (log.isDebugEnabled) System.currentTimeMillis else 0
      val iter = fileScanner.iterator(requiredIds)
      val end = if (log.isDebugEnabled) System.currentTimeMillis else 0
      logDebug("Construct File Iterator: " + (end - start) + " ms")
      iter
    }

    filterScanners match {
      case Some(indexScanners) if indexScanners.indexIsAvailable(path, conf) =>
        def getRowIds(options: Map[String, String]): Array[Int] = {
          indexScanners.initialize(path, conf)

          // total Row count can be get from the index scanner
          val limit = options.getOrElse(OapFileFormat.OAP_QUERY_LIMIT_OPTION_KEY, "0").toInt
          val rowIds = if (limit > 0) {
            // Order limit scan options
            val isAscending = options.getOrElse(
              OapFileFormat.OAP_QUERY_ORDER_OPTION_KEY, "true").toBoolean
            val sameOrder =
              !((indexScanners.order == Ascending) ^ isAscending)

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
        val iter = fileScanner.iterator(requiredIds, rows)
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
