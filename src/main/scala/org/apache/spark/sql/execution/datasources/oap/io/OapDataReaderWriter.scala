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

import org.apache.spark.executor.custom.CustomManager
import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListenerOapIndexInfoUpdate
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.{DataSourceMeta, OapFileFormat}
import org.apache.spark.sql.execution.datasources.oap.filecache.DataFiberBuilder
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.execution.datasources.oap.statistics._
import org.apache.spark.sql.execution.datasources.oap.utils.OapIndexInfoStatusSerDe
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
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

  private def updateStats(stats: ColumnStatistics.ParquetStatistics,
                          row: InternalRow, index: Int, dataType: DataType) = {
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
      if (!row.isNullAt(idx)) updateStats(statisticsArray(idx), row, idx, schema(idx).dataType)
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
      if (dictionaryDataLength > 0) out.write(dictByteData)
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
      indexInfoStatusSeq.map{case indexInfoStatus: OapIndexInfoStatus =>
        "partition file: " + indexInfoStatus.path +
          " use index: " + indexInfoStatus.useIndex + "\n"}.mkString("\n"))
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
  filterScanner: Option[IndexScanner],
  requiredIds: Array[Int]) extends Logging {

  def initialize(conf: Configuration,
                 options: Map[String, String] = Map.empty): Iterator[InternalRow] = {
    logDebug("Initializing OapDataReader...")
    // TODO how to save the additional FS operation to get the Split size
    val fileScanner = DataFile(path.toString, meta.schema, meta.dataReaderClassName, conf)

    val start = System.currentTimeMillis()
    filterScanner match {
      case Some(fs) if fs.existRelatedIndexFile(path, conf) =>
        val indexPath = IndexUtils.indexFileFromDataFile(path, fs.meta.name, fs.meta.time)

        val initFinished = System.currentTimeMillis()
        val statsAnalyseResult = tryToReadStatistics(indexPath, conf)
        val statsAnalyseFinished = System.currentTimeMillis()

        val indexFileSize = indexPath.getFileSystem(conf).getContentSummary(indexPath).getLength
        val dataFileSize = path.getFileSystem(conf).getContentSummary(path).getLength
        val isTesting = conf.getBoolean(SQLConf.OAP_IS_TESTING.key,
                              SQLConf.OAP_IS_TESTING.defaultValue.get)
        val enableOIndex = conf.getBoolean(SQLConf.OAP_ENABLE_OINDEX.key,
          SQLConf.OAP_ENABLE_OINDEX.defaultValue.get)
        val isAscending = options.getOrElse(
          OapFileFormat.OAP_QUERY_ORDER_OPTION_KEY, "false").toBoolean
        val limit = options.getOrElse(OapFileFormat.OAP_QUERY_LIMIT_OPTION_KEY, "0").toInt

        if (options.contains(OapFileFormat.OAP_INDEX_SCAN_NUM_OPTION_KEY)) {
          fs.setScanNumLimit(
            options.get(OapFileFormat.OAP_INDEX_SCAN_NUM_OPTION_KEY).get.toInt
          )
        }

        val isFastIndexQuery : Boolean =
          limit > 0 || options.contains(OapFileFormat.OAP_INDEX_SCAN_NUM_OPTION_KEY)

        /**
         * Once index is disabled, there is no way to do fast query.
         * OapStrategy should aware of this and create a non-fast query plan.
         */
        assert((!enableOIndex && isFastIndexQuery) == false)

        val iter =
          // Below is for OAP developers to easily analyze and compare performance without removing
          // the index after it's created.
          if (!enableOIndex) {
            logWarning("OAP index is disabled. Using below approach to enable index,\n" +
              "sqlContext.conf.setConfString(SQLConf.OAP_USE_INDEX_FOR_DEVELOPERS.key, true)")
            fileScanner.iterator(conf, requiredIds)
          } else if (!isFastIndexQuery && indexFileSize > dataFileSize * 0.7 && !isTesting) {
            logWarning(s"Index File size $indexFileSize B is too large comparing " +
                        s"to Data File Size $dataFileSize. Using Data File Scan instead.")
            fileScanner.iterator(conf, requiredIds)
          } else {
            statsAnalyseResult match {
              case StaticsAnalysisResult.FULL_SCAN if !isFastIndexQuery =>
                fileScanner.iterator(conf, requiredIds)
              case StaticsAnalysisResult.USE_INDEX =>
                fs.initialize(path, conf)
                // total Row count can be get from the filter scanner
                val rowIDs = {
                  if (limit > 0) {
                    if (isAscending) fs.toArray.take(limit)
                    else fs.toArray.reverse.take(limit)
                  }
                  else fs.toArray
                }

                OapIndexInfo.partitionOapIndex.put(path.toString(), true)
                logInfo("Partition File " + path.toString() + " will use OAP index.\n")
                fileScanner.iterator(conf, requiredIds, rowIDs)
              case StaticsAnalysisResult.SKIP_INDEX =>
                Iterator.empty
            }
          }

        val iteratorFinished = System.currentTimeMillis()
        logDebug("Load Index: " + (initFinished - start) + "ms")
        logDebug("Load Stats: " + (statsAnalyseFinished - initFinished) + "ms")
        logDebug("Construct Iterator: " + (iteratorFinished - statsAnalyseFinished) + "ms")
        iter
      case _ =>
        logDebug("No index file exist for data file: " + path)

        val iter = fileScanner.iterator(conf, requiredIds)
        val iteratorFinished = System.currentTimeMillis()
        logDebug("Construct Iterator: " + (iteratorFinished - start) + "ms")

        iter
    }
  }

  /**
   * Through getting statistics from related index file,
   * judging if we should bypass this datafile or full scan or by index.
   * return -1 means bypass, close to 1 means full scan and close to 0 means by index.
   */
  private def tryToReadStatistics(indexPath: Path, conf: Configuration): Double = {
    if (!filterScanner.get.canBeOptimizedByStatistics) {
      StaticsAnalysisResult.USE_INDEX
    } else if (filterScanner.get.intervalArray.isEmpty) {
      StaticsAnalysisResult.SKIP_INDEX
    } else {
      val fs = indexPath.getFileSystem(conf)
      val fin = fs.open(indexPath)

      // read stats size
      val fileLength = fs.getContentSummary(indexPath).getLength.toInt
      val startPosArray = new Array[Byte](8)

      fin.readFully(fileLength - 24, startPosArray)

      val stBase = Platform.getLong(startPosArray, Platform.BYTE_ARRAY_OFFSET).toInt

      val stsArray = new Array[Byte](fileLength - stBase)
      fin.readFully(stBase, stsArray)
      fin.close()

      val statisticsManager = new StatisticsManager
      statisticsManager.read(stsArray, filterScanner.get.getSchema)
      statisticsManager.analyse(filterScanner.get.intervalArray, conf)
    }
  }
}
