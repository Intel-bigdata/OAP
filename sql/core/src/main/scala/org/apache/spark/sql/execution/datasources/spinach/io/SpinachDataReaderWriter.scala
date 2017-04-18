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

package org.apache.spark.sql.execution.datasources.spinach.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.parquet.format.CompressionCodec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.spinach.DataSourceMeta
import org.apache.spark.sql.execution.datasources.spinach.filecache.DataFiberBuilder
import org.apache.spark.sql.execution.datasources.spinach.index._
import org.apache.spark.sql.execution.datasources.spinach.statistics._
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform


private[spinach] class SpinachDataWriter(
    isCompressed: Boolean,
    out: FSDataOutputStream,
    schema: StructType,
    conf: Configuration) extends Logging {
  // Using java options to config
  // NOTE: java options should not start with spark (e.g. "spark.xxx.xxx"), or it cannot pass
  // the config validation of SparkConf
  // TODO make it configuration via SparkContext / Table Properties
  private def DEFAULT_ROW_GROUP_SIZE = System.getProperty("spinach.rowgroup.size",
    "1024").toInt
  logDebug(s"spinach.rowgroup.size setting to ${DEFAULT_ROW_GROUP_SIZE}")
  private def COMPRESSION_CODEC_NAME = System.getProperty("spinach.compression.codec", "GZIP")
  logDebug(s"spinach.compression.codec setting to ${COMPRESSION_CODEC_NAME}")
  private var rowCount: Int = 0
  private var rowGroupCount: Int = 0

  private val rowGroup: Array[DataFiberBuilder] =
    DataFiberBuilder.initializeFromSchema(schema, DEFAULT_ROW_GROUP_SIZE)

  private val fiberMeta = new SpinachDataFileHandle(
    rowCountInEachGroup = DEFAULT_ROW_GROUP_SIZE,
    fieldCount = schema.length,
    codec = CompressionCodec.valueOf(COMPRESSION_CODEC_NAME))

  private val codecFactory = new CodecFactory(conf)
  private val compressor: BytesCompressor =
    codecFactory.getCompressor(CompressionCodec.valueOf(COMPRESSION_CODEC_NAME))

  def write(row: InternalRow) {
    var idx = 0
    while (idx < rowGroup.length) {
      rowGroup(idx).append(row)
      idx += 1
    }
    rowCount += 1
    if (rowCount % DEFAULT_ROW_GROUP_SIZE == 0) {
      writeRowGroup()
    }
  }

  private def writeRowGroup(): Unit = {
    rowGroupCount += 1
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
    val remainingRowCount = rowCount % DEFAULT_ROW_GROUP_SIZE
    if (remainingRowCount != 0) {
      // should be end of the insertion, put the row groups into the last row group
      writeRowGroup()
    }

    // and update the group count and row count in the last group
    fiberMeta
      .withGroupCount(rowGroupCount)
      .withRowCountInLastGroup(
        if (remainingRowCount != 0 || rowCount == 0) remainingRowCount else DEFAULT_ROW_GROUP_SIZE)

    fiberMeta.write(out)
    out.close()
  }
}

private[spinach] class SpinachDataReader(
  path: Path,
  meta: DataSourceMeta,
  filterScanner: Option[IndexScanner],
  requiredIds: Array[Int]) extends Logging {

  def initialize(conf: Configuration): Iterator[InternalRow] = {
    logDebug("Initializing SpinachDataReader...")
    // TODO how to save the additional FS operation to get the Split size
    val fileScanner = DataFile(path.toString, meta.schema, meta.dataReaderClassName)

    val start = System.currentTimeMillis()
    filterScanner match {
      case Some(fs) if fs.existRelatedIndexFile(path, conf) =>
        fs.initialize(path, conf)
        val indexPath = IndexUtils.indexFileFromDataFile(path, fs.meta.name)

        val initFinished = System.currentTimeMillis()
        val statsAnalyseResult = tryToReadStatistics(indexPath, conf)
        val statsAnalyseFinsihed = System.currentTimeMillis()

        val iter = statsAnalyseResult match {
          case StaticsAnalysisResult.FULL_SCAN =>
            fileScanner.iterator(conf, requiredIds)
          case StaticsAnalysisResult.USE_INDEX =>
            // total Row count can be get from the filter scanner
            val rowIDs = fs.toArray.sorted
            fileScanner.iterator(conf, requiredIds, rowIDs)
          case StaticsAnalysisResult.SKIP_INDEX =>
            Iterator.empty
        }
        val iteratorFinished = System.currentTimeMillis()
        logDebug("Load Index: " + (initFinished - start) + "ms")
        logDebug("Load Stats: " + (statsAnalyseFinsihed - initFinished) + "ms")
        logDebug("Construct Iterator: " + (iteratorFinished - statsAnalyseFinsihed) + "ms")
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
    } else if (filterScanner.get.intervalArray.length == 0) {
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

      var arrayOffset = 0L

      val stsEndOffset = fileLength - stBase - 24
      var resSum: Double = 0
      var resNum = 0

      while (arrayOffset < stsEndOffset && resSum != StaticsAnalysisResult.SKIP_INDEX) {
        val id = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + arrayOffset)
        val st = id match {
          case 0 => new MinMaxStatistics
          case 1 => new SampleBasedStatistics
          case 2 => new PartedByValueStatistics
          case _ => throw new UnsupportedOperationException(s"non-supported statistic in id $id")
        }
        val res = st.read(filterScanner.get.getSchema,
          filterScanner.get.intervalArray, stsArray, arrayOffset)
        arrayOffset = st.arrayOffset

        if (res == StaticsAnalysisResult.SKIP_INDEX) {
          resSum = StaticsAnalysisResult.SKIP_INDEX
        } else {
          resSum += res
          resNum += 1
        }
      }

      val fs_rate = conf.get(SQLConf.SPINACH_FULL_SCAN_THRESHOLD.key).toDouble

      if (resSum == StaticsAnalysisResult.SKIP_INDEX) {
        StaticsAnalysisResult.SKIP_INDEX
      } else if (resNum == 0 || resSum / resNum <= fs_rate) {
        StaticsAnalysisResult.USE_INDEX
      } else {
        StaticsAnalysisResult.FULL_SCAN
      }
    }
  }
}
