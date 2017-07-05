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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.{DataSourceMeta, OapFileFormat}
import org.apache.spark.sql.execution.datasources.oap.filecache.{DataFiberBuilder, FiberCacheManager}
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.execution.datasources.oap.statistics._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

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
  private val compressor: BytesCompressor =
    codecFactory.getCompressor(COMPRESSION_CODEC)

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
    out.close()
  }
}

private[oap] class OapDataReader(
  path: Path,
  meta: DataSourceMeta,
  filterScanner: Option[IndexScanner],
  requiredIds: Array[Int]) extends Logging {

  def initialize(conf: Configuration): Iterator[InternalRow] = {
    logDebug("Initializing OapDataReader...")
    // TODO how to save the additional FS operation to get the Split size
    val fileScanner = DataFile(path.toString, meta.schema, meta.dataReaderClassName)

    val start = System.currentTimeMillis()
    filterScanner match {
      case Some(fs) if fs.existRelatedIndexFile(path, conf) =>
        val indexPath = IndexUtils.indexFileFromDataFile(path, fs.meta.name, fs.meta.time)

        val initFinished = System.currentTimeMillis()
        val statsAnalyseResult = tryToReadStatistics(indexPath, conf)
        val statsAnalyseFinished = System.currentTimeMillis()

        /**
         * [WORKAROUND] if index file is larger than cache.maximumWeight / cache.concurrencyLevel,
         * it will be removed immediately after loading and MemoryBlock in FiberCache is freed.
         * But the IndexFiberCache is still used by IndexScanner cause JVM crash. For now, we skip
         * using index if index file is too large. To fix this, we need TODO:
         * 1. A better way to config cache.maximumWeight to maximize the use of off heap memory
         *    Currently, we let user to config this parameter and use a very small value in case
         *    off heap memory overhead.
         * 2. Split large fiber into small ones
         *   Currently, index file is a very large fiber, and a column in row group is a fiber.
         *   If row group is large, then fiber is large. We can expect a column in row group has
         *   multiple fibers and index fiber can slit into several small parts.
         * 3. Handle the exception if FiberCache is larger than maximumWeight / concurrencyLevel
         */
        val indexFileSize = indexPath.getFileSystem(conf).getContentSummary(indexPath).getLength
        val iter =
          if (indexFileSize > FiberCacheManager.getMaximumFiberSizeInBytes(conf)) {
            logWarning(s"Index File size $indexFileSize B is too large and couldn't be cached." +
              s"Please increase ${SQLConf.OAP_FIBERCACHE_SIZE.key} for better performance")
            fileScanner.iterator(conf, requiredIds)
          } else {
            statsAnalyseResult match {
              case StaticsAnalysisResult.FULL_SCAN =>
                fileScanner.iterator(conf, requiredIds)
              case StaticsAnalysisResult.USE_INDEX =>
                fs.initialize(path, conf)
                // total Row count can be get from the filter scanner
                val rowIDs = fs.toArray.sorted
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
