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

package org.apache.spark.sql.execution.datasources.spinach

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

private[spinach] class SpinachDataWriter(
    isCompressed: Boolean,
    out: FSDataOutputStream,
    schema: StructType) extends Logging {
  // Using java options to config
  // NOTE: java options should not start with spark (e.g. "spark.xxx.xxx"), or it cannot pass
  // the config validation of SparkConf
  // TODO make it configuration via SparkContext / Table Properties
  private def DEFAULT_ROW_GROUP_SIZE = System.getProperty("spinach.rowgroup.size",
    "1024").toInt
  logDebug(s"spinach.rowgroup.size setting to ${DEFAULT_ROW_GROUP_SIZE}")
  private var rowCount: Int = 0
  private var rowGroupCount: Int = 0

  private val rowGroup: Array[DataFiberBuilder] =
    DataFiberBuilder.initializeFromSchema(schema, DEFAULT_ROW_GROUP_SIZE)

  private val fiberMeta = new SpinachDataFileHandle(
    rowCountInEachGroup = DEFAULT_ROW_GROUP_SIZE, fieldCount = schema.length)

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
    var idx: Int = 0
    var totalDataSize = 0L
    val rowGroupMeta = new RowGroupMeta()

    rowGroupMeta.withNewStart(out.getPos).withNewFiberLens(fiberLens)
    while (idx < rowGroup.length) {
      val fiberByteData = rowGroup(idx).build()
      val newFiberData = fiberByteData.fiberData
      totalDataSize += newFiberData.length
      fiberLens(idx) = newFiberData.length
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
  filterScanner: Option[RangeScanner],
  requiredIds: Array[Int]) {

  def initialize(conf: Configuration): Iterator[InternalRow] = {
    // TODO how to save the additional FS operation to get the Split size
    val fileScanner = DataFile(path.toString, meta.schema, meta.dataReaderClassName)

    filterScanner match {
      case Some(fs) if fs.exist(path, conf) => fs.initialize(path, conf)
        val indexPath = IndexUtils.indexFileFromDataFile(path, fs.meta.name)

        val analysis =
          if (fs.canBeOptimizedByStatistics) {
            readStatistics(fs.intervalArray, indexPath, conf)
          } else 1 // for index without Statistics, use the index
        if (analysis == 0) {
          fileScanner.iterator(conf, requiredIds)
        } else if (analysis == 1) {
          // total Row count can be get from the filter scanner
          val rowIDs = fs.toArray.sorted
          fileScanner.iterator(conf, requiredIds, rowIDs)
        } else {
          Iterator.empty
        }
      case _ =>
        fileScanner.iterator(conf, requiredIds)
    }
  }

  /**
   * Through getting statistics from related index file,
   * judging if we should bypass this datafile or full scan or by index.
   * return -1 means bypass, close to 1 means full scan and close to 0 means by index.
   */
  private def readStatistics(intervalArray: ArrayBuffer[RangeInterval],
                             indexPath: Path, conf: Configuration): Double = {
    if (intervalArray.length == 0) {
      -1
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

      while (arrayOffset < stsEndOffset && resSum != -1) {
        val id = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + arrayOffset)
        val st = id match {
          case 0 => new MinMaxStatistics()
          case 1 => new SampleBasedStatistics()
          case 2 => new PartedByValueStatistics
          case _ => throw new UnsupportedOperationException(s"non-supported statistic in id $id")
        }
        val res = st.read(filterScanner.get.keySchema,
          filterScanner.get.intervalArray, stsArray, arrayOffset)
        arrayOffset = st.arrayOffset

        if (res == -1) {
          resSum = -1
        } else {
          resSum += res
          resNum += 1
        }
      }

      if (resSum == -1) -1
      else if (resNum == 0) 0
      else {
        if (resSum / resNum >= 0.8) 1
        else 0
      }
    }
  }
}
