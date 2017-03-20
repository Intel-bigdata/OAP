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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

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
      case Some(fs) if fs.existRelatedIndexFile(path, conf) => fs.initialize(path, conf)

        // should find the index of the file in this path
        // so that we could find related statistics info
        val fileIndex = meta.fileMetas.indexWhere(_.dataFileName == path.getName)
        AnalyzeFromStatistics(fileIndex, fs, conf) match {
          case StaticsAnalysisResult.FULL_SCAN =>
            fileScanner.iterator(conf, requiredIds)
          case StaticsAnalysisResult.USE_INDEX =>
            // total Row count can be get from the filter scanner
            val rowIDs = fs.toArray.sorted
            fileScanner.iterator(conf, requiredIds, rowIDs)
          case StaticsAnalysisResult.SKIP_INDEX =>
            Iterator.empty
        }
      case _ =>
        fileScanner.iterator(conf, requiredIds)
    }
  }

  def AnalyzeFromStatistics(fileIndex: Int, fs: RangeScanner, conf: Configuration): Int = {
    if (!fs.canBeOptimizedByStatistics) {
      StaticsAnalysisResult.USE_INDEX
    } else if (fs.intervalArray.length == 0 || meta.statsMetas.length == 0) {
      StaticsAnalysisResult.SKIP_INDEX
    } else {
      var resCollect: Double = 0
      var resStatCount: Int = 0
      var i = 0

      while (i < meta.statsMetas.length && resCollect != StaticsAnalysisResult.SKIP_INDEX) {
        val keySchema = fs.getSchema()
        val ids = keySchema.map(field => meta.schema.indexWhere(_ == field))
        val res = meta.statsMetas(i).statistics.analyze(fileIndex, ids.toArray,
          keySchema, fs.intervalArray.toArray)

        if (res == StaticsAnalysisResult.SKIP_INDEX) {
          resCollect = StaticsAnalysisResult.SKIP_INDEX
        } else if (res != StaticsAnalysisResult.UNSURE) {
          resCollect += res
          resStatCount += 1
        }
        i += 1
      }

      val fs_rate = conf.get(Statistics.thresName).toDouble

      if (resCollect == StaticsAnalysisResult.SKIP_INDEX) {
        StaticsAnalysisResult.SKIP_INDEX
      } else if (resStatCount != 0 && resCollect / resStatCount <= fs_rate) {
        StaticsAnalysisResult.USE_INDEX
      } else {
        StaticsAnalysisResult.FULL_SCAN
      }
    }
  }
}
