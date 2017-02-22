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
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
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

        val analysis = analyzeSts(indexPath, conf)
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
   * return -1 means bypass, 0 means full scan and 1 means by index.
   */
  def analyzeSts(indexPath: Path, conf: Configuration): Int = {
    val intervalArray = filterScanner.get.intervalArray

    if (intervalArray.length == 0) {
      -1
    } else {
      val fs = indexPath.getFileSystem(conf)
      val fin = fs.open(indexPath)

      // read stats size
      val fileLength = fs.getContentSummary(indexPath).getLength.toInt
      val offsetArray = new Array[Byte](8)

      fin.readFully(fileLength - 24, offsetArray)
      val stBase = Platform.getLong(offsetArray, Platform.BYTE_ARRAY_OFFSET).toInt

      val stsArray = new Array[Byte](fileLength - stBase)
      fin.readFully(stBase, stsArray)

      val stats = getSimpleStatistics(stsArray)

      fin.close()

      val min = stats.head._2
      val max = stats.last._2

      val start = intervalArray.head
      val end = intervalArray.last
      var result = false

      val ordering = GenerateOrdering.create(filterScanner.get.keySchema)

      if (start.start != RangeScanner.DUMMY_KEY_START) { // > or >= start
        if (start.startInclude) {
          result |= ordering.gt(start.start, max)
        } else {
          result |= ordering.gteq(start.start, max)
        }
      }
      if (end.end != RangeScanner.DUMMY_KEY_END) { // < or <= end
        if (end.endInclude) {
          result |= ordering.lt(end.end, min)
        } else {
          result |= ordering.lteq(end.end, min)
        }
      }

      if (result) -1 else 1
    }


  }

  def getSimpleStatistics(stsArray: Array[Byte]): ArrayBuffer[(Int, UnsafeRow, Long)] = {
    val sts = ArrayBuffer[(Int, UnsafeRow, Long)]()
    val size = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET)
    var i = 0
    var base = 4L

    while (i < size) {
      val now = extractSts(base, stsArray)
      sts += now
      i += 1
      base += now._1 + 8
    }

    sts
  }

  def extractSts(base: Long, stsArray: Array[Byte]): (Int, UnsafeRow, Long) = {
    val size = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + base)
    val value = getUnsafeRow(stsArray, base, size).copy()
    val offset = Platform.getLong(stsArray, Platform.BYTE_ARRAY_OFFSET + base + 4 + size)
    (size + 4, value, offset)
  }

  def getUnsafeRow(array: Array[Byte], offset: Long, size: Int): UnsafeRow = {
    val row = UnsafeRangeNode.row.get
    row.setNumFields(filterScanner.get.keySchema.length)
    row.pointTo(array, Platform.BYTE_ARRAY_OFFSET + offset + 4, size)
    row
  }
}
