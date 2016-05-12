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

import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, RecordWriter, TaskAttemptContext}
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.io.SnappyCompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

private[spinach] class SpinachDataWriter2(
    isCompressed: Boolean,
    out: FSDataOutputStream,
    schema: StructType) extends RecordWriter[NullWritable, InternalRow] with Logging {
  // TODO: make the fiber size configurable
  // Temporarily solve this TODO with java options
  // NOTE: java options should not start with spark (e.g. "spark.xxx.xxx"), or it cannot pass
  // the config validation of SparkConf
  private def DEFAULT_ROW_GROUP_SIZE = System.getProperty("spinach.rowgroup.size",
    "1024").toInt
  logDebug(s"spinach.rowgroup.size setting to ${DEFAULT_ROW_GROUP_SIZE}")
  private var rowCount: Int = 0
  private var rowGroupCount: Int = 0

  private val rowGroup: Array[FiberBuilder] =
    FiberBuilder.initializeFromSchema(schema, DEFAULT_ROW_GROUP_SIZE)

  private val fiberMeta = new DataFileMeta(
    rowCountInEachGroup = DEFAULT_ROW_GROUP_SIZE, fieldCount = schema.length)

  private val compCodec = new SnappyCompressionCodec(new SparkConf())

  override def write(ignore: NullWritable, row: InternalRow) {
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
      // TODO: disable compression temporarily, because there's some issue to solve for compression
      val newFiberData = if (false) {
        compCodec.compressedOutputBuffer(fiberByteData.fiberData)
      } else {
        fiberByteData.fiberData
      }
      totalDataSize += newFiberData.length
      fiberLens(idx) = newFiberData.length
      out.write(newFiberData)
      rowGroup(idx).clear()
      idx += 1
    }

    fiberMeta.appendRowGroupMeta(rowGroupMeta.withNewEnd(out.getPos))
  }

  override def close(context: TaskAttemptContext) {
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

private[spinach] class SpinachDataReader2(
    path: Path,
    schema: StructType,
    filterScanner: Option[RangeScanner],
    requiredIds: Array[Int]) extends RecordReader[NullWritable, InternalRow] {

  private var totalRowCount: Int = 0
  private var currentRowId: Int = 0
  private var currentRowIter: Iterator[InternalRow] = _
  private var currentRow: InternalRow = _
  var dataFileMeta: DataFileMeta = _

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    // TODO how to save the additional FS operation to get the Split size
    val pathInstring = path.toString
    val fileScanner = DataFileScanner(path.toString, schema, context)
    dataFileMeta = DataMetaCacheManager(fileScanner)

    filterScanner match {
      case Some(fs) => fs.initialize(pathInstring, context)
        // total Row count can be get from the filter scanner
        val rowIDs = fs.toArray.sorted
        totalRowCount = rowIDs.length
        currentRowIter = fileScanner.iterator(requiredIds, rowIDs)
      case None =>
        totalRowCount = dataFileMeta.totalRowCount()
        currentRowIter = fileScanner.iterator(requiredIds)
    }
  }

  override def getProgress: Float = if (totalRowCount > 0) {
    currentRowId / totalRowCount
  } else {
    1.0f
  }

  override def nextKeyValue(): Boolean = {
    if (currentRowIter.hasNext) {
      currentRowId += 1
      currentRow = currentRowIter.next()
      true
    } else {
      false
    }
  }

  def getCurrentValue: InternalRow = currentRow

  override def getCurrentKey: NullWritable = NullWritable.get()

  override def close() {
  }
}
