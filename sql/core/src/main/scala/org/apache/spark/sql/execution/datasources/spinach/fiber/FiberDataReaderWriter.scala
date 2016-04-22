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

package org.apache.spark.sql.execution.datasources.spinach.fiber

import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, RecordWriter, TaskAttemptContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.spinach._
import org.apache.spark.sql.types.StructType

private[spinach] class FiberDataWriter(
    isCompressed: Boolean,
    out: FSDataOutputStream,
    schema: StructType) extends RecordWriter[NullWritable, InternalRow] {
  // TODO: make the fiber size configurable
  private final val DEFAULT_ROW_GROUP_SIZE = 1024
  private var rowCount: Int = 0
  private var rowGroupCount: Int = 0

  private val rowGroup: Array[FiberBuilder] =
    FiberBuilder.initializeFromSchema(schema, DEFAULT_ROW_GROUP_SIZE)

  private val fiberMeta = new SpinachSplitMeta(
    rowCountInEachGroup = DEFAULT_ROW_GROUP_SIZE, fieldCount = schema.length)

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
      val fiberData = rowGroup(idx).build()
      totalDataSize += fiberData.buf.length
      fiberLens(idx) = fiberData.buf.length
      out.write(fiberData.buf)
      rowGroup(idx).clear()
      idx += 1
    }

    fiberMeta.appendRowGroupMeta(rowGroupMeta.withNewEnd(out.getPos))
  }

  override def close(context: TaskAttemptContext) {
    if (rowCount % DEFAULT_ROW_GROUP_SIZE != 0) {
      // should be end of the insertion, put the row groups into the last row group
      writeRowGroup()
    }

    // and update the group count and row count in the last group
    fiberMeta
      .withGroupCount(rowGroupCount)
      .withRowCountInLastGroup(rowCount % DEFAULT_ROW_GROUP_SIZE)

    fiberMeta.write(out)
    out.close()
  }
}

private[spinach] class FiberDataReader(
    path: Path,
    schema: StructType,
    requiredIds: Array[Int]) extends RecordReader[NullWritable, InternalRow] {

  private var totalRowCount: Int = 0
  private var currentRowId: Int = 0
  private var currentRowIter: Iterator[InternalRow] = _
  private var currentRow: InternalRow = _

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val tmpDF = DataFile(path.toString, context)
    val in = FiberDataFileHandler(tmpDF)
    val meta = new SpinachSplitMeta().read(in, split.getLength)

    totalRowCount = meta.totalRowCount()
    currentRowIter = new FiberReader(tmpDF.withMeta(meta), schema, requiredIds).iterator()
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
