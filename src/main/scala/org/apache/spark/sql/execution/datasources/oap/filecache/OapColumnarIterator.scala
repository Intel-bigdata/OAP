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

package org.apache.spark.sql.execution.datasources.oap.filecache

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.execution.columnar.{CachedBatch, MutableUnsafeRow}
import org.apache.spark.sql.execution.datasources.oap.ColumnValues
import org.apache.spark.sql.execution.datasources.oap.io.DataFile
import org.apache.spark.sql.types.{DataType, LongType, StructType}

abstract class OapColumnarIterator extends Iterator[InternalRow] {
  def initialize(
      dataFile: DataFile, columnTypes: Array[DataType], groupCount: Int,
      rowCountInEachGroup: Int, rowCountInLastGroup: Int): Unit
}

abstract class OapColumnarIterator2 extends Iterator[InternalRow] {
  def initialize(
      schema: StructType,
      dataFile: DataFile, groupCount: Int,
      rowCountInEachGroup: Int, rowCountInLastGroup: Int): Unit
}

class SpecificColumnarIterator extends OapColumnarIterator2 {

  private var dataFile: DataFile = _
  private var buffers: Seq[ColumnValues] = _
  private var configuration: Configuration = _

  private var numFields: Int = _
  private var schema: StructType = _
  private var groupCount: Int = _
  private var rowCount: Long = _
  private var rowCountInGroup: Int = _

  private var currentGroupId = 0
  private var currentRowId = 0L
  private var currentRowIdInGroup = 0

  private var unsafeRow: UnsafeRow = _
  private var bufferHolder: BufferHolder = _
  private var rowWriter: UnsafeRowWriter = _
  private var mutableRow: MutableUnsafeRow = _

  override def initialize(
      schema: StructType,
      dataFile: DataFile,
      groupCount: Int,
      rowCountInEachGroup: Int,
      rowCountInLastGroup: Int): Unit = {

    this.numFields = schema.length
    this.schema = schema
    this.groupCount = groupCount
    this.rowCount = (groupCount - 1).toLong * rowCountInEachGroup + rowCountInLastGroup
    this.rowCountInGroup = rowCountInEachGroup

    this.unsafeRow = new UnsafeRow(1)
    this.bufferHolder = new BufferHolder(unsafeRow)
    this.rowWriter = new UnsafeRowWriter(bufferHolder, 1)
    this.mutableRow = new MutableUnsafeRow(rowWriter)

    this.dataFile = dataFile
    this.configuration = configuration
    // this.buffers = schema.zipWithIndex.map(i =>
    //   new ColumnValues(rowCountInGroup, i._1.dataType,
    //     FiberCacheManager.get(DataFiber(dataFile, i._2, currentGroupId), configuration)))
    this.buffers = Seq(new ColumnValues(rowCountInGroup, LongType,
      FiberCacheManager.get(DataFiber(dataFile, 0, currentGroupId), configuration)))
  }

  override def hasNext: Boolean = {
    if (currentRowId < rowCount) {
      if (currentRowIdInGroup == rowCountInGroup) {
        currentGroupId += 1
        currentRowIdInGroup = 0
        // buffers = schema.zipWithIndex.map(i =>
        //   new ColumnValues(rowCountInGroup, i._1.dataType,
        //     FiberCacheManager.get(DataFiber(dataFile, i._2, currentGroupId), configuration))
        // )
        buffers = Seq(new ColumnValues(rowCountInGroup, LongType,
          FiberCacheManager.get(DataFiber(dataFile, 0, currentGroupId), configuration)))
      }
      true
    } else {
      false
    }
  }

  override def next(): InternalRow = {

    bufferHolder.reset()
    rowWriter.zeroOutNullBytes()
    mutableRow.setLong(0, buffers.head.getLongValue(currentRowId.toInt))
    unsafeRow.setTotalSize(bufferHolder.totalSize())

    currentRowId += 1
    currentRowIdInGroup += 1
    unsafeRow
  }
}
