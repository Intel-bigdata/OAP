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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.StringUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.spinach.{BatchColumn, ColumnValues}
import org.apache.spark.sql.execution.datasources.spinach.filecache._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform


private[spinach] case class SpinachDataFile(path: String, schema: StructType) extends DataFile {

  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): DataFiberCache = {
    val meta: SpinachDataFileHandle = DataFileHandleCacheManager(this, conf)
    val groupMeta = meta.rowGroupsMeta(groupId)
    // get the fiber data start position
    // TODO: update the meta to store the fiber start pos
    var i = 0
    var fiberStart = groupMeta.start
    while (i < fiberId) {
      fiberStart += groupMeta.fiberLens(i)
      i += 1
    }
    val len = groupMeta.fiberLens(fiberId)
    val bytes = new Array[Byte](len)

    val is = meta.fin
    is.synchronized {
      is.seek(fiberStart)
      is.readFully(bytes)
      putToFiberCache(bytes)
    }
  }

  private def putToFiberCache(buf: Array[Byte]): DataFiberCache = {
    // TODO: make it configurable
    // TODO: disable compress first since there's some issue to solve with conpression
    val fiberCacheData = MemoryManager.allocate(buf.length)
    Platform.copyMemory(buf, Platform.BYTE_ARRAY_OFFSET, fiberCacheData.fiberData.getBaseObject,
      fiberCacheData.fiberData.getBaseOffset, buf.length)
    fiberCacheData
  }

  // full file scan
  def iterator(conf: Configuration, requiredIds: Array[Int]): Iterator[InternalRow] = {
    val meta: SpinachDataFileHandle = DataFileHandleCacheManager(this, conf)
    val row = new BatchColumn()
    val columns: Array[ColumnValues] = new Array[ColumnValues](requiredIds.length)
    (0 until meta.groupCount).iterator.flatMap { groupId =>
      var i = 0
      while (i < columns.length) {
        columns(i) = new ColumnValues(
          meta.rowCountInEachGroup,
          schema(requiredIds(i)).dataType,
          FiberCacheManager(DataFiber(this, requiredIds(i), groupId), conf))
        i += 1
      }

      if (groupId < meta.groupCount - 1) {
        // not the last row group
        row.reset(meta.rowCountInEachGroup, columns).toIterator
      } else {
        row.reset(meta.rowCountInLastGroup, columns).toIterator
      }
    }
  }

  // scan by given row ids, and we assume the rowIds are sorted
  def iterator(conf: Configuration, requiredIds: Array[Int], rowIds: Array[Long])
  : Iterator[InternalRow] = {
    val meta: SpinachDataFileHandle = DataFileHandleCacheManager(this, conf)
    val row = new BatchColumn()
    val columns: Array[ColumnValues] = new Array[ColumnValues](requiredIds.length)
    var lastGroupId = -1
    (0 until rowIds.length).iterator.map { idx =>
      val rowId = rowIds(idx)
      val groupId = ((rowId + 1) / meta.rowCountInEachGroup).toInt
      val rowIdxInGroup = (rowId % meta.rowCountInEachGroup).toInt

      if (lastGroupId != groupId) {
        // if we move to another row group, or the first row group
        var i = 0
        while (i < columns.length) {
          columns(i) = new ColumnValues(
            meta.rowCountInEachGroup,
            schema(requiredIds(i)).dataType,
            FiberCacheManager(DataFiber(this, requiredIds(i), groupId), conf))
          i += 1
        }
        if (groupId < meta.groupCount - 1) {
          // not the last row group
          row.reset(meta.rowCountInEachGroup, columns)
        } else {
          row.reset(meta.rowCountInLastGroup, columns)
        }

        lastGroupId = groupId
      }

      row.moveToRow(rowIdxInGroup)
    }
  }

  override def createDataFileHandle(conf: Configuration): SpinachDataFileHandle = {
    val p = new Path(StringUtils.unEscapeString(path))

    val fs = p.getFileSystem(conf)

    new SpinachDataFileHandle().read(fs.open(p), fs.getFileStatus(p).getLen)
  }
}
