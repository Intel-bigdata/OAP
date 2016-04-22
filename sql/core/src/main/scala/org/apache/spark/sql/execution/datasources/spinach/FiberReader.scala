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

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.datasources.spinach.fiber.SpinachSplitMeta
import org.apache.spark.sql.types.{DataType, Decimal, StructType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

private[spinach] case class Fiber(file: DataFile, columnIndex: Int, rowGroupId: Int) {
  def newGroupId(id: Int): Fiber = Fiber(file, columnIndex, id)
}

private[spinach] case class DataFile(
    path: String, context: TaskAttemptContext, meta: SpinachSplitMeta = null) {
  override def hashCode(): Int = path.hashCode
  override def equals(that: Any): Boolean = that match {
    case DataFile(thatPath, _, _) => path == thatPath
    case _ => false
  }

  def withMeta(newMeta: SpinachSplitMeta): DataFile = {
    DataFile(path, context, newMeta)
  }

  def getFiber(
    is: FSDataInputStream,
    groupId: Int,
    fiberId: Int): FiberByteData = {
    val groupMeta = meta.rowGroupsMeta(groupId)
    // get the fiber data start position TODO update the meta to store the fiber start pos
    var i = 0
    var fiberStart = groupMeta.start
    while (i < fiberId) {
      fiberStart += groupMeta.fiberLens(i)
      i += 1
    }
    val lens = groupMeta.fiberLens(fiberId)
    val bytes = new Array[Byte](lens)

    is.synchronized {
      is.seek(fiberStart)
      is.read(bytes)
    }
    new FiberByteData(bytes)
  }
}

private[spinach] case class FiberReader(
    df: DataFile, schema: StructType, requiredIds: Array[Int]) {
  private val columns: Array[ColumnValues] = new Array[ColumnValues](requiredIds.length)

  def iterator(): Iterator[InternalRow] = {
    val row = new BatchRow()
    (0 until df.meta.groupCount).iterator.flatMap { groupId =>
      val fibers: Array[Fiber] = requiredIds.map(i => Fiber(df, requiredIds(i), groupId))
      var i = 0
      while (i < columns.length) {
        columns(i) = new ColumnValues(
                        df.meta.rowCountInEachGroup,
                        schema(requiredIds(i)).dataType,
                        FiberCacheManager(fibers(i)))
        i += 1
      }
      if (groupId < df.meta.groupCount - 1) {
        // not the last row group
        row.reset(df.meta.rowCountInEachGroup, columns).toIterator
      } else {
        row.reset(df.meta.rowCountInLastGroup, columns).toIterator
      }
    }
  }
}
