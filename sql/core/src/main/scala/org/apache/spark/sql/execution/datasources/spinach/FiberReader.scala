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

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal, StructType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

private[spinach] case class Fiber(file: DataFile, columnIndex: Int, rowGroupId: Int) {
  def newGroupId(id: Int): Fiber = Fiber(file, columnIndex, id)
}

private[spinach] case class DataFile(path: String, context: TaskAttemptContext) {
  override def hashCode(): Int = path.hashCode
  override def equals(that: Any): Boolean = that match {
    case DataFile(thatPath, _) => path.equals(thatPath)
    case _ => false
  }
}

private[spinach] case class ColumnarBatch(
  count: Int, columns: Array[ColumnValues], schema: StructType) {

  def rowIterator(): Iterator[InternalRow] = {
    object Row extends InternalRow {
      var rowId = -1
      override def numFields: Int = schema.fields.length
      override def anyNull: Boolean = columns.exists(_.isNullAt(rowId))
      override def copy(): InternalRow = throw new NotImplementedError("")
      override def get(ordinal: Int, dataType: DataType)
      : AnyRef = throw new NotImplementedError("")
      override def getUTF8String(ordinal: Int): UTF8String = columns(ordinal).getString(rowId)
      override def getBinary(ordinal: Int): Array[Byte] = columns(ordinal).getBinary(rowId)
      override def getDouble(ordinal: Int): Double = columns(ordinal).getDouble(rowId)
      override def getArray(ordinal: Int): ArrayData = throw new NotImplementedError("")
      override def getInterval(ordinal: Int): CalendarInterval = throw new NotImplementedError("")
      override def getFloat(ordinal: Int): Float = columns(ordinal).getFloatValue(rowId)
      override def getLong(ordinal: Int): Long = columns(ordinal).getLong(rowId)
      override def getMap(ordinal: Int): MapData = throw new NotImplementedError("")
      override def getByte(ordinal: Int): Byte = columns(ordinal).getByteValue(rowId)
      override def getDecimal(ordinal: Int, precision: Int, scale: Int)
      : Decimal = throw new NotImplementedError("")
      override def getBoolean(ordinal: Int): Boolean = columns(ordinal).getBooleanValue(rowId)
      override def getShort(ordinal: Int): Short = columns(ordinal).getShortValue(rowId)
      override def getStruct(ordinal: Int, numFields: Int)
      : InternalRow = throw new NotImplementedError("")
      override def getInt(ordinal: Int): Int = columns(ordinal).getInt(rowId)
      override def isNullAt(ordinal: Int): Boolean = columns(ordinal).isNullAt(rowId)
    }

    new Iterator[InternalRow]() {
      override def hasNext: Boolean =
        (Row.rowId < ColumnarBatch.this.count && ColumnarBatch.this.count > 0)

      override def next(): InternalRow = {
        Row.rowId = Row.rowId + 1
        Row
      }
    }
  }
}

private[spinach] case class DataMeta(
  file: DataFile, groups: Int, rowsPerGroup: Int, rowsInLastGroup: Int, totalCols: Int) {

  private def getFibers(schema: StructType, fibers: Array[Fiber]): Array[ColumnValues] = {
    fibers.zip(schema.fields).map { case (f, s) =>
      new ColumnValues(s.dataType, FiberCacheManager(f))
    }
  }

  def fiberGroupIterator(schema: StructType, requiredIds: Array[Int])
  : Iterator[ColumnarBatch] = { // TODO return the Array[ColumnValues]?
    var groupId = 0
    val caches = requiredIds.map( id => new Fiber(file, id, groupId))

    new Iterator[ColumnarBatch]() {
      override def hasNext: Boolean = groupId < groups
      override def next(): ColumnarBatch = {
        caches.foreach(_.newGroupId(groupId))

        val columns = getFibers(schema, caches)
        groupId += 1

        if (groupId < groups) {
          // if not the last group
          ColumnarBatch(rowsPerGroup, columns, schema)
        } else {
          // last groups
          ColumnarBatch(rowsInLastGroup, columns, schema)
        }
      }
    }
  }
}
