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

import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.vectorized.OapOnHeapColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

class ParquetDataFaultFiberReader(fiberCache: FiberCache, dataType: DataType, total: Int)
  extends ParquetDataFiberReader(
    address = fiberCache.getBaseOffset,
    dataType = dataType,
    total = total) {
  require(fiberCache.getColumn() != null)

  override def readBatch(
    start: Int, num: Int, column: OapOnHeapColumnVector): Unit = if (dictionary != null) {
    column.setDictionary(dictionary)
    val dictionaryIds = column.reserveDictionaryIds(num).asInstanceOf[OapOnHeapColumnVector]
    header match {
      case ParquetDataFiberHeader(true, false, _) =>
        Platform.copyMemory(
          fiberCache.getColumn().getDictionaryIds.asInstanceOf[OapOnHeapColumnVector].getIntData,
          Platform.INT_ARRAY_OFFSET + start * 4L,
          dictionaryIds.getIntData, Platform.INT_ARRAY_OFFSET, num * 4L)
      case ParquetDataFiberHeader(false, false, _) =>
        Platform.copyMemory(
          fiberCache.getColumn().getNulls,
          Platform.BYTE_ARRAY_OFFSET + start, column.getNulls, Platform.BYTE_ARRAY_OFFSET, num)
        Platform.copyMemory(
          fiberCache.getColumn().getDictionaryIds.asInstanceOf[OapOnHeapColumnVector].getIntData,
          Platform.INT_ARRAY_OFFSET + start * 4L,
          dictionaryIds.getIntData, Platform.INT_ARRAY_OFFSET, num * 4L)
      case ParquetDataFiberHeader(false, true, _) =>
        // can to this branch ?
        column.putNulls(0, num)
      case ParquetDataFiberHeader(true, true, _) =>
        throw new OapException("error header status (true, true, _)")
      case other => throw new OapException(s"impossible header status $other.")
    }
  } else {
    column.setDictionary(null)
    header match {
      case ParquetDataFiberHeader(true, false, _) =>
        readBatchFromColumn(start, num, column)
      case ParquetDataFiberHeader(false, false, _) =>
        Platform.copyMemory(fiberCache.getColumn().getNulls,
          Platform.BYTE_ARRAY_OFFSET + start, column.getNulls, Platform.BYTE_ARRAY_OFFSET, num)
        readBatchFromColumn(start, num, column)
      case ParquetDataFiberHeader(false, true, _) =>
        column.putNulls(0, num)
      case ParquetDataFiberHeader(true, true, _) =>
        throw new OapException("error header status (true, true, _)")
      case other => throw new OapException(s"impossible header status $other.")
    }
  }

  override def readBatch(rowIdList: IntList, column: OapOnHeapColumnVector): Unit = {
    throw new OapException("Read with row id list is not supported.")
  }

  private def readBatchFromColumn(
    start: Int, num: Int, column: OapOnHeapColumnVector): Unit = {
    val originalColumn = fiberCache.getColumn();

    def readBinaryToColumnVector(): Unit = {
      Platform.copyMemory(originalColumn.getArrayLengths,
        Platform.INT_ARRAY_OFFSET + start * 4L,
        column.getArrayLengths, Platform.INT_ARRAY_OFFSET, num * 4L)
      Platform.copyMemory(
        originalColumn.getArrayOffsets,
        Platform.INT_ARRAY_OFFSET + start * 4L,
        column.getArrayOffsets, Platform.INT_ARRAY_OFFSET, num * 4L)

      var lastIndex = num - 1
      while (lastIndex >= 0 && column.isNullAt(lastIndex)) {
        lastIndex -= 1
      }
      var firstIndex = 0
      while (firstIndex < num && column.isNullAt(firstIndex)) {
        firstIndex += 1
      }
      if (firstIndex < num && lastIndex >= 0) {
        val arrayOffsets: Array[Int] = column.getArrayOffsets
        val startOffset = arrayOffsets(firstIndex)
        for (idx <- firstIndex to lastIndex) {
          if (!column.isNullAt(idx)) {
            arrayOffsets(idx) -= startOffset
          }
        }

        val length = column.getArrayOffset(lastIndex) -
          column.getArrayOffset(firstIndex) + column.getArrayLength(lastIndex)

        val data = new Array[Byte](length)
        val child = originalColumn.getChild(0).asInstanceOf[OapOnHeapColumnVector]
        Platform.copyMemory(child.getByteData,
          Platform.BYTE_ARRAY_OFFSET + startOffset,
          data, Platform.BYTE_ARRAY_OFFSET, data.length)
        column.getChild(0).asInstanceOf[OapOnHeapColumnVector].setByteData(data)
      }
    }

    dataType match {
      case ByteType | BooleanType =>
        Platform.copyMemory(originalColumn.getByteData,
          Platform.BYTE_ARRAY_OFFSET + start, column.getByteData, Platform.BYTE_ARRAY_OFFSET, num)
      case ShortType =>
        Platform.copyMemory(originalColumn.getShortData,
          Platform.SHORT_ARRAY_OFFSET + start * 2L,
          column.getShortData, Platform.SHORT_ARRAY_OFFSET, num * 2L)
      case IntegerType | DateType =>
        Platform.copyMemory(originalColumn.getIntData,
          Platform.INT_ARRAY_OFFSET + start * 4L,
          column.getIntData, Platform.INT_ARRAY_OFFSET, num * 4L)
      case FloatType =>
        Platform.copyMemory(originalColumn.getFloatData,
          Platform.FLOAT_ARRAY_OFFSET + start * 4L,
          column.getFloatData, Platform.FLOAT_ARRAY_OFFSET, num * 4L)
      case LongType | TimestampType =>
        Platform.copyMemory(originalColumn.getLongData,
          Platform.LONG_ARRAY_OFFSET + start * 8L,
          column.getLongData, Platform.LONG_ARRAY_OFFSET, num * 8L)
      case DoubleType =>
        Platform.copyMemory(
          originalColumn.getDoubleData, Platform.DOUBLE_ARRAY_OFFSET + start * 8L,
          column.getDoubleData, Platform.DOUBLE_ARRAY_OFFSET, num * 8L)
      case BinaryType | StringType => readBinaryToColumnVector()
      // if DecimalType.is32BitDecimalType(other) as int data type.
      case other if DecimalType.is32BitDecimalType(other) =>
        Platform.copyMemory(originalColumn.getIntData,
          Platform.INT_ARRAY_OFFSET + start * 4L,
          column.getIntData, Platform.INT_ARRAY_OFFSET, num * 4L)
      // if DecimalType.is64BitDecimalType(other) as long data type.
      case other if DecimalType.is64BitDecimalType(other) =>
        Platform.copyMemory(originalColumn.getLongData,
          Platform.LONG_ARRAY_OFFSET + start * 8L,
          column.getLongData, Platform.LONG_ARRAY_OFFSET, num * 8L)
      // if DecimalType.isByteArrayDecimalType(other) as binary data type.
      case other if DecimalType.isByteArrayDecimalType(other) => readBinaryToColumnVector()
      case other => throw new OapException(s"impossible data type $other.")
    }
  }

  override def readRowGroupMetas(): Unit = {
    header = ParquetDataFiberHeader(fiberCache.getColumn(), fiberCache.getColumn().getCapacity)
    header match {
      case ParquetDataFiberHeader(_, _, 0) =>
        dictionary = null
      case ParquetDataFiberHeader(false, true, _) =>
        dictionary = null
      case ParquetDataFiberHeader(true, false, dicLength) =>
        dictionary = fiberCache.getColumn().getDictionary
      case ParquetDataFiberHeader(false, false, dicLength) =>
        dictionary = fiberCache.getColumn().getDictionary
      case ParquetDataFiberHeader(true, true, _) =>
        throw new OapException("error header status (true, true, _)")
      case other => throw new OapException(s"impossible header status $other.")
    }
  }
}

object ParquetDataFaultFiberReader {
  def apply(fiberCache: FiberCache, dataType: DataType, total: Int): ParquetDataFiberReader = {
    val reader = new ParquetDataFaultFiberReader(fiberCache, dataType, total)
    reader.readRowGroupMetas()
    reader
  }
}
