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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, FiberId}
import org.apache.spark.sql.execution.datasources.oap.io.ParquetDataFiberWriter.{dumpDataAndDicToFiber, dumpDataToFiber, dumpNullsToFiber, emptyDataFiber, fiberLength, logDebug}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.unsafe.Platform

object OrcDataFiberWriter extends Logging{
  // orc dumpToCache
  def orcDumpToCache(column: OnHeapColumnVector, total: Int,
                     fiberId: FiberId = null): FiberCache = {
    val header = ParquetDataFiberHeader(column, total)
    logDebug(s"will dump column to data fiber dataType = ${column.dataType()}, " +
      s"total = $total, header is $header")
    header match {
      case ParquetDataFiberHeader(true, false, 0) =>
        val length = fiberLength(column, total, 0 )
        logDebug(s"will apply $length bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(length, fiberId)
        if (!fiber.isFailedMemoryBlock()) {
          val nativeAddress = header.writeToCache(fiber.getBaseOffset)
          dumpDataToFiber(nativeAddress, column, total)
        } else {
          fiber.setColumn(column)
        }
        fiber
      case ParquetDataFiberHeader(true, false, dicLength) =>
        val length = fiberLength(column, total, 0, dicLength)
        logDebug(s"will apply $length bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(length, fiberId)
        if (!fiber.isFailedMemoryBlock()) {
          val nativeAddress = header.writeToCache(fiber.getBaseOffset)
          dumpDataAndDicToFiber(nativeAddress, column, total, dicLength)
        } else {
          fiber.setColumn(column)
        }
        fiber
      case ParquetDataFiberHeader(false, true, _) =>
        logDebug(s"will apply ${ParquetDataFiberHeader.defaultSize} " +
          s"bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(ParquetDataFiberHeader.defaultSize, fiberId)
        if (!fiber.isFailedMemoryBlock()) {
          header.writeToCache(fiber.getBaseOffset)
        } else {
          fiber.setColumn(column)
        }
        fiber
      case ParquetDataFiberHeader(false, false, 0) =>
        val length = fiberLength(column, total, 1)
        logDebug(s"will apply $length bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(length, fiberId)
        if (!fiber.isFailedMemoryBlock()) {
          val nativeAddress =
            dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset), column, total)
          dumpDataToFiber(nativeAddress, column, total)
        } else {
          fiber.setColumn(column)
        }
        fiber
      case ParquetDataFiberHeader(false, false, dicLength) =>
        val length = fiberLength(column, total, 1, dicLength)
        logDebug(s"will apply $length bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(length, fiberId)
        if (!fiber.isFailedMemoryBlock()) {
          val nativeAddress =
            dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset), column, total)
          dumpDataAndDicToFiber(nativeAddress, column, total, dicLength)
        } else {
          fiber.setColumn(column)
        }
        fiber
      case ParquetDataFiberHeader(true, true, _) =>
        throw new OapException("impossible header status (true, true, _).")
      case other => throw new OapException(s"impossible header status $other.")
    }
  }


  /**
   * TODO following functions are duplicate with those in ParquetDataFiberReaderWriter.scala
   * TODO code refactor
   */

  /**
   * Write nulls data to data fiber.
   */
  private def dumpNullsToFiber(
                                nativeAddress: Long, column: OnHeapColumnVector, total: Int): Long = {
    Platform.copyMemory(column.getNulls,
      Platform.BYTE_ARRAY_OFFSET, null, nativeAddress, total)
    nativeAddress + total
  }

  /**
   * noNulls is true, nulls are all 0, not dump nulls to cache,
   * allNulls is false, need dump to cache,
   * dicLength is 0, needn't calculate dictionary part.
   */
  private def dumpDataToFiber(nativeAddress: Long, column: OnHeapColumnVector, total: Int): Unit = {
    column.dataType match {
      case ByteType | BooleanType =>
        Platform.copyMemory(column.getByteData,
          Platform.BYTE_ARRAY_OFFSET, null, nativeAddress, total)
      case ShortType =>
        Platform.copyMemory(column.getShortData,
          Platform.SHORT_ARRAY_OFFSET, null, nativeAddress, total * 2L)
      case IntegerType | DateType =>
        Platform.copyMemory(column.getIntData,
          Platform.INT_ARRAY_OFFSET, null, nativeAddress, total * 4L)
      case FloatType =>
        Platform.copyMemory(column.getFloatData,
          Platform.FLOAT_ARRAY_OFFSET, null, nativeAddress, total * 4L)
      case LongType | TimestampType =>
        Platform.copyMemory(column.getLongData,
          Platform.LONG_ARRAY_OFFSET, null, nativeAddress, total * 8L)
      case DoubleType =>
        Platform.copyMemory(column.getDoubleData,
          Platform.DOUBLE_ARRAY_OFFSET, null, nativeAddress, total * 8L)
      case StringType | BinaryType =>
        Platform.copyMemory(column.getArrayLengths,
          Platform.INT_ARRAY_OFFSET, null, nativeAddress, total * 4L)
        Platform.copyMemory(column.getArrayOffsets,
          Platform.INT_ARRAY_OFFSET, null, nativeAddress + total * 4L, total * 4L)
        val child = column.getChild(0).asInstanceOf[OnHeapColumnVector]
        Platform.copyMemory(child.getByteData,
          Platform.BYTE_ARRAY_OFFSET, null, nativeAddress + total * 8L,
          child.getElementsAppended)
      case other if DecimalType.is32BitDecimalType(other) =>
        Platform.copyMemory(column.getIntData,
          Platform.INT_ARRAY_OFFSET, null, nativeAddress, total * 4L)
      case other if DecimalType.is64BitDecimalType(other) =>
        Platform.copyMemory(column.getLongData,
          Platform.LONG_ARRAY_OFFSET, null, nativeAddress, total * 8L)
      case other if DecimalType.isByteArrayDecimalType(other) =>
        Platform.copyMemory(column.getArrayLengths,
          Platform.INT_ARRAY_OFFSET, null, nativeAddress, total * 4L)
        Platform.copyMemory(column.getArrayOffsets,
          Platform.INT_ARRAY_OFFSET, null, nativeAddress + total * 4L, total * 4L)
        val child = column.getChild(0).asInstanceOf[OnHeapColumnVector]
        Platform.copyMemory(child.getByteData,
          Platform.BYTE_ARRAY_OFFSET, null, nativeAddress + total * 8L,
          child.getElementsAppended)
      case other => throw new OapException(s"$other data type is not support data cache.")
    }
  }

  /**
   * Write dictionaryIds(int array) and Dictionary data to data fiber.
   */
  private def dumpDataAndDicToFiber(
      nativeAddress: Long, column: OnHeapColumnVector, total: Int, dicLength: Int): Unit = {
    // dump dictionaryIds to data fiber, it's a int array.
    val dictionaryIds = column.getDictionaryIds.asInstanceOf[OnHeapColumnVector]
    Platform.copyMemory(dictionaryIds.getIntData,
      Platform.INT_ARRAY_OFFSET, null, nativeAddress, total * 4L)
    var dicNativeAddress = nativeAddress + total * 4L
    val dictionary = column.getDictionary
    // dump dictionary to data fiber case by dataType.
    column.dataType() match {
      case ByteType | ShortType | IntegerType | DateType =>
        val intDictionaryContent = new Array[Int](dicLength)
        (0 until dicLength).foreach(id => intDictionaryContent(id) = dictionary.decodeToInt(id))
        Platform.copyMemory(intDictionaryContent, Platform.INT_ARRAY_OFFSET, null,
          dicNativeAddress, dicLength * 4L)
      case FloatType =>
        val floatDictionaryContent = new Array[Float](dicLength)
        (0 until dicLength).foreach(id => floatDictionaryContent(id) = dictionary.decodeToFloat(id))
        Platform.copyMemory(floatDictionaryContent, Platform.FLOAT_ARRAY_OFFSET, null,
          dicNativeAddress, dicLength * 4L)
      case LongType | TimestampType =>
        val longDictionaryContent = new Array[Long](dicLength)
        (0 until dicLength).foreach(id => longDictionaryContent(id) = dictionary.decodeToLong(id))
        Platform.copyMemory(longDictionaryContent, Platform.LONG_ARRAY_OFFSET, null,
          dicNativeAddress, dicLength * 8L)
      case DoubleType =>
        val doubleDictionaryContent = new Array[Double](dicLength)
        (0 until dicLength).foreach(id =>
          doubleDictionaryContent(id) = dictionary.decodeToDouble(id))
        Platform.copyMemory(doubleDictionaryContent, Platform.DOUBLE_ARRAY_OFFSET, null,
          dicNativeAddress, dicLength * 8L);
      case StringType | BinaryType =>
        var bytesNativeAddress = dicNativeAddress + 4L * dicLength
        (0 until dicLength).foreach( id => {
          val binary = dictionary.decodeToBinary(id)
          val length = binary.length
          Platform.putInt(null, dicNativeAddress, length)
          dicNativeAddress += 4
          Platform.copyMemory(binary,
            Platform.BYTE_ARRAY_OFFSET, null, bytesNativeAddress, length)
          bytesNativeAddress += length
        })
      case other if DecimalType.is32BitDecimalType(other) =>
        val intDictionaryContent = new Array[Int](dicLength)
        (0 until dicLength).foreach(id => intDictionaryContent(id) = dictionary.decodeToInt(id))
        Platform.copyMemory(intDictionaryContent, Platform.INT_ARRAY_OFFSET, null,
          dicNativeAddress, dicLength * 4L)
      case other if DecimalType.is64BitDecimalType(other) =>
        val longDictionaryContent = new Array[Long](dicLength)
        (0 until dicLength).foreach(id => longDictionaryContent(id) = dictionary.decodeToLong(id))
        Platform.copyMemory(longDictionaryContent, Platform.LONG_ARRAY_OFFSET, null,
          dicNativeAddress, dicLength * 8L)
      case other if DecimalType.isByteArrayDecimalType(other) =>
        var bytesNativeAddress = dicNativeAddress + 4L * dicLength
        (0 until dicLength).foreach( id => {
          val binary = dictionary.decodeToBinary(id)
          val length = binary.length
          Platform.putInt(null, dicNativeAddress, length)
          dicNativeAddress += 4
          Platform.copyMemory(binary,
            Platform.BYTE_ARRAY_OFFSET, null, bytesNativeAddress, length)
          bytesNativeAddress += length
        })
      case other => throw new OapException(s"$other data type is not support dictionary.")
    }
  }

  /**
   * noNulls is true, nulls are all 0, not dump nulls to cache, nullsLength is 0,
   * allNulls is false, need dump to cache,
   * dicLength is 0, needn't calculate dictionary part.
   */
  private def fiberLength(column: OnHeapColumnVector, total: Int, nullUnitLength: Int): Long =
    if (isFixedLengthDataType(column.dataType())) {
      logDebug(s"dataType ${column.dataType()} is fixed length. ")
      // Fixed length data type fiber length.
      ParquetDataFiberHeader.defaultSize +
        nullUnitLength * total + column.dataType().defaultSize.toLong * total
    } else {
      logDebug(s"dataType ${column.dataType()} is not fixed length. ")
      // lengthData and offsetData will be set and data will be put in child if type is Array.
      // lengthData: 4 bytes, offsetData: 4 bytes, nulls: 1 byte,
      // child.data: childColumns[0].elementsAppended bytes.
      ParquetDataFiberHeader.defaultSize + nullUnitLength * total + total * 8L +
        column.getChild(0).getElementsAppended
    }

  /**
   * noNulls is true, nulls are all 0, not dump nulls to cache, nullsLength is 0,
   * allNulls is false, need dump to cache,
   * dicLength is not, need calculate dictionary part and dictionaryIds is a int array.
   */
  private def fiberLength(
      column: OnHeapColumnVector, total: Int, nullUnitLength: Int, dicLength: Int): Long = {
    val dicPartSize = column.dataType() match {
      case ByteType | ShortType | IntegerType | DateType => dicLength * 4L
      case FloatType => dicLength * 4L
      case LongType | TimestampType => dicLength * 8L
      case DoubleType => dicLength * 8L
      case StringType | BinaryType =>
        val dictionary = column.getDictionary
        (0 until dicLength).map(id => dictionary.decodeToBinary(id).length + 4L).sum
      // if DecimalType.is32BitDecimalType(other) as int data type
      case other if DecimalType.is32BitDecimalType(other) => dicLength * 4L
      // if DecimalType.is64BitDecimalType(other) as long data type
      case other if DecimalType.is64BitDecimalType(other) => dicLength * 8L
      // if DecimalType.isByteArrayDecimalType(other) as binary data type
      case other if DecimalType.isByteArrayDecimalType(other) =>
        val dictionary = column.getDictionary
        (0 until dicLength).map(id => dictionary.decodeToBinary(id).length + 4L).sum
      case other => throw new OapException(s"$other data type is not support dictionary.")
    }
    ParquetDataFiberHeader.defaultSize + nullUnitLength * total + 4L * total + dicPartSize
  }

  private def isFixedLengthDataType(dataType: DataType): Boolean = dataType match {
    case StringType | BinaryType => false
    case ByteType | BooleanType | ShortType |
         IntegerType | DateType | FloatType |
         LongType | DoubleType | TimestampType => true
    // if DecimalType.is32BitDecimalType(other) as int data type,
    // if DecimalType.is64BitDecimalType(other) as long data type,
    // so they are fixed length
    case other if DecimalType.is32BitDecimalType(other) ||
      DecimalType.is64BitDecimalType(other) => true
    // if DecimalType.isByteArrayDecimalType(other) as binary data type,
    // so it's not fixed length
    case other if DecimalType.isByteArrayDecimalType(other) => false
    case other => throw new OapException(s"$other data type is not implemented for cache.")
  }

  private def emptyDataFiber(fiberLength: Long, fiberId: FiberId = null): FiberCache =
    OapRuntime.getOrCreate.fiberCacheManager.getEmptyDataFiberCache(fiberLength, fiberId)

}
