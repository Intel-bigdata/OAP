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

import org.apache.parquet.column.{Dictionary, Encoding}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.parquet.ParquetDictionaryWrapper
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

/**
 * ParquetDataFiberWriter is a util use to write OnHeapColumnVector data to data fiber.
 * Data Fiber write as follow format:
 * ParquetDataFiberHeader: (noNulls:boolean:1 bit, allNulls:boolean:1 bit, dicLength:int:4 bit)
 * NullsData: (noNulls:false, allNulls: false) will store nulls to data fiber as bytes array
 * Values: store value data except (noNulls:false, allNulls: true) by dataType,
 * Dic encode data store as int array.
 * Dictionary: if dicLength > 0 will store dic data by dataType.
 */
object ParquetDataFiberWriter extends Logging {

  def dumpToCache(column: OnHeapColumnVector, total: Int): FiberCache = {
    val header = ParquetDataFiberHeader(column, total)
    logDebug(s"will dump column to data fiber dataType = ${column.dataType()}, " +
      s"total = $total, header is $header")
    header match {
      case ParquetDataFiberHeader(true, false, 0) =>
        val length = fiberLength(column, total, 0 )
        logDebug(s"will apply $length bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(length)
        val nativeAddress = header.writeToCache(fiber.getBaseOffset)
        dumpDataToFiber(nativeAddress, column, total)
        fiber
      case ParquetDataFiberHeader(true, false, dicLength) =>
        val length = fiberLength(column, total, 0, dicLength)
        logDebug(s"will apply $length bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(length)
        val nativeAddress = header.writeToCache(fiber.getBaseOffset)
        dumpDataAndDicToFiber(nativeAddress, column, total, dicLength)
        fiber
      case ParquetDataFiberHeader(false, true, _) =>
        logDebug(s"will apply ${ParquetDataFiberHeader.defaultSize} " +
          s"bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(ParquetDataFiberHeader.defaultSize)
        header.writeToCache(fiber.getBaseOffset)
        fiber
      case ParquetDataFiberHeader(false, false, 0) =>
        val length = fiberLength(column, total, 1)
        logDebug(s"will apply $length bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(length)
        val nativeAddress =
          dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset), column, total)
        dumpDataToFiber(nativeAddress, column, total)
        fiber
      case ParquetDataFiberHeader(false, false, dicLength) =>
        val length = fiberLength(column, total, 1, dicLength)
        logDebug(s"will apply $length bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(length)
        val nativeAddress =
          dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset), column, total)
        dumpDataAndDicToFiber(nativeAddress, column, total, dicLength)
        fiber
      case ParquetDataFiberHeader(true, true, _) =>
        throw new OapException("impossible header status (true, true, _).")
      case other => throw new OapException(s"impossible header status $other.")
    }
  }

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

  private def emptyDataFiber(fiberLength: Long): FiberCache =
    OapRuntime.getOrCreate.memoryManager.getEmptyDataFiberCache(fiberLength)
}

/**
 * ParquetDataFiberReader use to read data to ColumnVector.
 * @param address data fiber address.
 * @param dataType data type of data fiber.
 * @param total total row count of data fiber.
 */
class ParquetDataFiberReader private(address: Long, dataType: DataType, total: Int) extends
  Logging {

  private var header: ParquetDataFiberHeader = _

  private var dictionary: org.apache.spark.sql.execution.vectorized.Dictionary = _

  /**
   * Read num values to OnHeapColumnVector from data fiber by start position.
   * @param start data fiber start rowId position.
   * @param num need read values num.
   * @param column target OnHeapColumnVector.
   */
  def readBatch(
      start: Int, num: Int, column: OnHeapColumnVector): Unit = if (dictionary != null) {
    // Use dictionary encode, value store in dictionaryIds, it's a int array.
    column.setDictionary(dictionary)
    val dictionaryIds = column.reserveDictionaryIds(num).asInstanceOf[OnHeapColumnVector]
    header match {
      case ParquetDataFiberHeader(true, false, _) =>
        val dataNativeAddress = address + ParquetDataFiberHeader.defaultSize
        Platform.copyMemory(null,
          dataNativeAddress + start * 4L,
          dictionaryIds.getIntData, Platform.INT_ARRAY_OFFSET, num * 4L)
      case ParquetDataFiberHeader(false, false, _) =>
        val nullsNativeAddress = address + ParquetDataFiberHeader.defaultSize
        Platform.copyMemory(null,
          nullsNativeAddress + start, column.getNulls, Platform.BYTE_ARRAY_OFFSET, num)
        val dataNativeAddress = nullsNativeAddress + 1 * total
        Platform.copyMemory(null,
          dataNativeAddress + start * 4L,
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
        val dataNativeAddress = address + ParquetDataFiberHeader.defaultSize
        readBatch(dataNativeAddress, start, num, column)
      case ParquetDataFiberHeader(false, false, _) =>
        val nullsNativeAddress = address + ParquetDataFiberHeader.defaultSize
        Platform.copyMemory(null,
          nullsNativeAddress + start, column.getNulls, Platform.BYTE_ARRAY_OFFSET, num)
        val dataNativeAddress = nullsNativeAddress + 1 * total
        readBatch(dataNativeAddress, start, num, column)
      case ParquetDataFiberHeader(false, true, _) =>
        column.putNulls(0, num)
      case ParquetDataFiberHeader(true, true, _) =>
        throw new OapException("error header status (true, true, _)")
      case other => throw new OapException(s"impossible header status $other.")
    }
  }

  /**
   * Read a OnHeapColumnVector by rowIdList, suitable for rowIdList length is small,
   * read value one by one.
   * @param rowIdList need rowId List
   * @param column target OnHeapColumnVector
   */
  def readBatch(rowIdList: IntList, column: OnHeapColumnVector): Unit = if (dictionary != null) {
    // Use dictionary encode, value store in dictionaryIds, it's a int array.
    column.setDictionary(dictionary)
    val num = rowIdList.size()
    val dictionaryIds = column.reserveDictionaryIds(num).asInstanceOf[OnHeapColumnVector]
    header match {
      case ParquetDataFiberHeader(true, false, _) =>
        val dataNativeAddress = address + ParquetDataFiberHeader.defaultSize
        val intData = dictionaryIds.getIntData
        (0 until num).foreach(idx => {
          intData(idx) = Platform.getInt(null,
            dataNativeAddress + rowIdList.getInt(idx) * 4L)
        })
      case ParquetDataFiberHeader(false, false, _) =>
        val nullsNativeAddress = address + ParquetDataFiberHeader.defaultSize
        val nulls = column.getNulls
        val intData = dictionaryIds.getIntData
        (0 until num).foreach(idx => {
          nulls(idx) = Platform.getByte(null, nullsNativeAddress + rowIdList.getInt(idx))
        })
        val dataNativeAddress = nullsNativeAddress + 1 * total
        (0 until num).foreach(idx => {
          if (!column.isNullAt(idx)) {
            intData(idx) = Platform.getInt(null,
              dataNativeAddress + rowIdList.getInt(idx) * 4L)
          }
        })
      case ParquetDataFiberHeader(false, true, _) =>
        column.putNulls(0, num)
      case ParquetDataFiberHeader(true, true, _) =>
        throw new OapException("error header status (true, true, _)")
      case other => throw new OapException(s"impossible header status $other.")
    }
  } else {
    column.setDictionary(null)
    val num = rowIdList.size()
    header match {
      case ParquetDataFiberHeader(true, false, _) =>
        val dataNativeAddress = address + ParquetDataFiberHeader.defaultSize
        readBatch(dataNativeAddress, rowIdList, column)
      case ParquetDataFiberHeader(false, false, _) =>
        val nullsNativeAddress = address + ParquetDataFiberHeader.defaultSize
        val nulls = column.getNulls
        (0 until num).foreach(idx => {
          nulls(idx) = Platform.getByte(null, nullsNativeAddress + rowIdList.getInt(idx))
        })
        val dataNativeAddress = nullsNativeAddress + 1 * total
        readBatch(dataNativeAddress, rowIdList, column)
      case ParquetDataFiberHeader(false, true, _) =>
        column.putNulls(0, num)
      case ParquetDataFiberHeader(true, true, _) =>
        throw new OapException("error header status (true, true, _)")
      case other => throw new OapException(s"impossible header status $other.")
    }
  }

  /**
   * Read ParquetDataFiberHeader and dictionary from data fiber.
   */
  private def readRowGroupMetas(): Unit = {
    header = ParquetDataFiberHeader(address)
    header match {
      case ParquetDataFiberHeader(_, _, 0) =>
        dictionary = null
      case ParquetDataFiberHeader(false, true, _) =>
        dictionary = null
      case ParquetDataFiberHeader(true, false, dicLength) =>
        val dicNativeAddress = address + ParquetDataFiberHeader.defaultSize + 4L * total
        dictionary =
          new ParquetDictionaryWrapper(readDictionary(dataType, dicLength, dicNativeAddress))
      case ParquetDataFiberHeader(false, false, dicLength) =>
        val dicNativeAddress = address + ParquetDataFiberHeader.defaultSize + 1 * total + 4L * total
        dictionary =
          new ParquetDictionaryWrapper(readDictionary(dataType, dicLength, dicNativeAddress))
      case ParquetDataFiberHeader(true, true, _) =>
        throw new OapException("error header status (true, true, _)")
      case other => throw new OapException(s"impossible header status $other.")
    }
  }

  /**
   * Read num values to OnHeapColumnVector from data fiber by start position,
   * not Dictionary encode.
   */
  private def readBatch(
      dataNativeAddress: Long, start: Int, num: Int, column: OnHeapColumnVector): Unit = {

    def readBinaryToColumnVector(): Unit = {
      Platform.copyMemory(null,
        dataNativeAddress + start * 4L,
        column.getArrayLengths, Platform.INT_ARRAY_OFFSET, num * 4L)
      Platform.copyMemory(
        null,
        dataNativeAddress + total * 4L + start * 4L,
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
        Platform.copyMemory(null,
          dataNativeAddress + total * 8L + startOffset,
          data, Platform.BYTE_ARRAY_OFFSET, data.length)
        column.getChild(0).asInstanceOf[OnHeapColumnVector].setByteData(data)
      }
    }

    dataType match {
      case ByteType | BooleanType =>
        Platform.copyMemory(null,
          dataNativeAddress + start, column.getByteData, Platform.BYTE_ARRAY_OFFSET, num)
      case ShortType =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 2L,
          column.getShortData, Platform.SHORT_ARRAY_OFFSET, num * 2L)
      case IntegerType | DateType =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 4L,
          column.getIntData, Platform.INT_ARRAY_OFFSET, num * 4L)
      case FloatType =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 4L,
          column.getFloatData, Platform.FLOAT_ARRAY_OFFSET, num * 4L)
      case LongType | TimestampType =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 8L,
          column.getLongData, Platform.LONG_ARRAY_OFFSET, num * 8L)
      case DoubleType =>
        Platform.copyMemory(
          null, dataNativeAddress + start * 8L,
          column.getDoubleData, Platform.DOUBLE_ARRAY_OFFSET, num * 8L)
      case BinaryType | StringType => readBinaryToColumnVector()
      // if DecimalType.is32BitDecimalType(other) as int data type.
      case other if DecimalType.is32BitDecimalType(other) =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 4L,
          column.getIntData, Platform.INT_ARRAY_OFFSET, num * 4L)
      // if DecimalType.is64BitDecimalType(other) as long data type.
      case other if DecimalType.is64BitDecimalType(other) =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 8L,
          column.getLongData, Platform.LONG_ARRAY_OFFSET, num * 8L)
      // if DecimalType.isByteArrayDecimalType(other) as binary data type.
      case other if DecimalType.isByteArrayDecimalType(other) => readBinaryToColumnVector()
      case other => throw new OapException(s"impossible data type $other.")
    }
  }

  /**
   * Read a OnHeapColumnVector by rowIdList, suitable for rowIdList length is small,
   * read value one by one, not Dictionary encode.
   */
  private def readBatch(
      dataNativeAddress: Long, rowIdList: IntList, column: OnHeapColumnVector): Unit = {

    def readBinaryToColumnVector(): Unit = {
      val arrayLengths = column.getArrayLengths
      val arrayOffsets = column.getArrayOffsets
      val offsetsStart = total * 4L
      val dataStart = total * 8L
      var offset = 0
      val childColumn = column.getChild(0).asInstanceOf[OnHeapColumnVector]
      (0 until rowIdList.size()).foreach(idx => {
        if (!column.isNullAt(idx)) {
          val rowId = rowIdList.getInt(idx)
          val length = Platform.getInt(null, dataNativeAddress + rowId * 4L)
          val start = Platform.getInt(null, dataNativeAddress + offsetsStart + rowId * 4L)
          val data = new Array[Byte](length)
          Platform.copyMemory(null, dataNativeAddress + dataStart + start, data,
            Platform.BYTE_ARRAY_OFFSET, length)
          arrayOffsets(idx) = offset
          arrayLengths(idx) = length
          childColumn.appendBytes(length, data, 0)
          offset += length
        }
      })
    }

    dataType match {
      case ByteType | BooleanType =>
        val bytes = column.getByteData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            bytes(idx) = Platform.getByte(null, dataNativeAddress + rowIdList.getInt(idx))
          }
        })
      case ShortType =>
        val shorts = column.getShortData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            shorts(idx) = Platform.getShort(null,
              dataNativeAddress + rowIdList.getInt(idx) * 2L)
          }
        })
      case IntegerType | DateType =>
        val ints = column.getIntData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            ints(idx) = Platform.getInt(null, dataNativeAddress + rowIdList.getInt(idx) * 4L)
          }
        })
      case FloatType =>
        val floats = column.getFloatData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            floats(idx) = Platform.getFloat(null,
              dataNativeAddress + rowIdList.getInt(idx) * 4L)
          }
        })
      case LongType | TimestampType =>
        val longs = column.getLongData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            longs(idx) = Platform.getLong(null,
              dataNativeAddress + rowIdList.getInt(idx) * 8L)
          }
        })
      case DoubleType =>
        val doubles = column.getDoubleData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            doubles(idx) = Platform.getDouble(null,
              dataNativeAddress + rowIdList.getInt(idx) * 8L)
          }
        })
      case BinaryType | StringType => readBinaryToColumnVector()
      // if DecimalType.is32BitDecimalType(other) as int data type.
      case other if DecimalType.is32BitDecimalType(other) =>
        val ints = column.getIntData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            ints(idx) = Platform.getInt(null,
              dataNativeAddress + rowIdList.getInt(idx) * 4L)
          }
        })
      // if DecimalType.is64BitDecimalType(other) as long data type.
      case other if DecimalType.is64BitDecimalType(other) =>
        val longs = column.getLongData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            longs(idx) = Platform.getLong(null,
              dataNativeAddress + rowIdList.getInt(idx) * 8L)
          }
        })
      // if DecimalType.isByteArrayDecimalType(other) as binary data type.
      case other if DecimalType.isByteArrayDecimalType(other) => readBinaryToColumnVector()
      case other => throw new OapException(s"impossible data type $other.")
    }
  }

  /**
   * Read a `dicLength` size Parquet Dictionary from data fiber.
   */
  private def readDictionary(
      dataType: DataType, dicLength: Int, dicNativeAddress: Long): Dictionary = {

    def readBinaryDictionary: Dictionary = {
      val binaryDictionaryContent = new Array[Binary](dicLength)
      val lengthsArray = new Array[Int](dicLength)
      Platform.copyMemory(null, dicNativeAddress,
        lengthsArray, Platform.INT_ARRAY_OFFSET, dicLength * 4L)
      val dictionaryBytesLength = lengthsArray.sum
      val dictionaryBytes = new Array[Byte](dictionaryBytesLength)
      Platform.copyMemory(null,
        dicNativeAddress + dicLength * 4L,
        dictionaryBytes, Platform.BYTE_ARRAY_OFFSET, dictionaryBytesLength)
      var offset = 0
      for (i <- binaryDictionaryContent.indices) {
        val length = lengthsArray(i)
        binaryDictionaryContent(i) =
          Binary.fromConstantByteArray(dictionaryBytes, offset, length)
        offset += length
      }
      BinaryDictionary(binaryDictionaryContent)
    }

    dataType match {
      // ByteType, ShortType, IntegerType, DateType Dictionary read as Int type array.
      case ByteType | ShortType | IntegerType | DateType =>
        val intDictionaryContent = new Array[Int](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, intDictionaryContent, Platform.INT_ARRAY_OFFSET, dicLength * 4L)
        IntegerDictionary(intDictionaryContent)
      // FloatType Dictionary read as Float type array.
      case FloatType =>
        val floatDictionaryContent = new Array[Float](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, floatDictionaryContent, Platform.FLOAT_ARRAY_OFFSET, dicLength * 4L)
        FloatDictionary(floatDictionaryContent)
      // LongType Dictionary read as Long type array.
      case LongType | TimestampType =>
        val longDictionaryContent = new Array[Long](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, longDictionaryContent, Platform.LONG_ARRAY_OFFSET, dicLength * 8L)
        LongDictionary(longDictionaryContent)
      // DoubleType Dictionary read as Double type array.
      case DoubleType =>
        val doubleDictionaryContent = new Array[Double](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, doubleDictionaryContent, Platform.DOUBLE_ARRAY_OFFSET, dicLength * 8L)
        DoubleDictionary(doubleDictionaryContent)
      // StringType, BinaryType Dictionary read as a Int array and Byte array,
      // we use int array record offset and length of Byte array and use a shared backend
      // Byte array to construct all Binary.
      case StringType | BinaryType => readBinaryDictionary
      // if DecimalType.is32BitDecimalType(other) as int data type.
      case other if DecimalType.is32BitDecimalType(other) =>
        val intDictionaryContent = new Array[Int](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, intDictionaryContent, Platform.INT_ARRAY_OFFSET, dicLength * 4L)
        IntegerDictionary(intDictionaryContent)
      // if DecimalType.is64BitDecimalType(other) as long data type.
      case other if DecimalType.is64BitDecimalType(other) =>
        val longDictionaryContent = new Array[Long](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, longDictionaryContent, Platform.LONG_ARRAY_OFFSET, dicLength * 8L)
        LongDictionary(longDictionaryContent)
      // if DecimalType.isByteArrayDecimalType(other) as binary data type.
      case other if DecimalType.isByteArrayDecimalType(other) => readBinaryDictionary
      case other => throw new OapException(s"$other data type is not support dictionary.")
    }
  }
}

object ParquetDataFiberReader {
  def apply(address: Long, dataType: DataType, total: Int): ParquetDataFiberReader = {
    val reader = new ParquetDataFiberReader(address, dataType, total)
    reader.readRowGroupMetas()
    reader
  }
}

/**
 * Wrap a Int Array to a Parquet Dictionary.
 */
case class IntegerDictionary(dictionaryContent: Array[Int])
  extends Dictionary(Encoding.PLAIN) {

  override def decodeToInt(id: Int): Int = dictionaryContent(id)

  override def getMaxId: Int = dictionaryContent.length - 1
}

/**
 * Wrap a Float Array to a Parquet Dictionary.
 */
case class FloatDictionary(dictionaryContent: Array[Float])
  extends Dictionary(Encoding.PLAIN) {

  override def decodeToFloat(id: Int): Float = dictionaryContent(id)

  override def getMaxId: Int = dictionaryContent.length - 1
}

/**
 * Wrap a Long Array to a Parquet Dictionary.
 */
case class LongDictionary(dictionaryContent: Array[Long])
  extends Dictionary(Encoding.PLAIN) {

  override def decodeToLong(id: Int): Long = dictionaryContent(id)

  override def getMaxId: Int = dictionaryContent.length - 1
}

/**
 * Wrap a Double Array to a Parquet Dictionary.
 */
case class DoubleDictionary(dictionaryContent: Array[Double])
  extends Dictionary(Encoding.PLAIN) {

  override def decodeToDouble(id: Int): Double = dictionaryContent(id)

  override def getMaxId: Int = dictionaryContent.length - 1
}

/**
 * Wrap a Binary Array to a Parquet Dictionary.
 */
case class BinaryDictionary(dictionaryContent: Array[Binary])
  extends Dictionary(Encoding.PLAIN) {

  override def decodeToBinary(id: Int): Binary = dictionaryContent(id)

  override def getMaxId: Int = dictionaryContent.length - 1
}

/**
 * Define a `ParquetDataFiberHeader` to record data fiber status.
 * @param noNulls status represent no null value in this data fiber.
 * @param allNulls status represent all value are null in this data fiber.
 * @param dicLength dictionary length of this data fiber, if 0 represent there is no dictionary.
 */
case class ParquetDataFiberHeader(noNulls: Boolean, allNulls: Boolean, dicLength: Int) {

  /**
   * Write ParquetDataFiberHeader to Fiber
   * @param address dataFiber address offset
   * @return dataFiber address offset
   */
  def writeToCache(address: Long): Long = {
    Platform.putBoolean(null, address, noNulls)
    Platform.putBoolean(null, address + 1, allNulls)
    Platform.putInt(null, address + 2, dicLength)
    address + ParquetDataFiberHeader.defaultSize
  }
}

/**
 * Use to construct ParquetDataFiberHeader instance.
 */
object ParquetDataFiberHeader {

  def apply(vector: OnHeapColumnVector, total: Int): ParquetDataFiberHeader = {
    val numNulls = vector.numNulls
    val allNulls = numNulls == total
    val noNulls = numNulls == 0
    val dicLength = vector.dictionaryLength
    new ParquetDataFiberHeader(noNulls, allNulls, dicLength)
  }

  def apply(nativeAddress: Long): ParquetDataFiberHeader = {
    val noNulls = Platform.getBoolean(null, nativeAddress)
    val allNulls = Platform.getBoolean(null, nativeAddress + 1)
    val dicLength = Platform.getInt(null, nativeAddress + 2)
    new ParquetDataFiberHeader(noNulls, allNulls, dicLength)
  }

  /**
   * allNulls: Boolean: 1
   * noNulls: Boolean: 1
   * dicLength: Int: 4
   * @return 1 + 1 + 4
   */
  def defaultSize: Int = 6
}
