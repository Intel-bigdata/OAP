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
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

object ParquetDataFiberWriter extends Logging {

  def dumpToCache(vector: OnHeapColumnVector, total: Int): FiberCache = {
    val header = ParquetDataFiberHeader(vector, total)
    header match {
      case ParquetDataFiberHeader(true, false, 0) =>
        val fiber = emptyDataFiber(fiberLength(vector, total, 0 ))
        val nativeAddress = header.writeToCache(fiber.getBaseOffset)
        dumpDataToFiber(nativeAddress, vector, total)
        fiber
      case ParquetDataFiberHeader(true, false, dicLength) =>
        val fiber = emptyDataFiber(fiberLength(vector, total, 0, dicLength))
        val nativeAddress = header.writeToCache(fiber.getBaseOffset)
        dumpDataAndDicToFiber(nativeAddress, vector, total, dicLength)
        fiber
      case ParquetDataFiberHeader(false, true, _) =>
        val fiber = emptyDataFiber(ParquetDataFiberHeader.defaultSize)
        header.writeToCache(fiber.getBaseOffset)
        fiber
      case ParquetDataFiberHeader(false, false, 0) =>
        val fiber = emptyDataFiber(fiberLength(vector, total, 1))
        val nativeAddress =
          dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset), vector, total)
        dumpDataToFiber(nativeAddress, vector, total)
        fiber
      case ParquetDataFiberHeader(false, false, dicLength) =>
        val fiber = emptyDataFiber(fiberLength(vector, total, 1, dicLength))
        val nativeAddress =
          dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset), vector, total)
        dumpDataAndDicToFiber(nativeAddress, vector, total, dicLength)
        fiber
      case ParquetDataFiberHeader(true, true, _) => throw new OapException("error status.")
    }
  }

  private def dumpNullsToFiber(
      nativeAddress: Long, vector: OnHeapColumnVector, total: Int): Long = {
    Platform.copyMemory(vector.nulls, Platform.BYTE_ARRAY_OFFSET, null, nativeAddress, total)
    nativeAddress + total
  }

  /**
   * noNulls is true, nulls are all 0, not dump nulls to cache,
   * allNulls is false, need dump to cache,
   * dicLength is 0, needn't calculate dictionary part.
   */
  private def dumpDataToFiber(nativeAddress: Long, vector: OnHeapColumnVector, total: Int): Unit = {
    vector.dataType match {
      case ByteType | BooleanType =>
        Platform.copyMemory(vector.byteData, Platform.BYTE_ARRAY_OFFSET, null, nativeAddress, total)
      case ShortType =>
        Platform.copyMemory(vector.shortData,
          Platform.SHORT_ARRAY_OFFSET, null, nativeAddress, total * 2)
      case IntegerType | DateType =>
        Platform.copyMemory(vector.intData,
          Platform.INT_ARRAY_OFFSET, null, nativeAddress, total * 4)
      case FloatType =>
        Platform.copyMemory(vector.floatData,
          Platform.FLOAT_ARRAY_OFFSET, null, nativeAddress, total * 4)
      case LongType =>
        Platform.copyMemory(vector.longData,
          Platform.LONG_ARRAY_OFFSET, null, nativeAddress, total * 8)
      case DoubleType =>
        Platform.copyMemory(vector.doubleData,
          Platform.DOUBLE_ARRAY_OFFSET, null, nativeAddress, total * 8)
      case StringType | BinaryType =>
        Platform.copyMemory(vector.arrayLengths,
          Platform.INT_ARRAY_OFFSET, null, nativeAddress, total * 4)
        Platform.copyMemory(vector.arrayOffsets,
          Platform.INT_ARRAY_OFFSET, null, nativeAddress + total * 4, total * 4)
        val child = vector.getChildColumn(0).asInstanceOf[OnHeapColumnVector]
        Platform.copyMemory(child.byteData,
          Platform.BYTE_ARRAY_OFFSET, null, nativeAddress + total * 8, child.getElementsAppended)
    }
  }

  private def dumpDataAndDicToFiber(
      nativeAddress: Long, vector: OnHeapColumnVector, total: Int, dicLength: Int): Unit = {
    val dictionaryIds = vector.getDictionaryIds.asInstanceOf[OnHeapColumnVector]
    Platform.copyMemory(dictionaryIds.intData,
      Platform.INT_ARRAY_OFFSET, null, nativeAddress, total * 4)
    var dicNativeAddress = nativeAddress + total * 4
    val dictionary: Dictionary = vector.getDictionary
    vector.dataType() match {
      case ByteType | ShortType | IntegerType | DateType =>
        val intDictionaryContent = new Array[Int](dicLength)
        (0 until dicLength).foreach(id => intDictionaryContent(id) = dictionary.decodeToInt(id))
        Platform.copyMemory(intDictionaryContent, Platform.INT_ARRAY_OFFSET, null,
          dicNativeAddress, dicLength * 4)
      case FloatType =>
        val floatDictionaryContent = new Array[Float](dicLength)
        (0 until dicLength).foreach(id => floatDictionaryContent(id) = dictionary.decodeToFloat(id))
        Platform.copyMemory(floatDictionaryContent, Platform.FLOAT_ARRAY_OFFSET, null,
          dicNativeAddress, dicLength * 4)
      case LongType =>
        val longDictionaryContent = new Array[Long](dicLength)
        (0 until dicLength).foreach(id => longDictionaryContent(id) = dictionary.decodeToLong(id))
        Platform.copyMemory(longDictionaryContent, Platform.LONG_ARRAY_OFFSET, null,
          dicNativeAddress, dicLength * 8)
      case DoubleType =>
        val doubleDictionaryContent = new Array[Double](dicLength)
        (0 until dicLength).foreach(id =>
          doubleDictionaryContent(id) = dictionary.decodeToDouble(id))
        Platform.copyMemory(doubleDictionaryContent, Platform.DOUBLE_ARRAY_OFFSET, null,
          dicNativeAddress, dicLength * 8);
      case StringType | BinaryType =>
        var bytesNativeAddress = dicNativeAddress + 4 * dicLength
        (0 until dicLength).foreach( id => {
          val binary = dictionary.decodeToBinary(id)
          val length = binary.length
          Platform.putInt(null, dicNativeAddress, length)
          dicNativeAddress += 4
          Platform.copyMemory(binary.getBytes,
            Platform.BYTE_ARRAY_OFFSET, null, bytesNativeAddress, length)
          bytesNativeAddress += length
        })
      case otherTypes: DataType => throw new OapException(s"${otherTypes.simpleString}" +
        s" data type is not support dictionary.")
    }
  }

  /**
   * noNulls is true, nulls are all 0, not dump nulls to cache, nullsLength is 0,
   * allNulls is false, need dump to cache,
   * dicLength is 0, needn't calculate dictionary part.
   */
  private def fiberLength(vector: OnHeapColumnVector, total: Int, nullUnitLength: Int): Long =
    if (isFixedLengthDataType(vector.dataType())) {
      ParquetDataFiberHeader.defaultSize +
        nullUnitLength * total + vector.dataType().defaultSize * total
    } else {
      // lengthData and offsetData will be set and data will be put in child if type is Array.
      // lengthData: 4 bytes, offsetData: 4 bytes, nulls: 1 byte,
      // child.data: childColumns[0].elementsAppended bytes.
      ParquetDataFiberHeader.defaultSize + nullUnitLength * total + total * 8 +
        vector.getChildColumn(0).getElementsAppended
    }

  private def fiberLength(
      vector: OnHeapColumnVector, total: Int, nullUnitLength: Int, dicLength: Int): Long = {
    val dicPartSize = vector.dataType() match {
      case ByteType | ShortType | IntegerType | DateType => dicLength * 4
      case FloatType => dicLength * 4
      case LongType => dicLength * 8
      case DoubleType => dicLength * 8
      case StringType | BinaryType =>
        val dictionary: Dictionary = vector.getDictionary
        (0 until dicLength).map(id => dictionary.decodeToBinary(id).length() + 4).sum
      case otherTypes: DataType => throw new OapException(s"${otherTypes.simpleString}" +
        s" data type is not support dictionary.")
    }
    ParquetDataFiberHeader.defaultSize + nullUnitLength * total + 4 * total + dicPartSize
  }

  private def isFixedLengthDataType(dataType: DataType): Boolean = dataType match {
    case StringType | BinaryType => false
    case ByteType | BooleanType | ShortType |
         IntegerType | DateType | FloatType |
         LongType | DoubleType => true
    case otherTypes: DataType => throw new OapException(s"${otherTypes.simpleString}" +
      s" data type is not implemented for cache.")
  }

  private def emptyDataFiber(fiberLength: Long): FiberCache =
    OapRuntime.getOrCreate.memoryManager.getEmptyDataFiberCache(fiberLength)
}

case class ParquetDataFiberReader(address: Long, dataType: DataType, total: Int) extends Logging {

  private var header: ParquetDataFiberHeader = _

  private var dictionary: Dictionary = _

  def readRowGroupMetas(): Unit = {
    header = ParquetDataFiberHeader(address)
    header match {
      case ParquetDataFiberHeader(_, _, 0) =>
        dictionary = null
      case ParquetDataFiberHeader(false, true, _) =>
        dictionary = null
      case ParquetDataFiberHeader(true, false, dicLength) =>
        val dicNativeAddress = address + ParquetDataFiberHeader.defaultSize + 4 * total
        dictionary = readDictionary(dataType, dicLength, dicNativeAddress)
      case ParquetDataFiberHeader(false, false, dicLength) =>
        val dicNativeAddress = address + ParquetDataFiberHeader.defaultSize + 1 * total + 4 * total
        dictionary = readDictionary(dataType, dicLength, dicNativeAddress)
      case ParquetDataFiberHeader(true, true, _) => throw new OapException("error status.")
    }
  }

  def readBatch(
      start: Int, num: Int, column: OnHeapColumnVector): Unit = if (dictionary != null) {
    column.setDictionary(dictionary)
    val dictionaryIds = column.reserveDictionaryIds(num).asInstanceOf[OnHeapColumnVector]
    header match {
      case ParquetDataFiberHeader(true, false, _) =>
        val dataNativeAddress = address + ParquetDataFiberHeader.defaultSize
        Platform.copyMemory(null,
          dataNativeAddress + start * 4,
          dictionaryIds.intData, Platform.INT_ARRAY_OFFSET, num * 4)
      case ParquetDataFiberHeader(false, false, _) =>
        val nullsNativeAddress = address + ParquetDataFiberHeader.defaultSize
        Platform.copyMemory(null,
          nullsNativeAddress + start, column.nulls, Platform.BYTE_ARRAY_OFFSET, num)
        val dataNativeAddress = nullsNativeAddress + 1 * total
        Platform.copyMemory(null,
          dataNativeAddress + start * 4,
          dictionaryIds.intData, Platform.INT_ARRAY_OFFSET, num * 4)
      case ParquetDataFiberHeader(false, true, _) =>
        // can to this branch ?
        column.putNulls(0, num)
      case ParquetDataFiberHeader(true, true, _) => throw new OapException("error status.")
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
          nullsNativeAddress + start, column.nulls, Platform.BYTE_ARRAY_OFFSET, num)
        val dataNativeAddress = nullsNativeAddress + 1 * total
        readBatch(dataNativeAddress, start, num, column)
      case ParquetDataFiberHeader(false, true, _) =>
        column.putNulls(0, num)
      case ParquetDataFiberHeader(true, true, _) => throw new OapException("error status.")
    }
  }

  def readBatch(rowIdList: IntList, column: OnHeapColumnVector): Unit = if (dictionary != null) {
    column.setDictionary(dictionary)
    val num = rowIdList.size()
    val dictionaryIds = column.reserveDictionaryIds(num).asInstanceOf[OnHeapColumnVector]
    header match {
      case ParquetDataFiberHeader(true, false, _) =>
        val dataNativeAddress = address + ParquetDataFiberHeader.defaultSize
        val intData = dictionaryIds.intData
        (0 until num).foreach(idx => {
          intData(idx) = Platform.getInt(null, dataNativeAddress + rowIdList.getInt(idx) * 4)
        })
      case ParquetDataFiberHeader(false, false, _) =>
        val nullsNativeAddress = address + ParquetDataFiberHeader.defaultSize
        val nulls = column.nulls
        val intData = dictionaryIds.intData
        (0 until num).foreach(idx => {
          nulls(idx) = Platform.getByte(null, nullsNativeAddress + rowIdList.getInt(idx))
        })
        val dataNativeAddress = nullsNativeAddress + 1 * total
        (0 until num).foreach(idx => {
          if (!column.isNullAt(idx)) {
            intData(idx) = Platform.getInt(null, dataNativeAddress + rowIdList.getInt(idx) * 4)
          }
        })
      case ParquetDataFiberHeader(false, true, _) =>
        column.putNulls(0, num)
      case ParquetDataFiberHeader(true, true, _) => throw new OapException("error status.")
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
        val nulls = column.nulls
        (0 until num).foreach(idx => {
          nulls(idx) = Platform.getByte(null, nullsNativeAddress + rowIdList.getInt(idx))
        })
        val dataNativeAddress = nullsNativeAddress + 1 * total
        readBatch(dataNativeAddress, rowIdList, column)
      case ParquetDataFiberHeader(false, true, _) =>
        column.putNulls(0, num)
      case ParquetDataFiberHeader(true, true, _) => throw new OapException("error status.")
    }
  }

  private def readBatch(
      dataNativeAddress: Long, start: Int, num: Int, column: OnHeapColumnVector): Unit = {
    dataType match {
      case ByteType | BooleanType =>
        Platform.copyMemory(null,
          dataNativeAddress + start, column.byteData, Platform.BYTE_ARRAY_OFFSET, num)
      case ShortType =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 2,
          column.shortData, Platform.SHORT_ARRAY_OFFSET, num * 2)
      case IntegerType | DateType =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 4,
          column.intData, Platform.INT_ARRAY_OFFSET, num * 4)
      case FloatType =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 4,
          column.floatData, Platform.FLOAT_ARRAY_OFFSET, num * 4)
      case LongType =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 8,
          column.longData, Platform.LONG_ARRAY_OFFSET, num * 8)
      case DoubleType =>
        Platform.copyMemory(
          null, dataNativeAddress + start * 8,
          column.doubleData, Platform.DOUBLE_ARRAY_OFFSET, num * 8)
      case BinaryType | StringType =>
        Platform.copyMemory(null,
          dataNativeAddress + start * 4,
          column.arrayLengths, Platform.INT_ARRAY_OFFSET, num * 4)
        Platform.copyMemory(
          null,
          dataNativeAddress + total * 4 + start * 4,
          column.arrayOffsets, Platform.INT_ARRAY_OFFSET, num * 4)

        var lastIndex = num - 1
        while (lastIndex >= 0 && column.isNullAt(lastIndex)) {
          lastIndex -= 1
        }
        var firstIndex = 0
        while (firstIndex < num && column.isNullAt(firstIndex)) {
          firstIndex += 1
        }
        if (firstIndex < num && lastIndex >= 0) {
          val arrayOffsets: Array[Int] = column.arrayOffsets
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
            dataNativeAddress + total * 8 + startOffset,
            data, Platform.BYTE_ARRAY_OFFSET, data.length)
          column.getChildColumn(0).asInstanceOf[OnHeapColumnVector].byteData = data
        }
    }
  }

  private def readBatch(
      dataNativeAddress: Long, rowIdList: IntList, column: OnHeapColumnVector): Unit = {
    dataType match {
      case ByteType | BooleanType =>
        val bytes = column.byteData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            bytes(idx) = Platform.getByte(null, dataNativeAddress + rowIdList.getInt(idx))
          }
        })
      case ShortType =>
        val shorts = column.shortData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            shorts(idx) = Platform.getShort(null, dataNativeAddress + rowIdList.getInt(idx) * 2)
          }
        })
      case IntegerType | DateType =>
        val ints = column.intData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            ints(idx) = Platform.getInt(null, dataNativeAddress + rowIdList.getInt(idx) * 4)
          }
        })
      case FloatType =>
        val floats = column.floatData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            floats(idx) = Platform.getFloat(null, dataNativeAddress + rowIdList.getInt(idx) * 4)
          }
        })
      case LongType =>
        val longs = column.longData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            longs(idx) = Platform.getLong(null, dataNativeAddress + rowIdList.getInt(idx) * 8)
          }
        })
      case DoubleType =>
        val doubles = column.doubleData
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            doubles(idx) = Platform.getDouble(null, dataNativeAddress + rowIdList.getInt(idx) * 8)
          }
        })
      case BinaryType | StringType =>
        val arrayLengths = column.arrayLengths
        val arrayOffsets = column.arrayOffsets
        val offsetsStart = total * 4
        val dataStart = total * 8
        var offset = 0
        val childColumn = column.getChildColumn(0).asInstanceOf[OnHeapColumnVector]
        (0 until rowIdList.size()).foreach(idx => {
          if (!column.isNullAt(idx)) {
            val rowId = rowIdList.getInt(idx)
            val length = Platform.getInt(null, dataNativeAddress + rowId * 4)
            val start = Platform.getInt(null, dataNativeAddress + offsetsStart + rowId * 4)
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
  }

  private def readDictionary(
      dataType: DataType, dicLength: Int, dicNativeAddress: Long): Dictionary = {
    dataType match {
      case ByteType | ShortType | IntegerType | DateType =>
        val intDictionaryContent = new Array[Int](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, intDictionaryContent, Platform.INT_ARRAY_OFFSET, dicLength * 4)
        IntegerDictionary(intDictionaryContent)
      case FloatType =>
        val floatDictionaryContent = new Array[Float](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, floatDictionaryContent, Platform.FLOAT_ARRAY_OFFSET, dicLength * 4)
        FloatDictionary(floatDictionaryContent)
      case LongType =>
        val longDictionaryContent = new Array[Long](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, longDictionaryContent, Platform.LONG_ARRAY_OFFSET, dicLength * 8)
        LongDictionary(longDictionaryContent)
      case DoubleType =>
        val doubleDictionaryContent = new Array[Double](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, doubleDictionaryContent, Platform.DOUBLE_ARRAY_OFFSET, dicLength * 8)
        DoubleDictionary(doubleDictionaryContent)
      case StringType | BinaryType =>
        val binaryDictionaryContent = new Array[Binary](dicLength)
        val lengthsArray = new Array[Int](dicLength)
        Platform.copyMemory(null, dicNativeAddress,
          lengthsArray, Platform.INT_ARRAY_OFFSET, dicLength * 4)
        val dictionaryBytesLength = lengthsArray.sum
        val dictionaryBytes = new Array[Byte](dictionaryBytesLength)
        Platform.copyMemory(null,
          dicNativeAddress + dicLength * 4,
          dictionaryBytes, Platform.BYTE_ARRAY_OFFSET, dictionaryBytesLength)
        var offset = 0
        for (i <- binaryDictionaryContent.indices) {
          val length = lengthsArray(i)
          binaryDictionaryContent(i) =
            Binary.fromConstantByteArray(dictionaryBytes, offset, length)
          offset += length
        }
        BinaryDictionary(binaryDictionaryContent)
      case otherTypes: DataType => throw new OapException(s"${otherTypes.simpleString}" +
        s" data type is not support dictionary.")
    }
  }
}

case class IntegerDictionary(dictionaryContent: Array[Int]) extends Dictionary(Encoding.PLAIN) {

  override def decodeToInt(id: Int): Int = dictionaryContent(id)

  override def getMaxId: Int = dictionaryContent.length - 1
}

case class FloatDictionary(dictionaryContent: Array[Float])
  extends Dictionary(Encoding.PLAIN) {

  override def decodeToFloat(id: Int): Float = dictionaryContent(id)

  override def getMaxId: Int = dictionaryContent.length - 1
}

case class LongDictionary(dictionaryContent: Array[Long])
  extends Dictionary(Encoding.PLAIN) {

  override def decodeToLong(id: Int): Long = dictionaryContent(id)

  override def getMaxId: Int = dictionaryContent.length - 1
}

case class DoubleDictionary(dictionaryContent: Array[Double])
  extends Dictionary(Encoding.PLAIN) {

  override def decodeToDouble(id: Int): Double = dictionaryContent(id)

  override def getMaxId: Int = dictionaryContent.length - 1
}

case class BinaryDictionary(dictionaryContent: Array[Binary])
  extends Dictionary(Encoding.PLAIN) {

  override def decodeToBinary(id: Int): Binary = dictionaryContent(id)

  override def getMaxId: Int = dictionaryContent.length - 1
}

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
