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

import org.apache.parquet.column.Dictionary
import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.io.api.Binary
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.parquet.ParquetDictionaryWrapper
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

/**
 * ParquetDataFiberCompressedWriter is a util use to write compressed OnHeapColumnVector data
 * to data fiber.
 * Data Fiber write as follow format:
 * ParquetDataFiberCompressedHeader: (noNulls:boolean:1 bit, allNulls:boolean:1 bit,
 * dicLength:int:4 bit)
 * NullsData: (noNulls:false, allNulls: false) will store nulls to data fiber as bytes array
 * Values: store compressed value data except (noNulls:false, allNulls: true) by dataType,
 * Dic encode data store as int array.
 * Dictionary: if dicLength > 0 will store dic data by dataType.
 */
object ParquetDataFiberCompressedWriter extends Logging {

  def dumpToCache(column: OnHeapColumnVector, total: Int): FiberCache = {
    val header = ParquetDataFiberCompressedHeader(column, total)
    logDebug(s"will dump column to data fiber dataType = ${column.dataType()}, " +
      s"total = $total, header is $header")
    header match {
      case ParquetDataFiberCompressedHeader(true, false, 0) =>
        dumpDataToFiber(column, total, header)
      case ParquetDataFiberCompressedHeader(true, false, dicLength) =>
        dumpDataAndDicToFiber(column, total, dicLength, header)
      case ParquetDataFiberCompressedHeader(false, true, _) =>
        logDebug(s"will apply ${ParquetDataFiberCompressedHeader.defaultSize} " +
          s"bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(ParquetDataFiberCompressedHeader.defaultSize)
        header.writeToCache(fiber.getBaseOffset)
        fiber
      case ParquetDataFiberCompressedHeader(false, false, 0) =>
        dumpDataToFiber(column, total, header)
      case ParquetDataFiberCompressedHeader(false, false, dicLength) =>
        dumpDataAndDicToFiber(column, total, dicLength, header)
      case ParquetDataFiberCompressedHeader(true, true, _) =>
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
  private def dumpDataToFiber(
      column: OnHeapColumnVector,
      total: Int,
      header: ParquetDataFiberCompressedHeader): FiberCache = {

    val compressedLength =
      OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize

    def compressedBatchFiber(dataType: DataType): FiberCache = {
      val compressedUnitSize = if (total % compressedLength == 0) {
        total / compressedLength
      } else {
        (total / compressedLength) + 1
      }
      val arrayBytes: Array[Array[Byte]] = new Array[Array[Byte]](compressedUnitSize)
      var compressedSize = 0
      var count = 0
      var loadedRowCount = 0
      val batchCompressed = new Array[Boolean](compressedUnitSize)
      val compressor = OapRuntime.getOrCreate.fiberCacheManager.
        getCodecFactory.getCompressor(CompressionCodec.valueOf(
        OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionCodec)
      )
      var childColumnvectorLength = 0
      dataType match {
        case ByteType =>
          val bytes = column.getByteData
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val rawBytes: Array[Byte] = new Array[Byte](num)
            Platform.copyMemory(bytes,
              Platform.BYTE_ARRAY_OFFSET + count * compressedLength,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num)
            arrayBytes(count) = compressor.compress(rawBytes)
            batchCompressed(count) = true

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            count += 1
          }
        case ShortType =>
          val shortData = column.getShortData
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val rawBytes: Array[Byte] = new Array[Byte](num * 2)
            Platform.copyMemory(shortData,
              Platform.SHORT_ARRAY_OFFSET + count * compressedLength * 2,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 2)
            arrayBytes(count) = compressor.compress(rawBytes)
            batchCompressed(count) = true

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            count += 1
          }
        case IntegerType =>
          val intData = column.getIntData
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val rawBytes: Array[Byte] = new Array[Byte](num * 4)
            Platform.copyMemory(intData,
              Platform.INT_ARRAY_OFFSET + count * compressedLength * 4,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 4)
            arrayBytes(count) = compressor.compress(rawBytes)
            batchCompressed(count) = true

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            count += 1
          }
        case FloatType =>
          val floatData = column.getFloatData
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val rawBytes: Array[Byte] = new Array[Byte](num * 4)
            Platform.copyMemory(floatData,
              Platform.FLOAT_ARRAY_OFFSET + count * compressedLength * 4,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 4)
            arrayBytes(count) = compressor.compress(rawBytes)
            batchCompressed(count) = true

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            count += 1
          }
        case LongType =>
          val longData = column.getLongData
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val rawBytes: Array[Byte] = new Array[Byte](num * 8)
            Platform.copyMemory(longData,
              Platform.LONG_ARRAY_OFFSET + count * compressedLength * 8,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 8)
            arrayBytes(count) = compressor.compress(rawBytes)
            batchCompressed(count) = true

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            count += 1
          }
        case DoubleType =>
          val doubleData = column.getDoubleData
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val rawBytes: Array[Byte] = new Array[Byte](num * 8)
            Platform.copyMemory(doubleData,
              Platform.DOUBLE_ARRAY_OFFSET + count * compressedLength * 8,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 8)
            arrayBytes(count) = compressor.compress(rawBytes)
            batchCompressed(count) = true

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            count += 1
          }
        case StringType =>
          val arrayLengths: Array[Int] = column.getArrayLengths
          val arrayOffsets: Array[Int] = column.getArrayOffsets
          val childBytes: Array[Byte] = column.getChild(0)
            .asInstanceOf[OnHeapColumnVector].getByteData
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val startOffsets = arrayOffsets(count * compressedLength)
            val lastOffsets = arrayOffsets(num - 1)
            childColumnvectorLength = lastOffsets - startOffsets + arrayLengths(num - 1)

            val rawBytes = new Array[Byte](num * 8 + childColumnvectorLength)
            Platform.copyMemory(arrayLengths,
              Platform.INT_ARRAY_OFFSET + count * compressedLength * 4,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 4)

            Platform.copyMemory(arrayOffsets,
              Platform.INT_ARRAY_OFFSET + count * compressedLength * 4,
              rawBytes, Platform.BYTE_ARRAY_OFFSET + num * 4, num * 4)

            Platform.copyMemory(childBytes, Platform.BYTE_ARRAY_OFFSET + startOffsets,
              rawBytes, Platform.BYTE_ARRAY_OFFSET + num * 8, childColumnvectorLength )

            arrayBytes(count) = compressor.compress(rawBytes)
            batchCompressed(count) = true

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            count += 1
          }
      }

      val (fiber, nativeAddress) = if (header.noNulls) {
        val fiberLength = ParquetDataFiberCompressedHeader.defaultSize +
          compressedSize
        logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(fiberLength)
        (fiber, header.writeToCache(fiber.getBaseOffset))
      } else {
        val fiberLength = ParquetDataFiberCompressedHeader.defaultSize +
          total + compressedSize
        logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(fiberLength)
        val nullAddress = dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset),
          column, total)
        fiber.nullSize = total
        (fiber, nullAddress)
      }

      val fiberBatchedInfo = new mutable.HashMap[Int, (Long, Long, Boolean)]()
      var startAddress = nativeAddress
      count = 0
      while (count < compressedUnitSize) {
        Platform.copyMemory(arrayBytes(count), Platform.BYTE_ARRAY_OFFSET,
          null, startAddress, arrayBytes(count).length)
        fiberBatchedInfo.put(count,
          (startAddress, startAddress + arrayBytes(count).length, batchCompressed(count)))
        startAddress = startAddress + arrayBytes(count).length
        count += 1
      }

      // record the compressed parameter to fiber
      fiber.fiberBatchedInfo = fiberBatchedInfo
      if (dataType == StringType) {
        fiber.childColumnvectorLength = childColumnvectorLength
      }
      fiber
    }

    column.dataType match {
      case ByteType | BooleanType =>
        compressedBatchFiber(ByteType)
      case ShortType =>
        compressedBatchFiber(ShortType)
      case IntegerType | DateType =>
        compressedBatchFiber(IntegerType)
      case FloatType =>
        compressedBatchFiber(FloatType)
      case LongType | TimestampType =>
        compressedBatchFiber(LongType)
      case DoubleType =>
        compressedBatchFiber(DoubleType)
      case StringType | BinaryType =>
        compressedBatchFiber(StringType)
      case other if DecimalType.is32BitDecimalType(other) =>
        compressedBatchFiber(IntegerType)
      case other if DecimalType.is64BitDecimalType(other) =>
        compressedBatchFiber(LongType)
      case other if DecimalType.isByteArrayDecimalType(other) =>
        compressedBatchFiber(StringType)
      case other => throw new OapException(s"$other data type is not support data cache")
    }
  }

  /**
   * Write dictionaryIds(int array) and Dictionary data to data fiber.
   */
  private def dumpDataAndDicToFiber(
      column: OnHeapColumnVector, total: Int, dicLength: Int,
      header: ParquetDataFiberCompressedHeader): FiberCache = {

    val dictionary = column.getDictionary
    def compressedBatchDataAndDicToFiber(dataType: DataType) : FiberCache = {
      val compressedLength =
        OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
      val compressedUnitSize = if (total % compressedLength == 0) {
        total / compressedLength
      } else {
        (total / compressedLength) + 1
      }

      val arrayBytes: Array[Array[Byte]] = new Array[Array[Byte]](compressedUnitSize)
      var compressedSize = 0
      var count = 0
      var loadedRowCount = 0
      val batchCompressed = new Array[Boolean](compressedUnitSize)

      val dictionaryIds = column.getDictionaryIds.asInstanceOf[OnHeapColumnVector].getIntData
      val compressor = OapRuntime.getOrCreate.fiberCacheManager.
        getCodecFactory.getCompressor(CompressionCodec.valueOf(
        OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionCodec)
      )
      while (count < compressedUnitSize) {
        val num = Math.min(compressedLength, total - loadedRowCount)
        val rawBytes = new Array[Byte](num * 4)
        Platform.copyMemory(dictionaryIds,
          Platform.INT_ARRAY_OFFSET + count * compressedLength * 4,
          rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 4)

        arrayBytes(count) = compressor.compress(rawBytes)
        batchCompressed(count) = true

        compressedSize += arrayBytes(count).length
        loadedRowCount += compressedLength
        count += 1
      }
      var dictionaryBytes: Array[Byte] = null
      dataType match {
        case ByteType =>
          val typeLength = 4
          val dictionaryContent = new Array[Int](dicLength)
          (0 until dicLength).foreach(id => dictionaryContent(id) = dictionary.decodeToInt(id))
          dictionaryBytes = new Array[Byte](dicLength * typeLength)
          Platform.copyMemory(dictionaryContent, Platform.INT_ARRAY_OFFSET, dictionaryBytes,
            Platform.BYTE_ARRAY_OFFSET, dicLength * typeLength)
        case FloatType =>
          val typeLength = 4
          val dictionaryContent = new Array[Float](dicLength)
          (0 until dicLength).foreach(id => dictionaryContent(id) = dictionary.decodeToFloat(id))
          dictionaryBytes = new Array[Byte](dicLength * typeLength)
          Platform.copyMemory(dictionaryContent, Platform.FLOAT_ARRAY_OFFSET, dictionaryBytes,
            Platform.BYTE_ARRAY_OFFSET, dicLength * typeLength)
        case LongType =>
          val typeLength = 8
          val dictionaryContent = new Array[Long](dicLength)
          (0 until dicLength).foreach(id => dictionaryContent(id) = dictionary.decodeToLong(id))
          dictionaryBytes = new Array[Byte](dicLength * typeLength)
          Platform.copyMemory(dictionaryContent, Platform.LONG_ARRAY_OFFSET, dictionaryBytes,
            Platform.BYTE_ARRAY_OFFSET, dicLength * typeLength)
        case DoubleType =>
          val typeLength = 8
          val dictionaryContent = new Array[Double](dicLength)
          (0 until dicLength).foreach(id => dictionaryContent(id) = dictionary.decodeToDouble(id))
          dictionaryBytes = new Array[Byte](dicLength * typeLength)
          Platform.copyMemory(dictionaryContent, Platform.DOUBLE_ARRAY_OFFSET, dictionaryBytes,
            Platform.BYTE_ARRAY_OFFSET, dicLength * typeLength)
        case StringType =>
          val lengths = new Array[Int](dicLength)
          val bytes = new Array[Array[Byte]](dicLength)
          (0 until dicLength).foreach( id => {
            val binary = dictionary.decodeToBinary(id)
            lengths(id) = binary.length
            bytes(id) = binary
          })
          dictionaryBytes = new Array[Byte](dicLength * 4 + lengths.sum)
          Platform.copyMemory(lengths, Platform.INT_ARRAY_OFFSET,
            dictionaryBytes, Platform.BYTE_ARRAY_OFFSET, dicLength * 4)
          var address = dicLength * 4
          (0 until dicLength).foreach( id => {
            Platform.copyMemory(bytes(id), Platform.BYTE_ARRAY_OFFSET,
              dictionaryBytes, Platform.BYTE_ARRAY_OFFSET + address, lengths(id))
            address += lengths(id)
          })
      }
      // currently not support compress the dictionary content
      // val compressedDictionaryBytes = compressor.compress(dictionaryBytes)

      val (fiber, nativeAddress) = if (header.noNulls) {
        val fiberLength = ParquetDataFiberCompressedHeader.defaultSize +
          compressedSize + dictionaryBytes.length
        logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(fiberLength)
        (fiber, header.writeToCache(fiber.getBaseOffset))
      } else {
        val fiberLength = ParquetDataFiberCompressedHeader.defaultSize +
          total + compressedSize + dictionaryBytes.length
        logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
        val fiber = emptyDataFiber(fiberLength)
        val nullAddress = dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset),
          column, total)
        fiber.nullSize = total
        (fiber, nullAddress)
      }

      val fiberBatchedInfo = new mutable.HashMap[Int, (Long, Long, Boolean)]()
      var startAddress = nativeAddress
      count = 0
      while (count < compressedUnitSize) {
        Platform.copyMemory(arrayBytes(count), Platform.BYTE_ARRAY_OFFSET,
          null, startAddress, arrayBytes(count).length)
        fiberBatchedInfo.put(count,
          (startAddress, startAddress + arrayBytes(count).length, batchCompressed(count)))
        startAddress = startAddress + arrayBytes(count).length
        count += 1
      }

      val address = fiberBatchedInfo.get(compressedUnitSize -1).get._2

      Platform.copyMemory(dictionaryBytes, Platform.BYTE_ARRAY_OFFSET,
        null, address, dictionaryBytes.length)
      // record the compressed parameter to fiber
      fiber.fiberBatchedInfo = fiberBatchedInfo
      fiber
    }
    // dump dictionary to data fiber case by dataType.
    column.dataType() match {
      case ByteType | ShortType | IntegerType | DateType =>
        compressedBatchDataAndDicToFiber(ByteType)
      case FloatType =>
        compressedBatchDataAndDicToFiber(FloatType)
      case LongType | TimestampType =>
        compressedBatchDataAndDicToFiber(LongType)
      case DoubleType =>
        compressedBatchDataAndDicToFiber(DoubleType)
      case StringType | BinaryType =>
        compressedBatchDataAndDicToFiber(StringType)
      case other if DecimalType.is32BitDecimalType(other) =>
        compressedBatchDataAndDicToFiber(ByteType)
      case other if DecimalType.is64BitDecimalType(other) =>
        compressedBatchDataAndDicToFiber(LongType)
      case other if DecimalType.is64BitDecimalType(other) =>
        compressedBatchDataAndDicToFiber(StringType)
      case other => throw new OapException(s"$other data type is not support data cache")
    }
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
class ParquetDataFiberCompressedReader (
     address: Long, dataType: DataType, total: Int,
     var fiberCache: FiberCache) extends ParquetDataFiberReader(
     address = address, dataType = dataType, total = total) with Logging {

  private var header: ParquetDataFiberCompressedHeader = _

  var dictionary: org.apache.spark.sql.execution.vectorized.Dictionary = _

  /**
   * Read num values to OnHeapColumnVector from data fiber by start position.
   * @param start data fiber start rowId position.
   * @param num need read values num.
   * @param column target OnHeapColumnVector.
   */
  override def readBatch(
      start: Int, num: Int, column: OnHeapColumnVector): Unit = {
    // support get the fiber cache from on heap and off heap memory
    val baseObject = if (fiberCache != null) {
      fiberCache.fiberData.baseObject
    } else {
      null
    }

    if (dictionary != null) {
      // Use dictionary encode, value store in dictionaryIds, it's a int array.
      column.setDictionary(dictionary)
      val dictionaryIds = column.reserveDictionaryIds(num).asInstanceOf[OnHeapColumnVector]
      header match {
        case ParquetDataFiberCompressedHeader(true, false, _) =>
          val dataNativeAddress = fiberCache.fiberData.baseOffset
          Platform.copyMemory(baseObject,
            dataNativeAddress,
            dictionaryIds.getIntData, Platform.INT_ARRAY_OFFSET, num * 4)
        case ParquetDataFiberCompressedHeader(false, false, _) =>
          val nullsNativeAddress = fiberCache.fiberData.baseOffset
          Platform.copyMemory(baseObject,
            nullsNativeAddress + start, column.getNulls, Platform.BYTE_ARRAY_OFFSET, num)
          val dataNativeAddress = nullsNativeAddress + 1 * total
          Platform.copyMemory(baseObject,
            dataNativeAddress,
            dictionaryIds.getIntData, Platform.INT_ARRAY_OFFSET, num * 4)
        case ParquetDataFiberCompressedHeader(false, true, _) =>
          // can to this branch ?
          column.putNulls(0, num)
        case ParquetDataFiberCompressedHeader(true, true, _) =>
          throw new OapException("error header status (true, true, _)")
        case other => throw new OapException(s"impossible header status $other.")
      }
    } else {
      column.setDictionary(null)
      header match {
        case ParquetDataFiberCompressedHeader(true, false, _) =>
          val dataNativeAddress = fiberCache.fiberData.baseOffset
          readBatch(dataNativeAddress, start, num, column)
        case ParquetDataFiberCompressedHeader(false, false, _) =>
          val nullsNativeAddress = fiberCache.fiberData.baseOffset
          Platform.copyMemory(baseObject,
            fiberCache.fiberData.baseOffset + start, column.getNulls,
            Platform.BYTE_ARRAY_OFFSET, num)
          val dataNativeAddress = nullsNativeAddress + 1 * total
          readBatch(dataNativeAddress, start, num, column)
        case ParquetDataFiberCompressedHeader(false, true, _) =>
          column.putNulls(0, num)
        case ParquetDataFiberCompressedHeader(true, true, _) =>
          throw new OapException("error header status (true, true, _)")
        case other => throw new OapException(s"impossible header status $other.")
      }
    }
  }

  /**
   * Read ParquetDataFiberCompressedHeader and dictionary from data fiber.
   */
  private def readRowGroupMetas(): Unit = {
    header = ParquetDataFiberCompressedHeader(address, fiberCache)
    header match {
      case ParquetDataFiberCompressedHeader(_, _, 0) =>
        dictionary = null
      case ParquetDataFiberCompressedHeader(false, true, _) =>
        dictionary = null
      case ParquetDataFiberCompressedHeader(true, false, dicLength) =>
        val fiberBatchedInfo = fiberCache.fiberBatchedInfo
        val compressedSize = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
        val lastEndAddress = fiberBatchedInfo.get(total / compressedSize).get._2
        val firstStartAddress = fiberBatchedInfo.get(0).get._1
        val length = lastEndAddress - firstStartAddress
        val dicNativeAddress = address + ParquetDataFiberCompressedHeader.defaultSize + length
        dictionary =
          new ParquetDictionaryWrapper(readDictionary(dataType, dicLength, dicNativeAddress))
      case ParquetDataFiberCompressedHeader(false, false, dicLength) =>
        val fiberBatchedInfo = fiberCache.fiberBatchedInfo
        val compressedSize = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
        val lastEndAddress = fiberBatchedInfo.get(total / compressedSize).get._2
        val firstStartAddress = fiberBatchedInfo.get(0).get._1
        val length = lastEndAddress - firstStartAddress

        val dicNativeAddress = address +
          ParquetDataFiberCompressedHeader.defaultSize + 1 * total + length
        dictionary =
          new ParquetDictionaryWrapper(readDictionary(dataType, dicLength, dicNativeAddress))
      case ParquetDataFiberCompressedHeader(true, true, _) =>
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
    // support get the fiber cache from on heap and off heap memory
    val baseObject = if (fiberCache != null) {
      fiberCache.fiberData.baseObject
    } else {
      null
    }

    def readBinaryToColumnVector(): Unit = {
      Platform.copyMemory(baseObject,
        dataNativeAddress,
        column.getArrayLengths, Platform.INT_ARRAY_OFFSET, num * 4)
      Platform.copyMemory(
        baseObject,
        dataNativeAddress + num * 4,
        column.getArrayOffsets, Platform.INT_ARRAY_OFFSET, num * 4)
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
        Platform.copyMemory(baseObject,
          dataNativeAddress + num * 8 + startOffset,
          data, Platform.BYTE_ARRAY_OFFSET, data.length)
        column.getChild(0).asInstanceOf[OnHeapColumnVector].setByteData(data)
      }
    }

    dataType match {
      case ByteType | BooleanType =>
        Platform.copyMemory(baseObject, dataNativeAddress, column.getByteData,
          Platform.BYTE_ARRAY_OFFSET, num)
      case ShortType =>
        Platform.copyMemory(baseObject, dataNativeAddress, column.getShortData,
          Platform.BYTE_ARRAY_OFFSET, num * 2)
      case IntegerType | DateType =>
        Platform.copyMemory(baseObject, dataNativeAddress, column.getIntData,
          Platform.BYTE_ARRAY_OFFSET, num * 4)
      case FloatType =>
        Platform.copyMemory(baseObject, dataNativeAddress, column.getFloatData,
          Platform.BYTE_ARRAY_OFFSET, num * 4)
      case LongType | TimestampType =>
        Platform.copyMemory(baseObject, dataNativeAddress, column.getLongData,
          Platform.BYTE_ARRAY_OFFSET, num * 8)
      case DoubleType =>
        Platform.copyMemory(baseObject, dataNativeAddress, column.getDoubleData,
          Platform.BYTE_ARRAY_OFFSET, num * 8)
      case BinaryType | StringType => readBinaryToColumnVector()
      // if DecimalType.is32BitDecimalType(other) as int data type.
      case other if DecimalType.is32BitDecimalType(other) =>
        Platform.copyMemory(baseObject, dataNativeAddress, column.getIntData,
          Platform.BYTE_ARRAY_OFFSET, num * 4)
      // if DecimalType.is64BitDecimalType(other) as long data type.
      case other if DecimalType.is64BitDecimalType(other) =>
        Platform.copyMemory(baseObject, dataNativeAddress, column.getLongData,
          Platform.BYTE_ARRAY_OFFSET, num * 8)
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
    // support get the fiber cache from on heap and off heap memory
    val baseObject = if (fiberCache != null) {
      fiberCache.fiberData.baseObject
    } else {
      null
    }
    def readBinaryDictionary: Dictionary = {
      val binaryDictionaryContent = new Array[Binary](dicLength)
      val lengthsArray = new Array[Int](dicLength)
      Platform.copyMemory(baseObject, dicNativeAddress,
        lengthsArray, Platform.INT_ARRAY_OFFSET, dicLength * 4)
      val dictionaryBytesLength = lengthsArray.sum
      val dictionaryBytes = new Array[Byte](dictionaryBytesLength)
      Platform.copyMemory(baseObject,
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
    }

    dataType match {
      // ByteType, ShortType, IntegerType, DateType Dictionary read as Int type array.
      case ByteType | ShortType | IntegerType | DateType =>
        val intDictionaryContent = new Array[Int](dicLength)
        Platform.copyMemory(baseObject,
          dicNativeAddress, intDictionaryContent, Platform.INT_ARRAY_OFFSET, dicLength * 4)
        IntegerDictionary(intDictionaryContent)
      // FloatType Dictionary read as Float type array.
      case FloatType =>
        val floatDictionaryContent = new Array[Float](dicLength)
        Platform.copyMemory(baseObject,
          dicNativeAddress, floatDictionaryContent, Platform.FLOAT_ARRAY_OFFSET, dicLength * 4)
        FloatDictionary(floatDictionaryContent)
      // LongType Dictionary read as Long type array.
      case LongType | TimestampType =>
        val longDictionaryContent = new Array[Long](dicLength)
        Platform.copyMemory(baseObject,
          dicNativeAddress, longDictionaryContent, Platform.LONG_ARRAY_OFFSET, dicLength * 8)
        LongDictionary(longDictionaryContent)
      // DoubleType Dictionary read as Double type array.
      case DoubleType =>
        val doubleDictionaryContent = new Array[Double](dicLength)
        Platform.copyMemory(baseObject,
          dicNativeAddress, doubleDictionaryContent, Platform.DOUBLE_ARRAY_OFFSET, dicLength * 8)
        DoubleDictionary(doubleDictionaryContent)
      // StringType, BinaryType Dictionary read as a Int array and Byte array,
      // we use int array record offset and length of Byte array and use a shared backend
      // Byte array to construct all Binary.
      case StringType | BinaryType => readBinaryDictionary
      // if DecimalType.is32BitDecimalType(other) as int data type.
      case other if DecimalType.is32BitDecimalType(other) =>
        val intDictionaryContent = new Array[Int](dicLength)
        Platform.copyMemory(baseObject,
          dicNativeAddress, intDictionaryContent, Platform.INT_ARRAY_OFFSET, dicLength * 4)
        IntegerDictionary(intDictionaryContent)
      // if DecimalType.is64BitDecimalType(other) as long data type.
      case other if DecimalType.is64BitDecimalType(other) =>
        val longDictionaryContent = new Array[Long](dicLength)
        Platform.copyMemory(baseObject,
          dicNativeAddress, longDictionaryContent, Platform.LONG_ARRAY_OFFSET, dicLength * 8)
        LongDictionary(longDictionaryContent)
      // if DecimalType.isByteArrayDecimalType(other) as binary data type.
      case other if DecimalType.isByteArrayDecimalType(other) => readBinaryDictionary
      case other => throw new OapException(s"$other data type is not support dictionary.")
    }
  }
}

object ParquetDataFiberCompressedReader {
  def apply(address: Long, dataType: DataType, total: Int,
      fiberCache: FiberCache): ParquetDataFiberCompressedReader = {
    val reader = new ParquetDataFiberCompressedReader(address, dataType, total, fiberCache)
    reader.readRowGroupMetas()
    reader
  }
}

/**
 * Define a `ParquetDataFiberCompressedHeader` to record data fiber status.
 * @param noNulls status represent no null value in this data fiber.
 * @param allNulls status represent all value are null in this data fiber.
 * @param dicLength dictionary length of this data fiber, if 0 represent there is no dictionary.
 */
case class ParquetDataFiberCompressedHeader(noNulls: Boolean, allNulls: Boolean, dicLength: Int) {

  /**
   * Write ParquetDataFiberCompressedHeader to Fiber
   * @param address dataFiber address offset
   * @return dataFiber address offset
   */
  def writeToCache(address: Long): Long = {
    Platform.putBoolean(null, address, noNulls)
    Platform.putBoolean(null, address + 1, allNulls)
    Platform.putInt(null, address + 2, dicLength)
    address + ParquetDataFiberCompressedHeader.defaultSize
  }
}

/**
 * Use to construct ParquetDataFiberCompressedHeader instance.
 */
object ParquetDataFiberCompressedHeader {

  def apply(vector: OnHeapColumnVector, total: Int): ParquetDataFiberCompressedHeader = {
    val numNulls = vector.numNulls
    val allNulls = numNulls == total
    val noNulls = numNulls == 0
    val dicLength = vector.dictionaryLength
    new ParquetDataFiberCompressedHeader(noNulls, allNulls, dicLength)
  }

  def apply(nativeAddress: Long, fiberCache: FiberCache): ParquetDataFiberCompressedHeader = {
    val baseObject = if (fiberCache != null) {
      fiberCache.fiberData.baseObject
    } else {
      null
    }
    val noNulls = Platform.getBoolean(baseObject, nativeAddress)
    val allNulls = Platform.getBoolean(baseObject, nativeAddress + 1)
    val dicLength = Platform.getInt(baseObject, nativeAddress + 2)
    new ParquetDataFiberCompressedHeader(noNulls, allNulls, dicLength)
  }

  /**
   * allNulls: Boolean: 1
   * noNulls: Boolean: 1
   * dicLength: Int: 4
   * @return 1 + 1 + 4
   */
  def defaultSize: Int = 6
}
