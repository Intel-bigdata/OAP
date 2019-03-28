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
import org.apache.parquet.io.api.Binary
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.{DecompressBatchedFiberCache, FiberCache, MemoryBlockHolder}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetDictionaryWrapper, VectorizedColumnReader}
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

  def dumpToCache(reader: VectorizedColumnReader, total: Int, dataType: DataType): FiberCache = {
    val dictionary = reader.getPageReader.readDictionaryPage
    if (dictionary == null) {
      dumpDataToFiber(reader, total, dataType)
    } else {
      dumpDataAndDicToFiber(reader, total, dataType)
    }
  }

  /**
   * Write nulls data to data fiber.
   */
  private def dumpNullsToFiber(
      nativeAddress: Long, nulls: Array[Byte], total: Int): Long = {
    Platform.copyMemory(nulls,
      Platform.BYTE_ARRAY_OFFSET, null, nativeAddress, total)
    nativeAddress + total
  }

  /**
   * noNulls is true, nulls are all 0, not dump nulls to cache,
   * allNulls is false, need dump to cache,
   * dicLength is 0, needn't calculate dictionary part.
   */
  private def dumpDataToFiber(
      reader: VectorizedColumnReader,
      total: Int,
      dataType: DataType): FiberCache = {

    val compressedLength =
      OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize

    def compressedBatchFiber(): FiberCache = {
      val compressedUnitSize = math.ceil(total * 1.0 / compressedLength).toInt
      val arrayBytes: Array[Array[Byte]] = new Array[Array[Byte]](compressedUnitSize)
      var compressedSize = 0
      var count = 0
      var loadedRowCount = 0
      val batchCompressed = new Array[Boolean](compressedUnitSize)
      val compressor = OapRuntime.getOrCreate.fiberCacheManager.compressor
      var childColumnVectorLengths: Array[Int] = null
      var numNulls = 0
      val nulls: Array[Byte] = new Array[Byte](total)
      dataType match {
        case ByteType | BooleanType =>
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val column = new OnHeapColumnVector(num, dataType)
            reader.readBatch(num, column)
            val bytes = column.getByteData
            val rawBytes: Array[Byte] = new Array[Byte](num)
            Platform.copyMemory(bytes,
              Platform.BYTE_ARRAY_OFFSET,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num)
            val compressedBytes = compressor.compress(rawBytes)
            arrayBytes(count) = if (compressedBytes.length > rawBytes.length) {
              rawBytes
            } else {
              batchCompressed(count) = true
              compressedBytes
            }

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            numNulls += column.numNulls()
            nulls ++ column.getNulls
            count += 1
          }
        case ShortType =>
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val column = new OnHeapColumnVector(num, dataType)
            reader.readBatch(num, column)
            val shortData = column.getShortData
            val rawBytes: Array[Byte] = new Array[Byte](num * 2)
            Platform.copyMemory(shortData,
              Platform.SHORT_ARRAY_OFFSET,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 2)
            val compressedBytes = compressor.compress(rawBytes)
            arrayBytes(count) = if (compressedBytes.length > rawBytes.length) {
              rawBytes
            } else {
              batchCompressed(count) = true
              compressedBytes
            }

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            numNulls += column.numNulls()
            nulls ++ column.getNulls
            count += 1
          }
        case IntegerType | DateType =>
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val column = new OnHeapColumnVector(num, dataType)
            reader.readBatch(num, column)
            val intData = column.getIntData
            val rawBytes: Array[Byte] = new Array[Byte](num * 4)
            Platform.copyMemory(intData,
              Platform.INT_ARRAY_OFFSET,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 4)
            val compressedBytes = compressor.compress(rawBytes)
            arrayBytes(count) = if (compressedBytes.length > rawBytes.length) {
              rawBytes
            } else {
              batchCompressed(count) = true
              compressedBytes
            }

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            numNulls += column.numNulls()
            nulls ++ column.getNulls
            count += 1
          }
        case FloatType =>
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val column = new OnHeapColumnVector(num, dataType)
            reader.readBatch(num, column)
            val floatData = column.getFloatData
            val rawBytes: Array[Byte] = new Array[Byte](num * 4)
            Platform.copyMemory(floatData,
              Platform.FLOAT_ARRAY_OFFSET,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 4)
            val compressedBytes = compressor.compress(rawBytes)
            arrayBytes(count) = if (compressedBytes.length > rawBytes.length) {
              rawBytes
            } else {
              batchCompressed(count) = true
              compressedBytes
            }

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            numNulls += column.numNulls()
            nulls ++ column.getNulls
            count += 1
          }
        case LongType | TimestampType =>
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val column = new OnHeapColumnVector(num, dataType)
            reader.readBatch(num, column)
            val longData = column.getLongData
            val rawBytes: Array[Byte] = new Array[Byte](num * 8)
            Platform.copyMemory(longData,
              Platform.LONG_ARRAY_OFFSET,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 8)
            val compressedBytes = compressor.compress(rawBytes)
            arrayBytes(count) = if (compressedBytes.length > rawBytes.length) {
              rawBytes
            } else {
              batchCompressed(count) = true
              compressedBytes
            }

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            numNulls += column.numNulls()
            nulls ++ column.getNulls
            count += 1
          }
        case DoubleType =>
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val column = new OnHeapColumnVector(num, dataType)
            reader.readBatch(num, column)
            val doubleData = column.getDoubleData
            val rawBytes: Array[Byte] = new Array[Byte](num * 8)
            Platform.copyMemory(doubleData,
              Platform.DOUBLE_ARRAY_OFFSET,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 8)
            val compressedBytes = compressor.compress(rawBytes)
            arrayBytes(count) = if (compressedBytes.length > rawBytes.length) {
              rawBytes
            } else {
              batchCompressed(count) = true
              compressedBytes
            }

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            numNulls += column.numNulls()
            nulls ++ column.getNulls
            count += 1
          }
        case StringType | BinaryType =>
          childColumnVectorLengths = new Array[Int](compressedUnitSize)
          while (count < compressedUnitSize) {
            val num = Math.min(compressedLength, total - loadedRowCount)
            val column = new OnHeapColumnVector(num, dataType)
            reader.readBatch(num, column)

            val arrayLengths: Array[Int] = column.getArrayLengths
            val arrayOffsets: Array[Int] = column.getArrayOffsets
            val childBytes: Array[Byte] = column.getChild(0)
              .asInstanceOf[OnHeapColumnVector].getByteData

            var lastIndex = num  - 1
            while (lastIndex >= 0 && column.isNullAt(lastIndex)) {
              lastIndex -= 1
            }
            var firstIndex = 0
            while (firstIndex < num && column.isNullAt(firstIndex)) {
              firstIndex += 1
            }
            val startOffsets = arrayOffsets(firstIndex)
            val lastOffsets = arrayOffsets(lastIndex)
            childColumnVectorLengths(count) = lastOffsets - startOffsets + arrayLengths(lastIndex)

            val rawBytes = new Array[Byte](num * 8 + childColumnVectorLengths(count))
            Platform.copyMemory(arrayLengths,
              Platform.INT_ARRAY_OFFSET,
              rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 4)

            Platform.copyMemory(arrayOffsets,
              Platform.INT_ARRAY_OFFSET,
              rawBytes, Platform.BYTE_ARRAY_OFFSET + num * 4, num * 4)

            Platform.copyMemory(childBytes, Platform.BYTE_ARRAY_OFFSET + startOffsets,
              rawBytes, Platform.BYTE_ARRAY_OFFSET + num * 8, childColumnVectorLengths(count) )

            val compressedBytes = compressor.compress(rawBytes)
            arrayBytes(count) = if (compressedBytes.length > rawBytes.length) {
              rawBytes
            } else {
              batchCompressed(count) = true
              compressedBytes
            }

            compressedSize += arrayBytes(count).length
            loadedRowCount += compressedLength
            numNulls += column.numNulls()
            nulls ++ column.getNulls
            count += 1
          }
      }

      val header = ParquetDataFiberCompressedHeader(numNulls == 0, numNulls == total, 0)
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
          nulls, total)
        fiber.nullSize = total
        (fiber, nullAddress)
      }

      val fiberBatchedInfo = new mutable.HashMap[Int, (Long, Long, Boolean, Long)]()
      var startAddress = nativeAddress
      var batchCount = 0
      while (batchCount < compressedUnitSize) {
        Platform.copyMemory(arrayBytes(batchCount), Platform.BYTE_ARRAY_OFFSET,
          null, startAddress, arrayBytes(batchCount).length)
        if (dataType == StringType) {
          fiberBatchedInfo.put(batchCount,
            (startAddress, startAddress + arrayBytes(batchCount).length,
              batchCompressed(batchCount), childColumnVectorLengths(batchCount)))
        } else {
          fiberBatchedInfo.put(batchCount,
            (startAddress, startAddress + arrayBytes(batchCount).length,
              batchCompressed(batchCount), 0))
        }

        startAddress = startAddress + arrayBytes(batchCount).length
        batchCount += 1
      }

      // record the compressed parameter to fiber
      fiber.fiberBatchedInfo = fiberBatchedInfo
      fiber
    }

    dataType match {
      case ByteType | BooleanType =>
        compressedBatchFiber()
      case ShortType =>
        compressedBatchFiber()
      case IntegerType | DateType =>
        compressedBatchFiber()
      case FloatType =>
        compressedBatchFiber()
      case LongType | TimestampType =>
        compressedBatchFiber()
      case DoubleType =>
        compressedBatchFiber()
      case StringType | BinaryType =>
        compressedBatchFiber()
      case other if DecimalType.is32BitDecimalType(other) =>
        compressedBatchFiber()
      case other if DecimalType.is64BitDecimalType(other) =>
        compressedBatchFiber()
      case other if DecimalType.isByteArrayDecimalType(other) =>
        compressedBatchFiber()
      case other => throw new OapException(s"$other data type is not support data cache")
    }
  }

  /**
   * Write dictionaryIds(int array) and Dictionary data to data fiber.
   */
  private def dumpDataAndDicToFiber(
      reader: VectorizedColumnReader, total: Int, dataType: DataType): FiberCache = {


    def compressedBatchDataAndDicToFiber(dataType: DataType) : FiberCache = {
      val compressedLength =
        OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
      val compressedUnitSize = math.ceil(total * 1.0 / compressedLength).toInt
      val arrayBytes: Array[Array[Byte]] = new Array[Array[Byte]](compressedUnitSize)
      var compressedSize = 0
      var count = 0
      var loadedRowCount = 0
      val batchCompressed = new Array[Boolean](compressedUnitSize)

      val compressor = OapRuntime.getOrCreate.fiberCacheManager.compressor
      var column: OnHeapColumnVector = null
      var numNulls = 0
      val nulls: Array[Byte] = new Array[Byte](total)
      while (count < compressedUnitSize) {
        val num = Math.min(compressedLength, total - loadedRowCount)
        column = new OnHeapColumnVector(num, dataType)
        reader.readBatch(num, column)
        val dictionaryIds = column.getDictionaryIds.asInstanceOf[OnHeapColumnVector].getIntData
        val rawBytes = new Array[Byte](num * 4)
        Platform.copyMemory(dictionaryIds,
          Platform.INT_ARRAY_OFFSET,
          rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 4)

        arrayBytes(count) = compressor.compress(rawBytes)
        batchCompressed(count) = true

        compressedSize += arrayBytes(count).length
        loadedRowCount += compressedLength
        numNulls += column.numNulls()
        nulls ++ column.getNulls
        count += 1
      }
      val dictionary = column.getDictionary
      val dicLength = if (dictionary != null && dictionary.isInstanceOf[ParquetDictionaryWrapper]) {
        (dictionary.asInstanceOf[ParquetDictionaryWrapper]).getMaxId + 1
      } else 0

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
      val header = ParquetDataFiberCompressedHeader(numNulls == 0, numNulls == total, 0)
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
          nulls, total)
        fiber.nullSize = total
        (fiber, nullAddress)
      }

      val fiberBatchedInfo = new mutable.HashMap[Int, (Long, Long, Boolean, Long)]()
      var startAddress = nativeAddress
      var batchCount = 0
      while (batchCount < compressedUnitSize) {
        Platform.copyMemory(arrayBytes(batchCount), Platform.BYTE_ARRAY_OFFSET,
          null, startAddress, arrayBytes(batchCount).length)
        fiberBatchedInfo.put(batchCount,
          (startAddress, startAddress + arrayBytes(batchCount).length,
            batchCompressed(batchCount), 0))
        startAddress = startAddress + arrayBytes(batchCount).length
        batchCount += 1
      }

      val address = fiberBatchedInfo(compressedUnitSize -1)._2

      Platform.copyMemory(dictionaryBytes, Platform.BYTE_ARRAY_OFFSET,
        null, address, dictionaryBytes.length)
      // record the compressed parameter to fiber
      fiber.fiberBatchedInfo = fiberBatchedInfo
      fiber
    }
    // dump dictionary to data fiber case by dataType.
    dataType match {
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
    // decompress the compressed fiber cache
    val decompressedFiberCache = decompressFiberCache(fiberCache, column, start, num)

    val baseObject = decompressedFiberCache.fiberData.baseObject

    if (dictionary != null) {
      // Use dictionary encode, value store in dictionaryIds, it's a int array.
      column.setDictionary(dictionary)
      val dictionaryIds = column.reserveDictionaryIds(num).asInstanceOf[OnHeapColumnVector]
      header match {
        case ParquetDataFiberCompressedHeader(true, false, _) =>
          val dataNativeAddress = decompressedFiberCache.fiberData.baseOffset
          Platform.copyMemory(baseObject,
            dataNativeAddress,
            dictionaryIds.getIntData, Platform.INT_ARRAY_OFFSET, num * 4)
        case ParquetDataFiberCompressedHeader(false, false, _) =>
          val nullsNativeAddress = decompressedFiberCache.fiberData.baseOffset
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
          val dataNativeAddress = decompressedFiberCache.fiberData.baseOffset
          readBatch(decompressedFiberCache, dataNativeAddress, num, column)
        case ParquetDataFiberCompressedHeader(false, false, _) =>
          val nullsNativeAddress = decompressedFiberCache.fiberData.baseOffset
          Platform.copyMemory(baseObject,
            fiberCache.fiberData.baseOffset + start, column.getNulls,
            Platform.BYTE_ARRAY_OFFSET, num)
          val dataNativeAddress = nullsNativeAddress + 1 * total
          readBatch(decompressedFiberCache, dataNativeAddress, num, column)
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
        val lastEndAddress = fiberBatchedInfo(total / compressedSize)._2
        val firstStartAddress = fiberBatchedInfo(0)._1
        val length = lastEndAddress - firstStartAddress
        val dicNativeAddress = address + ParquetDataFiberCompressedHeader.defaultSize + length
        dictionary =
          new ParquetDictionaryWrapper(readDictionary(dataType, dicLength, dicNativeAddress))
      case ParquetDataFiberCompressedHeader(false, false, dicLength) =>
        val fiberBatchedInfo = fiberCache.fiberBatchedInfo
        val compressedSize = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
        val lastEndAddress = fiberBatchedInfo(total / compressedSize)._2
        val firstStartAddress = fiberBatchedInfo(0)._1
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
  def readBatch(
      decompressedFiberCache: FiberCache, dataNativeAddress: Long,
      num: Int, column: OnHeapColumnVector): Unit = {
    val baseObject = decompressedFiberCache.fiberData.baseObject

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
        val length = column.getArrayOffset(lastIndex) -
          column.getArrayOffset(firstIndex) + column.getArrayLength(lastIndex)
        val data = new Array[Byte](length)
        Platform.copyMemory(baseObject,
          dataNativeAddress + num * 8 + column.getArrayOffset(firstIndex),
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

  def decompressFiberCache(
      compressedFiberCache: FiberCache,
      columnVector: OnHeapColumnVector,
      start: Int, num: Int): FiberCache = {
    val defaultCapacity = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
    val fiberBatchedInfo = compressedFiberCache.fiberBatchedInfo(start / defaultCapacity)
    val decompress = OapRuntime.getOrCreate.fiberCacheManager.decompressor

    if (fiberBatchedInfo._3 && decompress != null) {
      val fiberCache = compressedFiberCache
      val startAddress = fiberBatchedInfo._1
      val endAddress = fiberBatchedInfo._2
      val length = endAddress - startAddress
      val compressedBytes = new Array[Byte](length.toInt)

      Platform.copyMemory(null,
        startAddress,
        compressedBytes, Platform.BYTE_ARRAY_OFFSET, length)

      val decompressedBytesLength = if (columnVector.hasDictionary) {
        num * 4
      } else {
        columnVector.dataType() match {
          case ByteType | BooleanType =>
            num
          case ShortType =>
            num * 2
          case IntegerType | DateType | FloatType =>
            num * 4
          case LongType | TimestampType | DoubleType =>
            num * 8
          case BinaryType | StringType =>
            num * 8 + fiberBatchedInfo._4.toInt
          // if DecimalType.is32BitDecimalType(other) as int data type.
          case other if DecimalType.is32BitDecimalType(other) =>
            num * 4
          // if DecimalType.is64BitDecimalType(other) as long data type.
          case other if DecimalType.is64BitDecimalType(other) =>
            num * 8
          // if DecimalType.isByteArrayDecimalType(other) as binary data type.
          case other if DecimalType.isByteArrayDecimalType(other) =>
            num * 8 + fiberBatchedInfo._4.toInt
          case other => throw new OapException(s"impossible data type $other.")
        }
      }

      var decompressedBytes = decompress.decompress(compressedBytes,
        decompressedBytesLength)

      var nullsBytes: Array[Byte] = null
      if (fiberCache.nullSize > 0) {
        // current nulls no compress because it is not big
        nullsBytes = new Array[Byte](fiberCache.nullSize)
        Platform.copyMemory(null, fiberCache.getBaseOffset + 6,
          nullsBytes, Platform.BYTE_ARRAY_OFFSET, fiberCache.nullSize)
      }
      if (nullsBytes != null) {
        decompressedBytes = nullsBytes ++ decompressedBytes
      }
      val memoryBlockHolder = new MemoryBlockHolder(
        decompressedBytes, Platform.BYTE_ARRAY_OFFSET,
        decompressedBytes.length, decompressedBytes.length)

      val fiberCacheReturned = if (num < defaultCapacity) {
        new DecompressBatchedFiberCache(memoryBlockHolder, fiberBatchedInfo._3, fiberCache)
      } else {
        new DecompressBatchedFiberCache(memoryBlockHolder, fiberBatchedInfo._3, null)
      }
      fiberCacheReturned.batchedCompressed = fiberBatchedInfo._3
      fiberCacheReturned
    } else {
      compressedFiberCache
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
    }

    dataType match {
      // ByteType, ShortType, IntegerType, DateType Dictionary read as Int type array.
      case ByteType | ShortType | IntegerType | DateType =>
        val intDictionaryContent = new Array[Int](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, intDictionaryContent, Platform.INT_ARRAY_OFFSET, dicLength * 4)
        IntegerDictionary(intDictionaryContent)
      // FloatType Dictionary read as Float type array.
      case FloatType =>
        val floatDictionaryContent = new Array[Float](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, floatDictionaryContent, Platform.FLOAT_ARRAY_OFFSET, dicLength * 4)
        FloatDictionary(floatDictionaryContent)
      // LongType Dictionary read as Long type array.
      case LongType | TimestampType =>
        val longDictionaryContent = new Array[Long](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, longDictionaryContent, Platform.LONG_ARRAY_OFFSET, dicLength * 8)
        LongDictionary(longDictionaryContent)
      // DoubleType Dictionary read as Double type array.
      case DoubleType =>
        val doubleDictionaryContent = new Array[Double](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, doubleDictionaryContent, Platform.DOUBLE_ARRAY_OFFSET, dicLength * 8)
        DoubleDictionary(doubleDictionaryContent)
      // StringType, BinaryType Dictionary read as a Int array and Byte array,
      // we use int array record offset and length of Byte array and use a shared backend
      // Byte array to construct all Binary.
      case StringType | BinaryType => readBinaryDictionary
      // if DecimalType.is32BitDecimalType(other) as int data type.
      case other if DecimalType.is32BitDecimalType(other) =>
        val intDictionaryContent = new Array[Int](dicLength)
        Platform.copyMemory(null,
          dicNativeAddress, intDictionaryContent, Platform.INT_ARRAY_OFFSET, dicLength * 4)
        IntegerDictionary(intDictionaryContent)
      // if DecimalType.is64BitDecimalType(other) as long data type.
      case other if DecimalType.is64BitDecimalType(other) =>
        val longDictionaryContent = new Array[Long](dicLength)
        Platform.copyMemory(null,
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

    val noNulls = Platform.getBoolean(null, nativeAddress)
    val allNulls = Platform.getBoolean(null, nativeAddress + 1)
    val dicLength = Platform.getInt(null, nativeAddress + 2)
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
