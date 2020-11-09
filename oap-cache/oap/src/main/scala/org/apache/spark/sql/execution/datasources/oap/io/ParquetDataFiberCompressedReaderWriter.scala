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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}

import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.Binary
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.io.{CompressionCodec => SparkCompressionCodec}
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetDictionaryWrapper, SkippableVectorizedColumnReader}
import org.apache.spark.sql.execution.vectorized.OapOnHeapColumnVector
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

  val compressedLength =
    OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
  val codecName = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionCodec
  val compressionCodec = SparkCompressionCodec.createCodec(new SparkConf(), codecName)

  def dumpToCache(reader: SkippableVectorizedColumnReader,
      total: Int, dataType: DataType, fiberId: FiberId = null): FiberCache = {
    // For the dictionary case, the column vector occurs some batch has dictionary
    // and some no dictionary. Therefore we still read the total value to cv instead of batch.
    // TODO: Next can split the read to column vector from total to batch?
    val column: OapOnHeapColumnVector = new OapOnHeapColumnVector(total, dataType)
    reader.readBatch(total, column)
    val dicLength = column.dictionaryLength()
    if (dicLength != 0) {
      dumpDataAndDicToFiber(column, total, dataType)
    } else {
      dumpDataToFiber(column, total, dataType)
    }
  }

  /**
   * Write nulls data to data fiber.
   */
  private def dumpNullsToFiber(nativeAddress: Long,
                               column: OapOnHeapColumnVector, total: Int): Long = {
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
                               column: OapOnHeapColumnVector,
                               total: Int,
                               dataType: DataType): FiberCache = {
    val compressedUnitSize = math.ceil(total * 1.0 / compressedLength).toInt
    val arrayBytes: Array[Array[Byte]] = new Array[Array[Byte]](compressedUnitSize)
    val batchCompressed = new Array[Boolean](compressedUnitSize)
    var childColumnVectorLengths: Array[Int] = null
    var compressedSize = 0
    var count = 0
    var loadedRowCount = 0
    def splitColumnVectorNoBinaryType(byteLength: Int, offset: Int): Unit = {
      val bytes =
        dataType match {
          case ByteType | BooleanType =>
            column.getByteData
          case ShortType =>
            column.getShortData
          case IntegerType | DateType =>
            column.getIntData
          case FloatType =>
            column.getFloatData
          case LongType | TimestampType =>
            column.getLongData
          case DoubleType =>
            column.getDoubleData
        }

      val compressedOutBuffer = new ByteArrayOutputStream()
      while (count < compressedUnitSize) {
        val num = Math.min(compressedLength, total - loadedRowCount)
        val rawBytes: Array[Byte] = new Array[Byte](num * byteLength)
        Platform.copyMemory(bytes,
          offset + loadedRowCount * byteLength,
          rawBytes, Platform.BYTE_ARRAY_OFFSET, num * byteLength)

        // convert the byte to the stream
        compressedOutBuffer.reset()
        val cos = compressionCodec.compressedOutputStream(compressedOutBuffer)
        cos.write(rawBytes)
        cos.close()
        val compressedBytes = compressedOutBuffer.toByteArray

        // if the compressed size is large than the decompressed size, skip the compress operator
        arrayBytes(count) = if (compressedBytes.length > rawBytes.length) {
          rawBytes
        } else {
          batchCompressed(count) = true
          compressedBytes
        }
        compressedSize += arrayBytes(count).length
        loadedRowCount += num
        count += 1
      }
    }

    def splitColumnVectorBinaryType(): Unit = {
      childColumnVectorLengths = new Array[Int](compressedUnitSize)
      val arrayLengths = column.getArrayLengths
      val arrayOffsets = column.getArrayOffsets
      val childBytes = column.getChild(0)
        .asInstanceOf[OapOnHeapColumnVector].getByteData
      val compressedOutBuffer = new ByteArrayOutputStream()
      while (count < compressedUnitSize) {
        val num = Math.min(compressedLength, total - loadedRowCount)
        val batchArrayLengths: Array[Int] = new Array[Int](num)
        val batchArrayOffsets: Array[Int] = new Array[Int](num)
        Platform.copyMemory(arrayLengths,
          Platform.INT_ARRAY_OFFSET + loadedRowCount * 4,
          batchArrayLengths, Platform.INT_ARRAY_OFFSET, num * 4)
        Platform.copyMemory(arrayOffsets,
          Platform.INT_ARRAY_OFFSET + loadedRowCount * 4,
          batchArrayOffsets, Platform.INT_ARRAY_OFFSET, num * 4)
        // check whether the batch column vector is null
        var lastIndex = num + loadedRowCount - 1
        while (lastIndex >= loadedRowCount && column.isNullAt(lastIndex)) {
          lastIndex -= 1
        }
        var firstIndex = loadedRowCount
        while (firstIndex < num + loadedRowCount && column.isNullAt(firstIndex)) {
          firstIndex += 1
        }
        firstIndex = firstIndex - loadedRowCount
        lastIndex = lastIndex - loadedRowCount
        if (firstIndex >= 0 && firstIndex < num && lastIndex >= 0 && lastIndex < num) {
          val startOffsets = batchArrayOffsets(firstIndex)
          val lastOffsets = batchArrayOffsets(lastIndex)
          for (idx <- firstIndex to lastIndex) {
            if (!column.isNullAt(idx + loadedRowCount)) {
              batchArrayOffsets(idx) -= startOffsets
            }
          }
          childColumnVectorLengths(count) =
            lastOffsets - startOffsets + batchArrayLengths(lastIndex)
          val rawBytes = new Array[Byte](num * 8 + childColumnVectorLengths(count))
          Platform.copyMemory(batchArrayLengths,
            Platform.INT_ARRAY_OFFSET,
            rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 4)
          Platform.copyMemory(batchArrayOffsets,
            Platform.INT_ARRAY_OFFSET,
            rawBytes, Platform.BYTE_ARRAY_OFFSET + num * 4, num * 4)
          Platform.copyMemory(childBytes, Platform.BYTE_ARRAY_OFFSET + startOffsets,
            rawBytes, Platform.BYTE_ARRAY_OFFSET + num * 8, childColumnVectorLengths(count))

          // convert the byte to the stream
          compressedOutBuffer.reset()
          val cos = compressionCodec.compressedOutputStream(compressedOutBuffer)
          cos.write(rawBytes)
          cos.close()
          val compressedBytes = compressedOutBuffer.toByteArray
          // if the compressed size is large than the decompressed size, skip the compress operator
          arrayBytes(count) = if (compressedBytes.length > rawBytes.length) {
            rawBytes
          } else {
            batchCompressed(count) = true
            compressedBytes
          }
          compressedSize += arrayBytes(count).length
        }
        loadedRowCount += num
        count += 1
      }
    }

    dataType match {
      case ByteType | BooleanType =>
        splitColumnVectorNoBinaryType(1, Platform.BYTE_ARRAY_OFFSET)
      case ShortType =>
        splitColumnVectorNoBinaryType(2, Platform.SHORT_ARRAY_OFFSET)
      case IntegerType | DateType =>
        splitColumnVectorNoBinaryType(4, Platform.INT_ARRAY_OFFSET)
      case FloatType =>
        splitColumnVectorNoBinaryType(4, Platform.FLOAT_ARRAY_OFFSET)
      case LongType | TimestampType =>
        splitColumnVectorNoBinaryType(8, Platform.LONG_ARRAY_OFFSET)
      case DoubleType =>
        splitColumnVectorNoBinaryType(8, Platform.LONG_ARRAY_OFFSET)
      case StringType | BinaryType =>
        splitColumnVectorBinaryType()
      case other if DecimalType.is32BitDecimalType(other) =>
        splitColumnVectorNoBinaryType(4, Platform.INT_ARRAY_OFFSET)
      case other if DecimalType.is64BitDecimalType(other) =>
        splitColumnVectorNoBinaryType(8, Platform.LONG_ARRAY_OFFSET)
      case other if DecimalType.isByteArrayDecimalType(other) =>
        splitColumnVectorBinaryType()
      case other => throw new OapException(s"$other data type is not support data cache.")
    }

    val header = ParquetDataFiberCompressedHeader(
      column.numNulls() == 0, column.numNulls() == total, 0)
    var fiber: FiberCache = null
    val fiberBatchedInfo = new mutable.HashMap[Int, CompressedBatchedFiberInfo]()
    if (!header.allNulls) {
      // handle the column vector is not all nulls
      val nativeAddress = if (header.noNulls) {
        val fiberLength = ParquetDataFiberCompressedHeader.defaultSize + compressedSize
        logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
        fiber = emptyDataFiber(fiberLength)
        header.writeToCache(fiber.getBaseOffset)
      } else {
        val fiberLength = ParquetDataFiberCompressedHeader.defaultSize +
          compressedSize + total
        logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
        fiber = emptyDataFiber(fiberLength)
        dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset), column, total)
      }

      var startAddress = nativeAddress
      var batchCount = 0
      while (batchCount < compressedUnitSize) {
        // the (arrayBytes(batchCount) maybe null if the type is StringType
        val length = if (arrayBytes(batchCount) != null) {
          arrayBytes(batchCount).length
        } else 0
        Platform.copyMemory(arrayBytes(batchCount), Platform.BYTE_ARRAY_OFFSET,
          null, startAddress, length)
        if (dataType == StringType || dataType == BinaryType) {
          fiberBatchedInfo.put(batchCount,
            (CompressedBatchedFiberInfo(startAddress, startAddress + length,
              batchCompressed(batchCount), childColumnVectorLengths(batchCount))))

        } else {
          fiberBatchedInfo.put(batchCount,
            (CompressedBatchedFiberInfo(startAddress, startAddress + length,
              batchCompressed(batchCount), 0)))
        }
        startAddress = startAddress + length
        batchCount += 1
      }
      fiber.fiberCompressed = true
      // record the compressed parameter to fiber
      fiber.fiberBatchedInfo = fiberBatchedInfo
    } else {
      // if the column vector is nulls, we only dump the header info to data fiber
      val fiberLength = ParquetDataFiberCompressedHeader.defaultSize
      logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
      fiber = emptyDataFiber(fiberLength)
      header.writeToCache(fiber.getBaseOffset)
    }
    fiber
  }

  /**
   * Write dictionaryIds(int array) and Dictionary data to data fiber.
   */
  private def dumpDataAndDicToFiber(
                                     column: OapOnHeapColumnVector, total: Int,
                                     dataType: DataType): FiberCache = {
    val compressedUnitSize = math.ceil(total * 1.0 / compressedLength).toInt
    val arrayBytes: Array[Array[Byte]] = new Array[Array[Byte]](compressedUnitSize)
    var compressedSize = 0
    var count = 0
    var loadedRowCount = 0
    val batchCompressed = new Array[Boolean](compressedUnitSize)

    val dictionaryIds = column.getDictionaryIds.asInstanceOf[OapOnHeapColumnVector].getIntData
    val compressedOutBuffer = new ByteArrayOutputStream()
    while (count < compressedUnitSize) {
      val num = Math.min(compressedLength, total - loadedRowCount)
      val rawBytes = new Array[Byte](num * 4)
      Platform.copyMemory(dictionaryIds,
        Platform.INT_ARRAY_OFFSET + loadedRowCount * 4,
        rawBytes, Platform.BYTE_ARRAY_OFFSET, num * 4)

      // convert the byte to the stream
      compressedOutBuffer.reset()
      val cos = compressionCodec.compressedOutputStream(compressedOutBuffer)
      cos.write(rawBytes)
      cos.close()
      val compressedBytes = compressedOutBuffer.toByteArray

      // if the compressed size is large than the decompressed size, skip the compress operator
      arrayBytes(count) = if (compressedBytes.length > rawBytes.length) {
        rawBytes
      } else {
        batchCompressed(count) = true
        compressedBytes
      }
      compressedSize += arrayBytes(count).length
      loadedRowCount += num
      count += 1
    }
    val dictionary = column.getDictionary
    val dicLength = column.dictionaryLength()
    var dictionaryBytes: Array[Byte] = null
    dataType match {
      case ByteType | ShortType | IntegerType | DateType =>
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
      case LongType | TimestampType =>
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
      case StringType | BinaryType =>
        val lengths = new Array[Int](dicLength)
        val bytes = new Array[Array[Byte]](dicLength)
        (0 until dicLength).foreach(id => {
          val binary = dictionary.decodeToBinary(id)
          lengths(id) = binary.length
          bytes(id) = binary
        })
        dictionaryBytes = new Array[Byte](dicLength * 4 + lengths.sum)
        Platform.copyMemory(lengths, Platform.INT_ARRAY_OFFSET,
          dictionaryBytes, Platform.BYTE_ARRAY_OFFSET, dicLength * 4)
        var address = dicLength * 4
        (0 until dicLength).foreach(id => {
          Platform.copyMemory(bytes(id), Platform.BYTE_ARRAY_OFFSET,
            dictionaryBytes, Platform.BYTE_ARRAY_OFFSET + address, lengths(id))
          address += lengths(id)
        })
      case other if DecimalType.is32BitDecimalType(other) =>
        val typeLength = 4
        val dictionaryContent = new Array[Int](dicLength)
        (0 until dicLength).foreach(id => dictionaryContent(id) = dictionary.decodeToInt(id))
        dictionaryBytes = new Array[Byte](dicLength * typeLength)
        Platform.copyMemory(dictionaryContent, Platform.INT_ARRAY_OFFSET, dictionaryBytes,
          Platform.BYTE_ARRAY_OFFSET, dicLength * typeLength)
      case other if DecimalType.is64BitDecimalType(other) =>
        val typeLength = 8
        val dictionaryContent = new Array[Long](dicLength)
        (0 until dicLength).foreach(id => dictionaryContent(id) = dictionary.decodeToLong(id))
        dictionaryBytes = new Array[Byte](dicLength * typeLength)
        Platform.copyMemory(dictionaryContent, Platform.LONG_ARRAY_OFFSET, dictionaryBytes,
          Platform.BYTE_ARRAY_OFFSET, dicLength * typeLength)
      case other if DecimalType.isByteArrayDecimalType(other) =>
        val lengths = new Array[Int](dicLength)
        val bytes = new Array[Array[Byte]](dicLength)
        (0 until dicLength).foreach(id => {
          val binary = dictionary.decodeToBinary(id)
          lengths(id) = binary.length
          bytes(id) = binary
        })
        dictionaryBytes = new Array[Byte](dicLength * 4 + lengths.sum)
        Platform.copyMemory(lengths, Platform.INT_ARRAY_OFFSET,
          dictionaryBytes, Platform.BYTE_ARRAY_OFFSET, dicLength * 4)
        var address = dicLength * 4
        (0 until dicLength).foreach(id => {
          Platform.copyMemory(bytes(id), Platform.BYTE_ARRAY_OFFSET,
            dictionaryBytes, Platform.BYTE_ARRAY_OFFSET + address, lengths(id))
          address += lengths(id)
        })
      case other => throw new OapException(s"$other data type is not support dictionary.")
    }
    // Currently the dictionaryBytes is not supported compressed.
    val header = ParquetDataFiberCompressedHeader(column.numNulls() == 0,
      column.numNulls() == total, dicLength)
    var fiber: FiberCache = null
    if (!header.allNulls) {
      val nativeAddress = if (header.noNulls) {
        val fiberLength = ParquetDataFiberCompressedHeader.defaultSize +
          compressedSize + dictionaryBytes.length
        logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
        fiber = emptyDataFiber(fiberLength)
        header.writeToCache(fiber.getBaseOffset)
      } else {
        val fiberLength = ParquetDataFiberCompressedHeader.defaultSize +
          compressedSize + dictionaryBytes.length + total
        logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
        fiber = emptyDataFiber(fiberLength)
        dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset), column, total)
      }

      val fiberBatchedInfo = new mutable.HashMap[Int, CompressedBatchedFiberInfo]()
      var startAddress = nativeAddress
      var batchCount = 0
      while (batchCount < compressedUnitSize) {
        Platform.copyMemory(arrayBytes(batchCount), Platform.BYTE_ARRAY_OFFSET,
          null, startAddress, arrayBytes(batchCount).length)
        fiberBatchedInfo.put(batchCount,
          CompressedBatchedFiberInfo(startAddress, startAddress + arrayBytes(batchCount).length,
          batchCompressed(batchCount), 0))
        startAddress = startAddress + arrayBytes(batchCount).length
        batchCount += 1
      }
      val address = fiberBatchedInfo(compressedUnitSize - 1).endAddress
      Platform.copyMemory(dictionaryBytes, Platform.BYTE_ARRAY_OFFSET,
        null, address, dictionaryBytes.length)
      // record the compressed parameter to fiber
      fiber.fiberBatchedInfo = fiberBatchedInfo
      fiber.fiberCompressed = true
    } else {
      // if the column vector is nulls, we only dump the header info to data fiber
      val fiberLength = ParquetDataFiberCompressedHeader.defaultSize
      logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
      fiber = emptyDataFiber(fiberLength)
      header.writeToCache(fiber.getBaseOffset)
    }
    fiber
  }

  private def emptyDataFiber(fiberLength: Long): FiberCache =
    OapRuntime.getOrCreate.fiberCacheManager.getEmptyDataFiberCache(fiberLength)
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
     address = address, dataType = dataType, total = total) {

  private var compressHeader: ParquetDataFiberCompressedHeader = _

  /**
   * Read num values to OnHeapColumnVector from data fiber by start position.
   * @param start data fiber start rowId position.
   * @param num need read values num.
   * @param column target OnHeapColumnVector.
   */
  override def readBatch(
      start: Int, num: Int, column: OapOnHeapColumnVector): Unit = {
    // decompress the compressed fiber cache
    val decompressedFiberCache = decompressFiberCache(fiberCache, column, start, num)
    val defaultCapacity = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
    val baseObject = decompressedFiberCache.fiberData.baseObject
    if (dictionary != null) {
      // Use dictionary encode, value store in dictionaryIds, it's a int array.
      column.setDictionary(dictionary)
      val dictionaryIds = column.reserveDictionaryIds(num).asInstanceOf[OapOnHeapColumnVector]
      compressHeader match {
        case ParquetDataFiberCompressedHeader(true, false, _) =>
          if (baseObject != null) {
            // the batch is compressed
            val dataNativeAddress = decompressedFiberCache.fiberData.baseOffset
            Platform.copyMemory(baseObject,
              dataNativeAddress,
              dictionaryIds.getIntData, Platform.INT_ARRAY_OFFSET, num * 4)
          } else {
            // the batch is not compressed
            val fiberBatchedInfo = fiberCache.fiberBatchedInfo(start / defaultCapacity)
            val startAddress = fiberBatchedInfo.startAddress
            val endAddress = fiberBatchedInfo.endAddress
            val length = endAddress - startAddress
            if (length == num * 4) {
              Platform.copyMemory(baseObject,
                startAddress,
                dictionaryIds.getIntData, Platform.INT_ARRAY_OFFSET, num * 4)
            }
          }
        case ParquetDataFiberCompressedHeader(false, false, _) =>
          if (baseObject != null) {
            // the batch is compressed
            val nullsNativeAddress = fiberCache.fiberData.baseOffset +
              ParquetDataFiberCompressedHeader.defaultSize
            Platform.copyMemory(fiberCache.fiberData.baseObject,
              nullsNativeAddress + start, column.getNulls, Platform.BYTE_ARRAY_OFFSET, num)
            val dataNativeAddress = decompressedFiberCache.fiberData.baseOffset
            Platform.copyMemory(baseObject,
              dataNativeAddress,
              dictionaryIds.getIntData, Platform.INT_ARRAY_OFFSET, num * 4)
          } else {
            // the batch is not compressed
            val nullsNativeAddress = fiberCache.fiberData.baseOffset +
              ParquetDataFiberCompressedHeader.defaultSize
            Platform.copyMemory(baseObject,
              nullsNativeAddress + start, column.getNulls, Platform.BYTE_ARRAY_OFFSET, num)
            val fiberBatchedInfo = fiberCache.fiberBatchedInfo(start / defaultCapacity)
            val startAddress = fiberBatchedInfo.startAddress
            val endAddress = fiberBatchedInfo.endAddress
            val length = endAddress - startAddress
            if (length == num * 4) {
              Platform.copyMemory(baseObject,
                startAddress,
                dictionaryIds.getIntData, Platform.INT_ARRAY_OFFSET, num * 4)
            }
          }
        case ParquetDataFiberCompressedHeader(false, true, _) =>
          // can to this branch ?
          column.putNulls(0, num)
        case ParquetDataFiberCompressedHeader(true, true, _) =>
          throw new OapException("error header status (true, true, _)")
        case other => throw new OapException(s"impossible header status $other.")
      }
    } else {
      column.setDictionary(null)
      compressHeader match {
        case ParquetDataFiberCompressedHeader(true, false, _) =>
          if (baseObject != null) {
            // the batch is compressed
            val dataNativeAddress = decompressedFiberCache.fiberData.baseOffset
            readBatch(decompressedFiberCache, dataNativeAddress, num, column)
          } else {
            // the batch is not compressed
            val fiberBatchedInfo = fiberCache.fiberBatchedInfo(start / defaultCapacity)
            val startAddress = fiberBatchedInfo.startAddress
            val endAddress = fiberBatchedInfo.endAddress
            val length = endAddress - startAddress
            val expectLength = decompressedLength(column.dataType(),
              num, fiberBatchedInfo.length.toInt)
            if (length == expectLength) {
              readBatch(fiberCache, startAddress, num, column)
            }
          }
        case ParquetDataFiberCompressedHeader(false, false, _) =>
          if (baseObject != null) {
            // the batch is compressed
            val nullsNativeAddress = fiberCache.fiberData.baseOffset +
              ParquetDataFiberCompressedHeader.defaultSize
            Platform.copyMemory(fiberCache.fiberData.baseObject,
              nullsNativeAddress + start, column.getNulls, Platform.BYTE_ARRAY_OFFSET, num)
            val dataNativeAddress = decompressedFiberCache.fiberData.baseOffset
            readBatch(decompressedFiberCache, dataNativeAddress, num, column)
          } else {
            // the batch is not compressed
            val nullsNativeAddress = fiberCache.fiberData.baseOffset +
              ParquetDataFiberCompressedHeader.defaultSize
            Platform.copyMemory(baseObject,
              nullsNativeAddress + start, column.getNulls, Platform.BYTE_ARRAY_OFFSET, num)
            val fiberBatchedInfo = fiberCache.fiberBatchedInfo(start / defaultCapacity)
            val startAddress = fiberBatchedInfo.startAddress
            val endAddress = fiberBatchedInfo.endAddress
            val length = endAddress - startAddress
            val expectLength = decompressedLength(
              column.dataType(), num, fiberBatchedInfo.length.toInt)
            if (length == expectLength) {
              readBatch(fiberCache, startAddress, num, column)
            }
          }
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
  override def readRowGroupMetas(): Unit = {
    compressHeader = ParquetDataFiberCompressedHeader(address, fiberCache)
    compressHeader match {
      case ParquetDataFiberCompressedHeader(_, _, 0) =>
        dictionary = null
      case ParquetDataFiberCompressedHeader(false, true, _) =>
        dictionary = null
      case ParquetDataFiberCompressedHeader(true, false, dicLength) =>
        val fiberBatchedInfo = fiberCache.fiberBatchedInfo
        val compressedSize = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
        val lastIndex = math.ceil(total * 1.0 / compressedSize).toInt - 1
        val lastEndAddress = fiberBatchedInfo(lastIndex).endAddress
        val firstStartAddress = fiberBatchedInfo(0).startAddress
        val length = lastEndAddress - firstStartAddress
        val dicNativeAddress =
          address + ParquetDataFiberCompressedHeader.defaultSize + length
        dictionary =
          new ParquetDictionaryWrapper(readDictionary(dataType, dicLength, dicNativeAddress))
      case ParquetDataFiberCompressedHeader(false, false, dicLength) =>
        val fiberBatchedInfo = fiberCache.fiberBatchedInfo
        val compressedSize = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
        val lastIndex = math.ceil(total * 1.0 / compressedSize).toInt - 1
        val lastEndAddress = fiberBatchedInfo(lastIndex).endAddress
        val firstStartAddress = fiberBatchedInfo(0).startAddress
        val length = lastEndAddress - firstStartAddress
        val dicNativeAddress = address +
          ParquetDataFiberCompressedHeader.defaultSize + length + total
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
      num: Int, column: OapOnHeapColumnVector): Unit = {
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
        column.getChild(0).asInstanceOf[OapOnHeapColumnVector].setByteData(data)
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

  def decompressedLength(dataType: DataType, num: Int,
      childColumnVectorLength: Int): Int = {
    dataType match {
      case ByteType | BooleanType =>
        num
      case ShortType =>
        num * 2
      case IntegerType | DateType | FloatType =>
        num * 4
      case LongType | TimestampType | DoubleType =>
        num * 8
      case BinaryType | StringType =>
        num * 8 + childColumnVectorLength
      // if DecimalType.is32BitDecimalType(other) as int data type.
      case other if DecimalType.is32BitDecimalType(other) =>
        num * 4
      // if DecimalType.is64BitDecimalType(other) as long data type.
      case other if DecimalType.is64BitDecimalType(other) =>
        num * 8
      // if DecimalType.isByteArrayDecimalType(other) as binary data type.
      case other if DecimalType.isByteArrayDecimalType(other) =>
        num * 8 + childColumnVectorLength
      case other => throw new OapException(s"impossible data type $other.")
    }
  }

  def decompressFiberCache(
                            compressedFiberCache: FiberCache,
                            columnVector: OapOnHeapColumnVector,
                            start: Int, num: Int): FiberCache = {
    val defaultCapacity = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
    val fiberBatchedInfo = compressedFiberCache.fiberBatchedInfo(start / defaultCapacity)
    val codecName = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionCodec
    val decompressionCodec = SparkCompressionCodec.createCodec(new SparkConf(), codecName)

    if (fiberBatchedInfo.compressed && decompressionCodec != null) {
      val fiberCache = compressedFiberCache
      val startAddress = fiberBatchedInfo.startAddress
      val endAddress = fiberBatchedInfo.endAddress
      val length = endAddress - startAddress
      val compressedBytes = new Array[Byte](length.toInt)
      Platform.copyMemory(null,
        startAddress,
        compressedBytes, Platform.BYTE_ARRAY_OFFSET, length)
      val decompressedBytesLength: Int = if (dictionary != null) {
        num * 4
      } else {
        decompressedLength(columnVector.dataType(), num, fiberBatchedInfo.length.toInt)
      }
      val cis = decompressionCodec.compressedInputStream(new ByteArrayInputStream(compressedBytes))
      val decompressedBytes = new Array[Byte](decompressedBytesLength)
      new DataInputStream(cis).readFully(decompressedBytes)
      val memoryBlockHolder = new MemoryBlockHolder(
        decompressedBytes, Platform.BYTE_ARRAY_OFFSET,
        decompressedBytes.length, decompressedBytes.length, SourceEnum.DRAM)

      val fiberCacheReturned = if (num < defaultCapacity) {
        new DecompressBatchedFiberCache(fiberCache.fiberType, memoryBlockHolder,
          fiberBatchedInfo.compressed, fiberCache)
      } else {
        new DecompressBatchedFiberCache(fiberCache.fiberType, memoryBlockHolder,
          fiberBatchedInfo.compressed, null)
      }
      fiberCacheReturned.batchedCompressed = fiberBatchedInfo.compressed
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
    val reader = new ParquetDataFiberCompressedReader(
      address, dataType, total, fiberCache)
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

  def apply(vector: OapOnHeapColumnVector, total: Int): ParquetDataFiberCompressedHeader = {
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
