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

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.format.CompressionCodec
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.{DecompressBatchedFiberCache, FiberCache, MemoryBlockHolder}
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader
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
class ParquetDataFiberCompressedWriter() extends Logging {
  private val codecFactory: CodecFactory = new CodecFactory(new Configuration())

  /**
   * Write nulls data to data fiber.
   */
  private def dumpNullsToFiber(
      nativeAddress: Long, nulls: Array[Array[Byte]], total: Int): Long = {
    var count = 0
    var startAddress = nativeAddress
    while (count < nulls.length) {
      Platform.copyMemory(nulls(count),
        Platform.BYTE_ARRAY_OFFSET, null, startAddress, nulls(count).length)
      startAddress += nulls(count).length
      count += 1
    }
    nativeAddress + total
  }

  /**
   * noNulls is true, nulls are all 0, not dump nulls to cache,
   * allNulls is false, need dump to cache,
   * dicLength is 0, needn't calculate dictionary part.
   */
  def dumpDataToFiber(
      reader: VectorizedColumnReader,
      total: Int,
      dataType: DataType): FiberCache = {
    val compressedLength =
      OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize
    val compressedUnitSize = math.ceil(total * 1.0 / compressedLength).toInt
    val arrayBytes: Array[Array[Byte]] = new Array[Array[Byte]](compressedUnitSize)

    val codecName = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionCodec
    val codec = CompressionCodec.valueOf(codecName)
    val compressor = codecFactory.getCompressor(codec)

    val batchCompressed = new Array[Boolean](compressedUnitSize)
    var childColumnVectorLengths: Array[Int] = null
    var numNulls = 0
    var compressedSize = 0
    var count = 0
    var loadedRowCount = 0
    val nulls: Array[Array[Byte]] = new Array[Array[Byte]](compressedUnitSize)

    // used for collect the compressed info
    var totalCompressedTime: Long = 0
    var totalCompressedSize: Int = 0
    var totalUncompressedSize: Int = 0

    def splitColumnVectorNoBinaryType(byteLength: Int, offset: Int): Unit = {
      while (count < compressedUnitSize) {
        val num = Math.min(compressedLength, total - loadedRowCount)
        val column = new OnHeapColumnVector(num, dataType)
        reader.readBatch(num, column)
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
        val rawBytes: Array[Byte] = new Array[Byte](num * byteLength)
        Platform.copyMemory(bytes,
          offset,
          rawBytes, Platform.BYTE_ARRAY_OFFSET, num * byteLength)
        // store the nulls info
        numNulls += column.numNulls()
        nulls(count) = column.getNulls
        val startTime = System.currentTimeMillis()

        val compressedBytes = compressor.compress(rawBytes)

        totalCompressedTime += System.currentTimeMillis() - startTime
        totalCompressedSize += compressedBytes.length
        totalUncompressedSize += rawBytes.length

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
      while (count < compressedUnitSize) {
        val num = Math.min(compressedLength, total - loadedRowCount)
        val column = new OnHeapColumnVector(num, dataType)
        reader.readBatch(num, column)
        val arrayLengths: Array[Int] = column.getArrayLengths
        val arrayOffsets: Array[Int] = column.getArrayOffsets
        val childBytes: Array[Byte] = column.getChild(0)
          .asInstanceOf[OnHeapColumnVector].getByteData

        var lastIndex = num - 1
        while (lastIndex >= 0 && column.isNullAt(lastIndex)) {
          lastIndex -= 1
        }
        var firstIndex = 0
        while (firstIndex < num && column.isNullAt(firstIndex)) {
          firstIndex += 1
        }
        if (firstIndex < num && lastIndex >= 0) {
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
            rawBytes, Platform.BYTE_ARRAY_OFFSET + num * 8, childColumnVectorLengths(count))
          // store the nulls info
          numNulls += column.numNulls()
          nulls(count) = column.getNulls
          val startTime = System.currentTimeMillis()

          val compressedBytes = compressor.compress(rawBytes)
          totalCompressedTime += System.currentTimeMillis() - startTime
          totalCompressedSize += compressedBytes.length
          totalUncompressedSize += rawBytes.length
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

    logInfo(s"dumpDataToFiber with compressed: the totalCompressedTime is #$totalCompressedTime#;" +
      s" the totalCompressedSize is #$totalCompressedSize#;" +
      s" the totalUncompressedSize is #$totalUncompressedSize#" +
      s" and the total is #${total}# and the data type is #${dataType}#")

    val header = ParquetDataFiberCompressedHeader(numNulls == 0, numNulls == total, 0)
    var fiber: FiberCache = null
    val fiberBatchedInfo = new mutable.HashMap[Int, (Long, Long, Boolean, Long)]()
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
        dumpNullsToFiber(header.writeToCache(fiber.getBaseOffset), nulls, total)
      }

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
      fiber.containBatchCompressed = true
      // record the compressed parameter to fiber
      fiber.fiberBatchedInfo = fiberBatchedInfo
    } else {
      // if the column vector is nulls, we only dump the header info to data fiber
      val fiberLength = ParquetDataFiberCompressedHeader.defaultSize
      logDebug(s"will apply $fiberLength bytes off heap memory for data fiber.")
      fiber = emptyDataFiber(fiberLength)
    }
    fiber
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

  // var dictionary: org.apache.spark.sql.execution.vectorized.Dictionary = _
  private val codecFactory: CodecFactory = new CodecFactory(new Configuration())

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
    val defaultCapacity = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize

    val baseObject = decompressedFiberCache.fiberData.baseObject

    column.setDictionary(null)
    header match {
      case ParquetDataFiberCompressedHeader(true, false, _) =>
        if (baseObject != null) {
          // the batch is compressed
          val dataNativeAddress = decompressedFiberCache.fiberData.baseOffset
          readBatch(decompressedFiberCache, dataNativeAddress, num, column)
        } else {
          // the batch is not compressed
          val fiberBatchedInfo = fiberCache.fiberBatchedInfo(start / defaultCapacity)
          val startAddress = fiberBatchedInfo._1
          readBatch(fiberCache, startAddress, num, column)
        }
      case ParquetDataFiberCompressedHeader(false, false, _) =>
        if (baseObject != null) {
          // the batch is compressed
          val nullsNativeAddress = decompressedFiberCache.fiberData.baseOffset
          Platform.copyMemory(baseObject,
            nullsNativeAddress, column.getNulls,
            Platform.BYTE_ARRAY_OFFSET, num)
          val dataNativeAddress = nullsNativeAddress + 1 * num
          readBatch(decompressedFiberCache, dataNativeAddress, num, column)
        } else {
          // the batch is not compressed
          val nullsNativeAddress = fiberCache.fiberData.baseOffset +
            ParquetDataFiberCompressedHeader.defaultSize
          Platform.copyMemory(baseObject,
            nullsNativeAddress + start, column.getNulls, Platform.BYTE_ARRAY_OFFSET, num)
          val fiberBatchedInfo = fiberCache.fiberBatchedInfo(start / defaultCapacity)
          val startAddress = fiberBatchedInfo._1
          readBatch(fiberCache, startAddress, num, column)
        }

      case ParquetDataFiberCompressedHeader(false, true, _) =>
        column.putNulls(0, num)
      case ParquetDataFiberCompressedHeader(true, true, _) =>
        throw new OapException("error header status (true, true, _)")
      case other => throw new OapException(s"impossible header status $other.")
    }
  }

  /**
   * Read ParquetDataFiberCompressedHeader and dictionary from data fiber.
   */
  private def readRowGroupMetas(): Unit = {
    header = ParquetDataFiberCompressedHeader(address, fiberCache)
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

    val codecName = OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionCodec
    val codec = CompressionCodec.valueOf(codecName)
    val decompressor = codecFactory.getDecompressor(codec)

   // val decompress = OapRuntime.getOrCreate.fiberCacheManager.decompressor
    if (fiberBatchedInfo._3 && decompressor != null) {
      val fiberCache = compressedFiberCache
      val startAddress = fiberBatchedInfo._1
      val endAddress = fiberBatchedInfo._2
      val length = endAddress - startAddress
      val compressedBytes = new Array[Byte](length.toInt)
      Platform.copyMemory(null,
        startAddress,
        compressedBytes, Platform.BYTE_ARRAY_OFFSET, length)
      val decompressedBytesLength: Int = columnVector.dataType() match {
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

      var decompressedBytes = decompressor.decompress(compressedBytes,
        decompressedBytesLength)
      var nulls: Array[Byte] = null
      if (!header.noNulls) {
        nulls = new Array[Byte](num)
        Platform.copyMemory(null, fiberCache.fiberData.baseOffset +
          ParquetDataFiberCompressedHeader.defaultSize + start, nulls,
          Platform.BYTE_ARRAY_OFFSET, num)
        decompressedBytes = nulls ++ decompressedBytes
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
