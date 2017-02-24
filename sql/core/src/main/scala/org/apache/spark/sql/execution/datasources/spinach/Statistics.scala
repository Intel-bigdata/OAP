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

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.hadoop.fs.FSDataOutputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{BaseOrdering, GenerateOrdering}
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

abstract class Statistics{
  val id: Int
  var arrayOffset: Long

  // write function parameters need to be optimized
  def write(schema: StructType, fileOut: FSDataOutputStream, uniqueKeys: Array[InternalRow],
            hashMap: java.util.HashMap[InternalRow, java.util.ArrayList[Long]],
            offsetMap: java.util.HashMap[InternalRow, Long]): Unit

  // parameter needs to be optimized
  def read(schema: StructType, intervalArray: ArrayBuffer[RangeInterval],
           stsArray: Array[Byte], offset: Long): Double
}

class MinMaxStatistics extends Statistics {
  override val id: Int = 0
  private var keySchema: StructType = _
  @transient private lazy val converter = UnsafeProjection.create(keySchema)
  var arrayOffset = 0L

  override def read(schema: StructType, intervalArray: ArrayBuffer[RangeInterval],
                    stsArray: Array[Byte], offset: Long): Double = {
    keySchema = schema

    val stats = getSimpleStatistics(stsArray, offset)

    val min = stats.head._2
    val max = stats.last._2

    val start = intervalArray.head
    val end = intervalArray.last
    var result = false

    val ordering = GenerateOrdering.create(keySchema)

    if (start.start != RangeScanner.DUMMY_KEY_START) { // > or >= start
      if (start.startInclude) {
        result |= ordering.gt(start.start, max)
      } else {
        result |= ordering.gteq(start.start, max)
      }
    }
    if (end.end != RangeScanner.DUMMY_KEY_END) { // < or <= end
      if (end.endInclude) {
        result |= ordering.lt(end.end, min)
      } else {
        result |= ordering.lteq(end.end, min)
      }
    }

    if (result) -1 else 0
  }

  override def write(schema: StructType, fileOut: FSDataOutputStream,
                     uniqueKeys: Array[InternalRow],
                     hashMap: java.util.HashMap[InternalRow, java.util.ArrayList[Long]],
                     offsetMap: java.util.HashMap[InternalRow, Long]): Unit = {
    keySchema = schema

    // write statistic id
    IndexUtils.writeInt(fileOut, id)

    // write stats size
    IndexUtils.writeInt(fileOut, 2)

    // write minval
    writeStatistic(uniqueKeys.head, offsetMap, fileOut)

    // write maxval
    writeStatistic(uniqueKeys.last, offsetMap, fileOut)
  }

  private def getSimpleStatistics(stsArray: Array[Byte],
                                  offset: Long): ArrayBuffer[(Int, UnsafeRow, Long)] = {
    val sts = ArrayBuffer[(Int, UnsafeRow, Long)]()
    val size = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + offset + 4)
    var i = 0
    var base = offset + 8

    while (i < size) {
      val now = extractSts(base, stsArray)
      sts += now
      i += 1
      base += now._1 + 8
    }

    arrayOffset = base

    sts
  }

  private def extractSts(base: Long, stsArray: Array[Byte]): (Int, UnsafeRow, Long) = {
    val size = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + base)
    val value = Statistics.getUnsafeRow(keySchema.length, stsArray, base, size).copy()
    val offset = Platform.getLong(stsArray, Platform.BYTE_ARRAY_OFFSET + base + 4 + size)
    (size + 4, value, offset)
  }

  // write min and max value at the beginning of index file
  // the statistics is like
  // | value[Bytes] | offset[Long] |
  private def writeStatistic(row: InternalRow,
                             offsetMap: java.util.HashMap[InternalRow, Long],
                             fileOut: FSDataOutputStream) = {
    Statistics.writeInternalRow(converter, row, fileOut)
    IndexUtils.writeLong(fileOut, offsetMap.get(row))
    fileOut.flush()
  }

}

class SampleBasedStatistics extends Statistics {
  override val id: Int = 1

  private var sampleRate = 0.1

  def setSampleRate(rate: Double): Unit = {
    sampleRate = rate
  }

  // for SampleBasedStatistics, input keys should be the whole file
  // instead of uniqueKeys, can be refactor later
  def write(schema: StructType, fileOut: FSDataOutputStream, uniqueKeys: Array[InternalRow],
            hashMap: java.util.HashMap[InternalRow, java.util.ArrayList[Long]],
            offsetMap: java.util.HashMap[InternalRow, Long]): Unit = {
    // SampleBasedStatistics file structure
    // statistics_id        4 Bytes, Int, specify the [[Statistic]] type
    // sample_size          4 Bytes, Int, number of UnsafeRow
    //
    // | unsafeRow-1 sizeInBytes | unsafeRow-1 content |
    // | unsafeRow-2 sizeInBytes | unsafeRow-2 content |
    // | unsafeRow-3 sizeInBytes | unsafeRow-3 content |
    // ...
    // | unsafeRow-(sample_size) sizeInBytes | unsafeRow-(sample_size) content |

    val converter = UnsafeProjection.create(schema)
    val sample_size = (uniqueKeys.length * sampleRate).toInt

    IndexUtils.writeInt(fileOut, id)
    IndexUtils.writeInt(fileOut, sample_size)

    val sample_array_index = Random.shuffle(uniqueKeys.indices.toList).take(sample_size)
    sample_array_index.foreach(idx =>
      Statistics.writeInternalRow(converter, uniqueKeys(idx), fileOut))
  }

  override var arrayOffset: Long = _

  override def read(schema: StructType, intervalArray: ArrayBuffer[RangeInterval],
                    stsArray: Array[Byte], offset_temp: Long): Double = {
    var offset = offset_temp
    val id_from_file = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + offset)
    offset += 4
    assert(id_from_file == id, "Statistics type mismatch")
    val ordering = GenerateOrdering.create(schema)
    val size_from_file = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + offset)
    offset += 4

    def readSingleUnsafeRow(array: Array[Byte], offset: Long): (UnsafeRow, Long) = {
      val size = Platform.getInt(array, Platform.BYTE_ARRAY_OFFSET + offset)
      val row = Statistics.getUnsafeRow(schema.length, array, offset, size).copy()
      (row, offset + 4 + size)
    }


    val sample_array_unsaferow = new Array[UnsafeRow](size_from_file)
    (0 until size_from_file).foreach(i => {
      val (row, off) = readSingleUnsafeRow(stsArray, offset)
      sample_array_unsaferow(i) = row
      offset = off
    })
    arrayOffset = offset

    sample_array_unsaferow.count(
      Statistics.rowInIntervalArray(_, intervalArray, ordering)) / (1.0 * size_from_file)
  }
}

class PartedByValueStatistics extends Statistics {
  override val id: Int = 2
  private val maxPartNum: Int = 5
  private var keySchema: StructType = _
  @transient private lazy val converter = UnsafeProjection.create(keySchema)
  var arrayOffset = 0L

  override def read(schema: StructType, intervalArray: ArrayBuffer[RangeInterval],
                    stsArray: Array[Byte], offset: Long): Double = {
    keySchema = schema

    val stats = getStatistics(stsArray, offset)

    // ._2 value
    // ._3 index
    // ._4 count
    val wholeCount = stats.last._4
    val partNum = stats.length - 1

    val start = intervalArray.head
    val end = intervalArray.last

    val ordering = GenerateOrdering.create(keySchema)

    /*
      if (start.start != RangeScanner.DUMMY_KEY_START) { // > or >= start
        if (ordering.gt(start.start, stats.last._2)) {
          cover = 0
          left = partNum + 1
        } else {
          while (left <= partNum && ordering.gt(start.start, stats(left)._2)) {
            left += 1
          }
          val resi = if (left > 0) {
            stats(left)._4 - stats(left - 1)._4
          } else 0

          if (ordering.gt(stats(left)._2, start.start)) {
            cover -= stats(left)._4
            cover += 0.5 * resi
          }
        }
      }
      if (end.end != RangeScanner.DUMMY_KEY_END) { // < or <= end
        if (ordering.gt(stats.head._2, end.end)) {
          cover = 0
        } else {
          while (right >= left && ordering.lt(end.end, stats(right)._2)) {
            right -= 1
          }

          val resi = if (right < partNum) {
            stats(right + 1)._4 - stats(right)._4
          } else 0

          if (ordering.lt(stats(right)._2, end.end)) {
            cover += 0.5 * stats(right + 1)._4
          }
        }
      }
     */

    var i = 0
    while (i <= partNum &&
      !Statistics.rowInIntervalArray(stats(i)._2, intervalArray, ordering)) {
      i += 1
    }
    val left = i
    if (left == partNum + 1) {
      -1
    } else {
      while (i <= partNum &&
        Statistics.rowInIntervalArray(stats(i)._2, intervalArray, ordering)) {
        i += 1
      }
      val right = i

      var cover: Double = if (right <= partNum) stats(right)._4 else stats.last._4

      if (start.start != RangeScanner.DUMMY_KEY_START && left > 0 &&
        ordering.lteq(start.start, stats(left)._2)) {
        cover -= stats(left - 1)._4
        cover += 0.5 * (stats(left)._4 - stats(left - 1)._4)
      }

      if (end.end != RangeScanner.DUMMY_KEY_END && right <= partNum &&
        ordering.gteq(end.end, stats(right - 1)._2)) {
        cover -= 0.5 * (stats(right)._4 - stats(right - 1)._4)
      }

      cover / wholeCount
    }
  }

//  private def isBetween(obj: InternalRow, min: InternalRow, max: InternalRow,
//                        ordering: BaseOrdering): Boolean = {
//    (min == RangeScanner.DUMMY_KEY_START || ordering.gteq(obj, min)) &&
//      (max == RangeScanner.DUMMY_KEY_END || ordering.lteq(obj, max))
//  }

  private def getStatistics(stsArray: Array[Byte],
                             offset: Long): ArrayBuffer[(Int, UnsafeRow, Int, Int)] = {
    val sts = ArrayBuffer[(Int, UnsafeRow, Int, Int)]()
    val partNum = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + offset + 4)
    var i = 0
    var base = offset + 8

    while (i < partNum) {
      val now = extractSts(base, stsArray)
      sts += now
      i += 1
      base += now._1
    }

    arrayOffset = base

    sts
  }

  private def extractSts(base: Long, stsArray: Array[Byte]): (Int, UnsafeRow, Int, Int) = {
    val size = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + base)
    val value = Statistics.getUnsafeRow(keySchema.length, stsArray, base, size).copy()
    val index = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + base + 4 + size)
    val count = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + base + 8 + size)
    (size + 12, value, index, count)
  }

  override def write(schema: StructType, fileOut: FSDataOutputStream,
                     uniqueKeys: Array[InternalRow],
                     hashMap: java.util.HashMap[InternalRow, java.util.ArrayList[Long]],
                     offsetMap: java.util.HashMap[InternalRow, Long]): Unit = {
    keySchema = schema

    // first write statistic id
    IndexUtils.writeInt(fileOut, id)

    val size = hashMap.size()
    if (size > 0) {
      val partNum = if (size > maxPartNum) maxPartNum else size
      val perSize = size / partNum

      // first write part number
      IndexUtils.writeInt(fileOut, partNum + 1)

      var i = 0
      var count = 0
      var index = 0
      while (i < partNum) {
        index = i * perSize
        var begin = Math.max(index - perSize + 1, 0)
        while (begin <= index) {
          count += hashMap.get(uniqueKeys(begin)).size()
          begin += 1
        }
        writeEntry(fileOut, uniqueKeys(index), index, count)
        i += 1
      }

      index += 1
      while (index < uniqueKeys.size) {
        count += hashMap.get(uniqueKeys(index)).size()
        index += 1
      }
      writeEntry(fileOut, uniqueKeys.last, size - 1, count)
    }
  }

  private def writeEntry(fileOut: FSDataOutputStream,
                         internalRow: InternalRow,
                         index: Int, count: Int): Unit = {
    Statistics.writeInternalRow(converter, internalRow, fileOut)
    IndexUtils.writeInt(fileOut, index)
    IndexUtils.writeInt(fileOut, count)
    fileOut.flush()
  }
}

object Statistics {
  def getUnsafeRow(schemaLen: Int, array: Array[Byte], offset: Long, size: Int): UnsafeRow = {
    val row = UnsafeRangeNode.row.get
    row.setNumFields(schemaLen)
    row.pointTo(array, Platform.BYTE_ARRAY_OFFSET + offset + 4, size)
    row
  }

  /**
    * This method help spinach convert InternalRow type to UnsafeRow type
    * @param internalRow
    * @param keyBuf
    * @return unsafeRow
    */
  def convertHelper(converter: UnsafeProjection,
                    internalRow: InternalRow,
                    keyBuf: ByteArrayOutputStream): UnsafeRow = {
    val writeRow = converter.apply(internalRow)
    IndexUtils.writeInt(keyBuf, writeRow.getSizeInBytes)
    writeRow
  }

  def writeInternalRow(converter: UnsafeProjection,
                       internalRow: InternalRow,
                       fileOut: FSDataOutputStream): Unit = {
    val keyBuf = new ByteArrayOutputStream()
    val value = convertHelper(converter, internalRow, keyBuf)
    value.writeToStream(keyBuf, null)

    keyBuf.writeTo(fileOut)
    fileOut.flush()
    keyBuf.close()
  }

  // logic is complex, needs to be refactored :(
  def rowInSingleInterval(row: UnsafeRow, interval: RangeInterval,
                          order: BaseOrdering): Boolean = {
    if (interval.start == RangeScanner.DUMMY_KEY_START) {
      if (interval.end == RangeScanner.DUMMY_KEY_END) true
      else {
        if (order.lt(row, interval.end)) true
        else if (order.equiv(row, interval.end) && interval.endInclude) true
        else false
      }
    } else {
      if (order.lt(row, interval.start)) false
      else if (order.equiv(row, interval.start)) {
        if (interval.startInclude) {
          if (interval.end != RangeScanner.DUMMY_KEY_END &&
            order.equiv(interval.start, interval.end) && !interval.endInclude) {false}
          else true
        } else interval.end == RangeScanner.DUMMY_KEY_END || order.gt(interval.end, row)
      }
      else if (interval.end != RangeScanner.DUMMY_KEY_END && (order.gt(row, interval.end) ||
        (order.equiv(row, interval.end) && !interval.endInclude))) {false}
      else true
    }
  }
  def rowInIntervalArray(row: UnsafeRow, intervalArray: ArrayBuffer[RangeInterval],
                        order: BaseOrdering): Boolean = {
    if (intervalArray == null || intervalArray.isEmpty) false
    else intervalArray.exists(interval => rowInSingleInterval(row, interval, order))
  }
}