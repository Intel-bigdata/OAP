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

  // write function parameters need to be optimized
  def write(schema: StructType, fileOut: FSDataOutputStream,
            uniqueKeys: Array[InternalRow],
            offsetMap: java.util.HashMap[InternalRow, Long]): Unit

  // parameter needs to be optimized
  def read(schema: StructType, intervalArray: ArrayBuffer[RangeInterval],
           stsArray: Array[Byte], offset: Long): Double
}

class SampleBasedStatistics extends Statistics {
  override val id: Int = 1

  private var sampleRate = 0.1

  def setSampleRate(rate: Double): Unit = {
    sampleRate = rate
  }

  // for SampleBasedStatistics, input keys should be the whole file
  // instead of uniqueKeys, can be refactor later
  def write(schema: StructType, fileOut: FSDataOutputStream,
            uniqueKeys: Array[InternalRow],
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
    sample_array_index.foreach(idx => writeSingleRow(uniqueKeys(idx), fileOut))

    // can be reused, consider transfer this function to [[Statistics]] class
    def writeSingleRow(row: InternalRow, fileOut: FSDataOutputStream): Unit = {
      val unsafeRow = converter(row)
      val keyBuf = new ByteArrayOutputStream()
      IndexUtils.writeInt(keyBuf, unsafeRow.getSizeInBytes)
      unsafeRow.writeToStream(keyBuf, null)
      keyBuf.writeTo(fileOut)
      fileOut.flush()
      keyBuf.close()
    }
  }

  var arrayOffset: Long = _

  override def read(schema: StructType, intervalArray: ArrayBuffer[RangeInterval],
                    stsArray: Array[Byte], offset_temp: Long): Double = {
    var offset = Platform.BYTE_ARRAY_OFFSET + offset_temp
    val id_from_file = Platform.getInt(stsArray, offset)
    offset += 4
    assert(id_from_file == id, "Statistics type mismatch")
    val ordering = GenerateOrdering.create(schema)
    val size_from_file = Platform.getInt(stsArray, offset)
    offset += 4

    def readSingleUnsafeRow(array: Array[Byte], offset: Long): (UnsafeRow, Long) = {
      val size = Platform.getInt(array, offset)
      val row = UnsafeIndexNode.row.get
      row.setNumFields(schema.length)
      row.pointTo(array, offset + 4, size)
      (row.copy(), offset + 4 + size)
    }

    def rowInSingleInterval(row: UnsafeRow, interval: RangeInterval)
                           (implicit order: BaseOrdering = ordering): Boolean = {
      if (interval.start == RangeScanner.DUMMY_KEY_START) {
        if (interval.end == RangeScanner.DUMMY_KEY_END) true
        else {
          if (order.lt(row, interval.end)) true
          else if (order.equiv(row, interval.end) && interval.endInclude) true
          else false
        }
      } else {
        if (order.lt(row, interval.start)) false
        else if (order.equiv(row, interval.start) && !interval.startInclude) false
        else if (interval.end != RangeScanner.DUMMY_KEY_END && (order.gt(row, interval.end) ||
          (order.equiv(row, interval.end) && !interval.endInclude))) {false}
        else true
      }
    }
    def rowInIntervalArray(row: UnsafeRow, intervalArray: ArrayBuffer[RangeInterval])
                          (implicit order: BaseOrdering = ordering): Boolean = {
      if (intervalArray == null || intervalArray.isEmpty) false
      else intervalArray.exists(interval => rowInSingleInterval(row, interval))
    }

    val sample_array_unsaferow = new Array[UnsafeRow](size_from_file)
    (0 until size_from_file).foreach(i => {
      val (row, off) = readSingleUnsafeRow(stsArray, offset)
      sample_array_unsaferow(i) = row
      offset = off
    })
    arrayOffset = offset - Platform.BYTE_ARRAY_OFFSET
    sample_array_unsaferow.count(rowInIntervalArray(_, intervalArray)) / (1.0 * size_from_file)
  }
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

    if (result) -1 else 1
  }

  override def write(schema: StructType, fileOut: FSDataOutputStream,
                     uniqueKeys: Array[InternalRow],
                     offsetMap: java.util.HashMap[InternalRow, Long]): Unit = {
    keySchema = schema
    writeStatistics(fileOut, uniqueKeys, offsetMap)
  }

  private def getSimpleStatistics(stsArray: Array[Byte],
                          offset: Long): ArrayBuffer[(Int, UnsafeRow, Long)] = {
    val sts = ArrayBuffer[(Int, UnsafeRow, Long)]()
    val size = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET)
    var i = 0
    var base = offset + 4

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
    val value = getUnsafeRow(stsArray, base, size).copy()
    val offset = Platform.getLong(stsArray, Platform.BYTE_ARRAY_OFFSET + base + 4 + size)
    (size + 4, value, offset)
  }

  private def getUnsafeRow(array: Array[Byte], offset: Long, size: Int): UnsafeRow = {
    val row = UnsafeRangeNode.row.get
    row.setNumFields(keySchema.length)
    row.pointTo(array, Platform.BYTE_ARRAY_OFFSET + offset + 4, size)
    row
  }

  private def writeStatistics(fileOut: FSDataOutputStream,
                              uniqueKeys: Array[InternalRow],
                              offsetMap: java.util.HashMap[InternalRow, Long]): Unit = {
    // write Min Max sts
    writeMinMaxSts(fileOut, uniqueKeys, offsetMap)
  }

  private def writeMinMaxSts(fileOut: FSDataOutputStream,
                             uniqueKeys: Array[InternalRow],
                             offsetMap: java.util.HashMap[InternalRow, Long]): Unit = {
    // write stats size
    IndexUtils.writeInt(fileOut, 2)

    // write minval
    writeStatistic(uniqueKeys.head, offsetMap, fileOut)

    // write maxval
    writeStatistic(uniqueKeys.last, offsetMap, fileOut)
  }

  // write min and max value at the beginning of index file
  // the statistics is like
  // | value[Bytes] | offset[Long] |
  private def writeStatistic(row: InternalRow,
                             offsetMap: java.util.HashMap[InternalRow, Long],
                             fileOut: FSDataOutputStream) = {
    val keyBuf = new ByteArrayOutputStream()
    val value = convertHelper(row, keyBuf)
    value.writeToStream(keyBuf, null)

    keyBuf.writeTo(fileOut)
    IndexUtils.writeLong(fileOut, offsetMap.get(row))
    fileOut.flush()

    keyBuf.close()
  }

  /**
   * This method help spinach convert InternalRow type to UnsafeRow type
   * @param internalRow
   * @param keyBuf
   * @return unsafeRow
   */
  private def convertHelper(internalRow: InternalRow, keyBuf: ByteArrayOutputStream): UnsafeRow = {
    val writeRow = converter.apply(internalRow)
    IndexUtils.writeInt(keyBuf, writeRow.getSizeInBytes)
    writeRow
  }
}
