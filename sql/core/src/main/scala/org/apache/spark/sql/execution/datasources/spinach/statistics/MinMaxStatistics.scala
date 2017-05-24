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

package org.apache.spark.sql.execution.datasources.spinach.statistics

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.Key
import org.apache.spark.sql.execution.datasources.spinach.index._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

class MinMaxStatistics extends Statistics {
  override val id: Int = MinMaxStatisticsType.id
  @transient private lazy val converter = UnsafeProjection.create(schema)
  @transient private lazy val ordering = GenerateOrdering.create(schema)
  var arrayOffset = 0L

  protected var min: Key = _
  protected var max: Key = _

  override def initialize(schema: StructType): Unit = {
    super.initialize(schema)
    min = null
    max = null
  }

  override def addSpinachKey(key: Key): Unit = {
    if (min == null || max == null) {
      min = key
      max = key
    } else {
      if (ordering.compare(key, min) < 0) min = key
      if (ordering.compare(key, max) > 0) max = key
    }
  }

  override def write(writer: IndexOutputWriter, sortedKeys: ArrayBuffer[Key]): Long = {
    var offset = super.write(writer, sortedKeys)
    offset += Statistics.writeInternalRow(converter, min, writer)
    offset += Statistics.writeInternalRow(converter, max, writer)
    offset
  }

  override def read(bytes: Array[Byte], baseOffset: Long): Long = {
    var offset = super.read(bytes, baseOffset) + baseOffset // offset after super.read

    val minSize = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    min = Statistics.getUnsafeRow(schema.length, bytes, offset, minSize).copy()
    offset += (4 + minSize)

    val maxSize = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    max = Statistics.getUnsafeRow(schema.length, bytes, offset, maxSize).copy()
    offset += (4 + maxSize)

    offset - baseOffset
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double = {
    val start = intervalArray.head
    val end = intervalArray.last
    var result = false

    if (start.start != IndexScanner.DUMMY_KEY_START) {
      // > or >= start
      if (start.startInclude) {
        result |= ordering.gt(start.start, max)
      } else {
        result |= ordering.gteq(start.start, max)
      }
    }
    if (end.end != IndexScanner.DUMMY_KEY_END) {
      // < or <= end
      if (end.endInclude) {
        result |= ordering.lt(end.end, min)
      } else {
        result |= ordering.lteq(end.end, min)
      }
    }

    if (result) StaticsAnalysisResult.SKIP_INDEX else StaticsAnalysisResult.USE_INDEX
  }


  override def read(schema: StructType, intervalArray: ArrayBuffer[RangeInterval],
                    stsArray: Array[Byte], offset: Long): Double = {
    val stats = getSimpleStatistics(stsArray, offset)

    min = stats.head._2 // UnsafeRow
    max = stats.last._2 // UnsafeRow

    val start = intervalArray.head
    val end = intervalArray.last
    var result = false

    val ordering = GenerateOrdering.create(schema)

    if (start.start != IndexScanner.DUMMY_KEY_START) {
      // > or >= start
      if (start.startInclude) {
        result |= ordering.gt(start.start, max)
      } else {
        result |= ordering.gteq(start.start, max)
      }
    }
    if (end.end != IndexScanner.DUMMY_KEY_END) {
      // < or <= end
      if (end.endInclude) {
        result |= ordering.lt(end.end, min)
      } else {
        result |= ordering.lteq(end.end, min)
      }
    }

    if (result) StaticsAnalysisResult.SKIP_INDEX else StaticsAnalysisResult.USE_INDEX
  }

  override def write(schema: StructType, writer: IndexOutputWriter,
                     uniqueKeys: Array[Key],
                     hashMap: java.util.HashMap[Key, java.util.ArrayList[Long]],
                     offsetMap: java.util.HashMap[Key, Long]): Unit = {

    // write statistic id
    IndexUtils.writeInt(writer, id)

    // write stats size
    IndexUtils.writeInt(writer, 2)

    min = uniqueKeys.head
    max = uniqueKeys.last

    // write minval
    writeStatistic(min, offsetMap, writer)

    // write maxval
    writeStatistic(max, offsetMap, writer)
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
    val value = Statistics.getUnsafeRow(schema.length, stsArray, base, size).copy()
    val offset = Platform.getLong(stsArray, Platform.BYTE_ARRAY_OFFSET + base + 4 + size)
    (size + 4, value, offset)
  }

  // write min and max value at the beginning of index file
  // the statistics is like
  // | value[Bytes] | offset[Long] |
  private def writeStatistic(row: Key,
                             offsetMap: java.util.HashMap[Key, Long],
                             writer: IndexOutputWriter) = {
    Statistics.writeInternalRow(converter, row, writer)
    IndexUtils.writeLong(writer, offsetMap.get(row))
  }
}
