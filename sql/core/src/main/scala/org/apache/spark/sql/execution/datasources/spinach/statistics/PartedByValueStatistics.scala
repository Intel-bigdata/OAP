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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.index._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

class PartedByValueStatistics extends Statistics {
  override val id: Int = PartByValueStatisticsType.id
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

    var i = 0
    while (i <= partNum &&
      ordering.gteq(end.end, stats(i)._2) &&
      !Statistics.rowInIntervalArray(stats(i)._2, intervalArray, ordering)) {
      i += 1
    }
    val left = i
    while (i <= partNum &&
      Statistics.rowInIntervalArray(stats(i)._2, intervalArray, ordering)) {
      i += 1
    }
    val right = i
    if (left == partNum + 1 || right == 0) {
      // interval.min > partition.max || interval.max < partition.min
      StaticsAnalysisResult.SKIP_INDEX
    } else {
      var cover: Double = if (right <= partNum) stats(right)._4 else stats.last._4

      if (start.start != IndexScanner.DUMMY_KEY_START && left > 0 &&
        ordering.lteq(start.start, stats(left)._2)) {
        cover -= stats(left - 1)._4
        cover += 0.5 * (stats(left)._4 - stats(left - 1)._4)
      }

      if (end.end != IndexScanner.DUMMY_KEY_END && right <= partNum &&
        ordering.gteq(end.end, stats(right - 1)._2)) {
        cover -= 0.5 * (stats(right)._4 - stats(right - 1)._4)
      }

      cover / wholeCount
    }
  }

  //  private def isBetween(obj: InternalRow, min: InternalRow, max: InternalRow,
  //                        ordering: BaseOrdering): Boolean = {
  //    (min == IndexScanner.DUMMY_KEY_START || ordering.gteq(obj, min)) &&
  //      (max == IndexScanner.DUMMY_KEY_END || ordering.lteq(obj, max))
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

  override def write(schema: StructType, writer: IndexOutputWriter,
                     uniqueKeys: Array[InternalRow],
                     hashMap: java.util.HashMap[InternalRow, java.util.ArrayList[Long]],
                     offsetMap: java.util.HashMap[InternalRow, Long]): Unit = {
    keySchema = schema

    // first write statistic id
    IndexUtils.writeInt(writer, id)

    val size = hashMap.size()
    if (size > 0) {
      val partNum = if (size > maxPartNum) maxPartNum else size
      val perSize = size / partNum

      // first write part number
      IndexUtils.writeInt(writer, partNum + 1)

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
        writeEntry(writer, uniqueKeys(index), index, count)
        i += 1
      }

      index += 1
      while (index < uniqueKeys.size) {
        count += hashMap.get(uniqueKeys(index)).size()
        index += 1
      }
      writeEntry(writer, uniqueKeys.last, size - 1, count)
    }
  }

  private def writeEntry(writer: IndexOutputWriter,
                         internalRow: InternalRow,
                         index: Int, count: Int): Unit = {
    Statistics.writeInternalRow(converter, internalRow, writer)
    IndexUtils.writeInt(writer, index)
    IndexUtils.writeInt(writer, count)
  }
}
