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

import org.apache.hadoop.fs.FSDataOutputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.index.{IndexScanner, RangeInterval}
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

class PartedByValueStatistics(var content: Seq[StatisticsEntry] = null,
                              partNum: Int = 1, total_size: Int = 0) extends Statistics {
  override val id: Int = 2
  private val maxPartNum: Int = 5
  private var keySchema: StructType = _
  @transient private lazy val converter = UnsafeProjection.create(keySchema)
  var arrayOffset = 0L

  override def write(fileOut: FSDataOutputStream): Unit = {
    IndexUtils.writeInt(fileOut, id)
    IndexUtils.writeInt(fileOut, total_size) // not content length
    IndexUtils.writeInt(fileOut, partNum + 1)
    fileOut.flush()

    writeContent(fileOut)
    fileOut.flush()
  }

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
    if (left == partNum + 1) {
      -1
    } else {
      while (i <= partNum &&
        Statistics.rowInIntervalArray(stats(i)._2, intervalArray, ordering)) {
        i += 1
      }
      val right = i

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

  private def getStatistics(stsArray: Array[Byte],
                            offset: Long): ArrayBuffer[(Int, UnsafeRow, Int, Int)] = {
    val sts = ArrayBuffer[(Int, UnsafeRow, Int, Int)]()
    val partNum = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + offset + 8)
    var i = 0
    var base = offset + 12

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
    val value = Statistics.getUnsafeRow(keySchema.length, stsArray, base + 4, size).copy()
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
