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

package org.apache.spark.sql.execution.datasources.oap.statistics

import java.io.{ByteArrayOutputStream, OutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.types.StructType

// PartedByValueStatistics gives statistics with the value interval.
// for example, in an array where all internal rows appear only once
// 1, 2, 3, ..., 300
// `partNum` = 5, then the file content should be
//    RowContent      curMaxIdx   curAccumulatorCount
// (  1,  "test#1")       0              1
// ( 61,  "test#61")     60             61
// (121,  "test#121")   120            121
// (181,  "test#181")   180            181
// (241,  "test#241")   240            241
// (300,  "test#300")   299            300

private[oap] class PartByValueStatistics(schema: StructType) extends Statistics(schema) {
  override val id: Int = StatisticsType.TYPE_PART_BY_VALUE

  private lazy val maxPartNum: Int = StatisticsManager.partNumber
  @transient private lazy val ordering = GenerateOrdering.create(schema)
  @transient private lazy val partialOrdering =
    GenerateOrdering.create(StructType(schema.dropRight(1)))

  protected case class PartedByValueMeta(
      idx: Int, row: InternalRow, curMaxId: Int, accumulatorCnt: Int)
  protected lazy val metas: ArrayBuffer[PartedByValueMeta] = new ArrayBuffer[PartedByValueMeta]()

  override def write(writer: OutputStream, sortedKeys: ArrayBuffer[Key]): Int = {
    var offset = super.write(writer, sortedKeys)
    val hashMap = new java.util.HashMap[Key, Int]()
    val uniqueKeys: ArrayBuffer[Key] = new ArrayBuffer[Key]()

    var prev: Key = null
    var prevCnt: Int = 0

    for (key <- sortedKeys) {
      if (prev == null) {
        prev = key
        prevCnt += 1
      } else {
        if (ordering.compare(prev, key) == 0) prevCnt += 1
        else {
          hashMap.put(prev, prevCnt)
          uniqueKeys.append(prev)
          prevCnt = 1
          prev = key
        }
      }
    }
    if (prev != null) {
      hashMap.put(prev, prevCnt)
      uniqueKeys.append(prev)
    }

    buildPartMeta(uniqueKeys, hashMap)

    // start writing
    IndexUtils.writeInt(writer, metas.length)
    val tempWriter = new ByteArrayOutputStream()
    metas.foreach(meta => {
      nnkw.writeKey(tempWriter, meta.row)
      IndexUtils.writeInt(writer, meta.curMaxId)
      IndexUtils.writeInt(writer, meta.accumulatorCnt)
      IndexUtils.writeInt(writer, tempWriter.size())
      offset += 12
    })
    offset += tempWriter.size
    writer.write(tempWriter.toByteArray)
    offset
  }

  override def read(fiberCache: FiberCache, offset: Int): Int = {
    var readOffset = super.read(fiberCache, offset) + offset

    val size = fiberCache.getInt(readOffset)
    readOffset += 4

    var rowOffset = 0
    for (i <- 0 until size) {
      val row = nnkr.readKey(fiberCache, readOffset + size * 12 + rowOffset)._1
      val index = fiberCache.getInt(readOffset + i * 12)
      val count = fiberCache.getInt(readOffset + i * 12 + 4)
      rowOffset = fiberCache.getInt(readOffset + i * 12 + 8)
      metas.append(PartedByValueMeta(i, row, index, count))
    }
    readOffset += (size * 12 + rowOffset)
    readOffset - offset
  }

  //  meta id:             0       1       2       3       4       5
  //                       |_______|_______|_______|_______|_______|
  // interval id:        0     1       2       3       4       5      6
  // value array:(-inf, r0) [r0,r1) [r1,r2) [r2,r3) [r3,r4) [r4,r5]  (r5, +inf)
  private def getIntervalIdx(row: Key, include: Boolean, isStart: Boolean): Int = {
    // Only two cases are accepted, or something is wrong.
    // 1. meta.row = [1, "aaa"], row = [1, "bbb"] => row.numFields == schema.length
    // 2. meta.row = [1, "aaa"], row = [1, DUMMY_KEY_START] => row.numFields == schema.length - 1
    assert(row.numFields == schema.length || row.numFields == schema.length - 1,
      s"Can't compare row with current schema. row: $row, schema: $schema")

    metas.zipWithIndex.indexWhere {
      case (meta, index) =>
        if (row.numFields == schema.length) {
          if (include && index < metas.length - 1) ordering.compare(row, meta.row) < 0
          else ordering.compare(row, meta.row) <= 0
        } else {
          if (isStart) partialOrdering.compare(row, meta.row) <= 0
          else partialOrdering.compare(row, meta.row) < 0
        }
    }
  }

  protected def getIntervalIdxForStart(start: Key, include: Boolean): Int = {
    getIntervalIdx(start, include, isStart = true)
  }

  protected def getIntervalIdxForEnd(end: Key, include: Boolean): Int = {
    getIntervalIdx(end, include, isStart = false)
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double = {
    val wholeCount = metas.last.accumulatorCnt

    val start = intervalArray.head
    val end = intervalArray.last

    val left = getIntervalIdxForStart(start.start, start.startInclude)
    val right = getIntervalIdxForEnd(end.end, end.endInclude)

    if (left == -1 || right == 0) {
      // interval.min > partition.max || interval.max < partition.min
      StaticsAnalysisResult.SKIP_INDEX
    } else {
      var cover: Double =
        if (right != -1) metas(right).accumulatorCnt else metas.last.accumulatorCnt

      if (left > 0) {
        cover -= metas(left - 1).accumulatorCnt
        cover += 0.5 * (metas(left).accumulatorCnt - metas(left - 1).accumulatorCnt)
      }

      if (right != -1) {
        cover -= 0.5 * (metas(right).accumulatorCnt - metas(right - 1).accumulatorCnt)
      }

      if (cover > wholeCount) 1.0
      else if (cover < 0) 0.0
      else cover / wholeCount
    }
  }

  // TODO needs refactor, kept for easy debug
  private def buildPartMeta(uniqueKeys: ArrayBuffer[Key], hashMap: java.util.HashMap[Key, Int]) = {
    val size = hashMap.size()
    if (size > 0) {
      val partNum = if (size > maxPartNum) maxPartNum else size
      val perSize = size / partNum

      var i = 0
      var count = 0
      var index = 0
      while (i < partNum) {
        index = i * perSize
        var begin = Math.max(index - perSize + 1, 0)
        while (begin <= index) {
          count += hashMap.get(uniqueKeys(begin))
          begin += 1
        }
        metas.append(PartedByValueMeta(i, uniqueKeys(Math.max(begin - 1, 0)), index, count))
        i += 1
      }

      index += 1
      while (index < uniqueKeys.size) {
        count += hashMap.get(uniqueKeys(index))
        index += 1
      }
      metas.append(PartedByValueMeta(partNum, uniqueKeys.last, size - 1, count))
    }
  }
}
