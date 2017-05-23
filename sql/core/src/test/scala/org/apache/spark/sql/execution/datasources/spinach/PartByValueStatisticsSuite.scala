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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.spinach.index.IndexUtils
import org.apache.spark.sql.execution.datasources.spinach.statistics._
import org.apache.spark.unsafe.Platform

class PartByValueStatisticsSuite extends StatisticsTest{
  // for all data in this suite, all internal rows appear only once
  // 1, 2, 3, ..., 300
  // `partNum` = 5, then the file content should be
  //    RowContent      curMaxIdx   curAccumulatorCount
  // (  1,  "test#1")       0              1
  // ( 61,  "test#61")     60             61
  // (121,  "test#121")   120            121
  // (181,  "test#181")   180            181
  // (241,  "test#241")   240            241
  // (300,  "test#300")   299            300

  test("test write function") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray
    val hashMap = new java.util.HashMap[InternalRow, java.util.ArrayList[Long]]()
    val part = 6
    val cntPerPart = keys.length / (part - 1)

    for (i <- keys.indices) {
      val key = keys(i)
      val offsetList = new java.util.ArrayList[Long]()
      offsetList.add(i * 8)
      hashMap.put(key, offsetList)
    }

    val statistics = new PartedByValueStatistics()
    statistics.write(schema, out, keys, hashMap, null)

    val bytes = out.buf.toByteArray

    var offset = 0L
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET) == 2) // PartByValueStatisticsType.id
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + 4) == part) // part count
    offset += 8

    for (i <- 0 until part) {
      val curMaxIdx = Math.min(i * cntPerPart, keys.length - 1)
      val size = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      val row = Statistics.getUnsafeRow(schema.length, bytes, offset, size)
      checkInternalRow(row, converter(keys(curMaxIdx))) // row
      offset += size + 4

      assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset) == curMaxIdx) // index
      assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset + 4)
        == (curMaxIdx + 1)) // count
      offset += 8
    }
  }

  test("read function test") {
    val content = Array(1, 61, 121, 181, 241, 300)
    val curMaxId = Array(0, 60, 120, 180, 240, 299)
    val curAccumuCount = Array(1, 61, 121, 181, 241, 300)

    IndexUtils.writeInt(out, 2) // PartByValueStatisticsType.id
    IndexUtils.writeInt(out, 6) // partNum

    for (i <- content.indices) {
      Statistics.writeInternalRow(converter, rowGen(content(i)), out)
      IndexUtils.writeInt(out, curMaxId(i))
      IndexUtils.writeInt(out, curAccumuCount(i))
    }

    val bytes = out.buf.toByteArray
    val statistics = new PartedByValueStatistics()

    generateInterval(rowGen(10), rowGen(70), true, true)
    assert(statistics.read(schema, intervalArray, bytes, 0) == 0.4)

    generateInterval(rowGen(10), rowGen(190), true, true)
    assert(statistics.read(schema, intervalArray, bytes, 0) == 0.8)

    generateInterval(rowGen(-10), rowGen(10), true, true)
    assert(statistics.read(schema, intervalArray, bytes, 0) == 31.0 / 300)

    generateInterval(rowGen(-10), rowGen(0), true, true)
    assert(statistics.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(310), rowGen(400), true, true)
    assert(statistics.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(-10), rowGen(0), true, true)
    assert(statistics.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.SKIP_INDEX)
  }

  test("read and write") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray
    val hashMap = new java.util.HashMap[InternalRow, java.util.ArrayList[Long]]()

    for (i <- keys.indices) {
      val key = keys(i)
      val offsetList = new java.util.ArrayList[Long]()
      offsetList.add(i * 8)
      hashMap.put(key, offsetList)
    }

    val statistics = new PartedByValueStatistics()
    statistics.write(schema, out, keys, hashMap, null)

    val bytes = out.buf.toByteArray

    val statisticsRead = new PartedByValueStatistics()

    generateInterval(rowGen(10), rowGen(70), true, true)
    assert(statisticsRead.read(schema, intervalArray, bytes, 0) == 0.4)

    generateInterval(rowGen(10), rowGen(190), true, true)
    assert(statisticsRead.read(schema, intervalArray, bytes, 0) == 0.8)

    generateInterval(rowGen(-10), rowGen(10), true, true)
    assert(statisticsRead.read(schema, intervalArray, bytes, 0) == 31.0 / 300)

    generateInterval(rowGen(-10), rowGen(0), true, true)
    assert(statisticsRead.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(310), rowGen(400), true, true)
    assert(statisticsRead.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(-10), rowGen(0), true, true)
    assert(statisticsRead.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.SKIP_INDEX)
  }
}
