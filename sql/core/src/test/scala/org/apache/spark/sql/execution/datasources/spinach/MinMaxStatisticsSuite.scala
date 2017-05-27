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

class MinMaxStatisticsSuite extends StatisticsTest {
  test("write function test") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray

    val hashMap = new java.util.HashMap[InternalRow, Long]()

    keys.indices.foreach(i => {
      hashMap.put(keys(i), i * 8L)
    })

    val minMax = new MinMaxStatistics
    minMax.write(schema, out, keys, null, hashMap)

    checkInternalRow(minMax.min, keys.head)
    checkInternalRow(minMax.max, keys.last)

    val bytes = out.buf.toByteArray

    var offset = 0L
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET) == 0) // MinMaxStatisticsType.id
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + 4) == 2) // stat size
    offset += 8

    // read minRow
    val minSize = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    val minKey = Statistics.getUnsafeRow(schema.length, bytes, offset, minSize)
    val minOffset = Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + offset + 4 + minSize)
    checkInternalRow(minKey, converter(keys.head))
    assert(minOffset == hashMap.get(keys.head))

    offset += 4 + minSize + 8

    // read maxRow
    val maxSize = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    val maxKey = Statistics.getUnsafeRow(schema.length, bytes, offset, maxSize)
    val maxOffset = Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + offset + 4 + maxSize)
    checkInternalRow(maxKey, converter(keys.last))
    assert(maxOffset == hashMap.get(keys.last))

    offset += 4 + maxSize + 8
  }

  test("read function test") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray

    val hashMap = new java.util.HashMap[InternalRow, Long]()

    keys.indices.foreach(i => {
      hashMap.put(keys(i), i * 8L)
    })

    // construct out buffer
    IndexUtils.writeInt(out, 0) // MinMaxStatisticsType.id
    IndexUtils.writeInt(out, 2) // stats size
    Statistics.writeInternalRow(converter, keys.head, out)
    IndexUtils.writeLong(out, hashMap.get(keys.head))
    Statistics.writeInternalRow(converter, keys.last, out)
    IndexUtils.writeLong(out, hashMap.get(keys.last))

    val bytes = out.buf.toByteArray
    val minMax = new MinMaxStatistics

    generateInterval(rowGen(-10), rowGen(-1), startInclude = true, endInclude = true)
    assert(minMax.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(301), rowGen(400), startInclude = true, endInclude = true)
    assert(minMax.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(300), rowGen(400), startInclude = true, endInclude = true)
    assert(minMax.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(30), rowGen(40), startInclude = true, endInclude = true)
    assert(minMax.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(-10), rowGen(1), startInclude = true, endInclude = true)
    assert(minMax.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.USE_INDEX)

    checkInternalRow(minMax.min, converter(keys.head))
    checkInternalRow(minMax.max, converter(keys.last))
  }

  test("read and write") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray
    val hashMap = new java.util.HashMap[InternalRow, Long]()
    keys.indices.foreach(i => {
      hashMap.put(keys(i), i * 8L)
    })

    val minMax = new MinMaxStatistics
    minMax.write(schema, out, keys, null, hashMap)

    val bytes = out.buf.toByteArray
    val minMaxRead = new MinMaxStatistics

    generateInterval(rowGen(-10), rowGen(-1), startInclude = true, endInclude = true)
    assert(minMaxRead.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(301), rowGen(400), startInclude = true, endInclude = true)
    assert(minMaxRead.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(300), rowGen(400), startInclude = true, endInclude = true)
    assert(minMaxRead.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(30), rowGen(40), startInclude = true, endInclude = true)
    assert(minMaxRead.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(-10), rowGen(1), startInclude = true, endInclude = true)
    assert(minMaxRead.read(schema, intervalArray, bytes, 0) == StaticsAnalysisResult.USE_INDEX)


    checkInternalRow(minMaxRead.min, converter(keys.head))
    checkInternalRow(minMaxRead.max, converter(keys.last))
  }
}
