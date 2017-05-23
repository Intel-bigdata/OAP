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

import org.apache.spark.sql.execution.datasources.spinach.index.IndexUtils
import org.apache.spark.sql.execution.datasources.spinach.statistics._
import org.apache.spark.unsafe.Platform

class SampleBasedStatisticsSuite extends StatisticsTest{

  class TestSampleBaseStatistics(rate: Double = 0.1) extends SampleBasedStatistics(rate) {
    override def takeSample(keys: Array[Key], size: Int): Array[Key] = keys.take(size)
  }

  test("test write function") {
    val sampleRate = 0.2
    val keys = (1 to 300).map(i => rowGen(i)).toArray
    val sampleSize = (keys.length * sampleRate).toInt

    val statistics = new TestSampleBaseStatistics(sampleRate)
    statistics.write(schema, out, keys, null, null)

    val bytes = out.buf.toByteArray

    var offset = 0L
    // header
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET) == 1) // SampleBasedStatisticsType.id
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + 4) == sampleSize)
    offset += 8

    // content
    for (i <- 0 until sampleSize) {
      val size = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      val row = Statistics.getUnsafeRow(schema.length, bytes, offset, size)
      checkInternalRow(row, converter(keys(i)))
      offset += 4 + size
    }
  }

  test("read function test") {
    val sampleRate = 0.2
    val keys = (1 to 300).map(i => rowGen(i)).toArray
    val sampleSize = (keys.length * sampleRate).toInt

    IndexUtils.writeInt(out, 1) // SampleBasedStatisticsType.id
    IndexUtils.writeInt(out, sampleSize)

    for (i <- 0 until sampleSize) {
      Statistics.writeInternalRow(converter, keys(i), out)
    }

    val bytes = out.buf.toByteArray


    val statistics = new TestSampleBaseStatistics()

    generateInterval(rowGen(-10), rowGen(-1), startInclude = true, endInclude = true)
    val res1 = statistics.read(schema, intervalArray, bytes, 0)
    assert(res1 <= 0)

    generateInterval(rowGen(301), rowGen(400), startInclude = true, endInclude = true)
    val res2 = statistics.read(schema, intervalArray, bytes, 0)
    assert(res2 <= 0)

    generateInterval(rowGen(1), rowGen(300), startInclude = true, endInclude = true)
    val res3 = statistics.read(schema, intervalArray, bytes, 0)
    assert(res3 == 1.0) // all
  }

  test("read and write") {
    val sampleRate = 0.2
    val keys = (1 to 300).map(i => rowGen(i)).toArray

    // use random shuffle for integration test
    val statistics = new SampleBasedStatistics(sampleRate)
    statistics.write(schema, out, keys, null, null)

    val bytes = out.buf.toByteArray

    val statisticsRead = new SampleBasedStatistics()

    generateInterval(rowGen(-10), rowGen(-1), startInclude = true, endInclude = true)
    val res1 = statisticsRead.read(schema, intervalArray, bytes, 0)
    assert(res1 <= 0)

    generateInterval(rowGen(301), rowGen(400), startInclude = true, endInclude = true)
    val res2 = statisticsRead.read(schema, intervalArray, bytes, 0)
    assert(res2 <= 0)

    generateInterval(rowGen(1), rowGen(300), startInclude = true, endInclude = true)
    val res3 = statisticsRead.read(schema, intervalArray, bytes, 0)
    assert(res3 == 1.0) // all
  }
}

