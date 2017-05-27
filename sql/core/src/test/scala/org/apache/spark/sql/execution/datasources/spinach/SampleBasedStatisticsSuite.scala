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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.execution.datasources.spinach.index.{IndexScanner, IndexUtils}
import org.apache.spark.sql.execution.datasources.spinach.statistics._
import org.apache.spark.unsafe.Platform

class SampleBasedStatisticsSuite extends StatisticsTest{

  class TestSample extends SampleBasedStatistics {
    override def takeSample(keys: ArrayBuffer[Key], size: Int): Array[Key] = keys.take(size).toArray
    def getSampleArray: Array[Key] = sampleArray
  }

  test("test write function") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray // keys needs to be sorted

    val testSample = new TestSample
    testSample.initialize(schema)
    testSample.write(out, keys.to[ArrayBuffer])

    var offset = 0L
    val bytes = out.buf.toByteArray
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      == SampleBasedStatisticsType.id)
    offset += 4
    val size = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    offset += 4

    for (i <- 0 until size) {
      val rowSize = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      val row = Statistics.getUnsafeRow(schema.length, bytes, offset, rowSize).copy()
      assert(ordering.compare(row, keys(i)) == 0)
      offset += 4 + rowSize
    }
  }

  test("read function test") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray // random order
    val size = (Random.nextInt() % 200 + 200) % 200 + 10 // assert nonEmpty sample
    assert(size >= 0 && size <= 300)

    IndexUtils.writeInt(out, SampleBasedStatisticsType.id)
    IndexUtils.writeInt(out, size)

    for (idx <- 0 until size) {
      Statistics.writeInternalRow(converter, keys(idx), out)
    }

    val bytes = out.buf.toByteArray

    val testSample = new TestSample
    testSample.initialize(schema)
    testSample.read(bytes, 0)

    val array = testSample.getSampleArray

    for (i <- array.indices) {
      assert(ordering.compare(keys(i), array(i)) == 0)
    }
  }

  test("read and write") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    val sampleWrite = new TestSample
    sampleWrite.initialize(schema)
    sampleWrite.write(out, keys.to[ArrayBuffer])

    val bytes = out.buf.toByteArray

    val sampleRead = new TestSample
    sampleRead.initialize(schema)
    sampleRead.read(bytes, 0)

    val array = sampleRead.getSampleArray

    for (i <- array.indices) {
      assert(ordering.compare(keys(i), array(i)) == 0)
    }
  }

  test("test analyze function") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    val sampleWrite = new TestSample
    sampleWrite.initialize(schema)
    sampleWrite.write(out, keys.to[ArrayBuffer])

    val bytes = out.buf.toByteArray

    val sampleRead = new TestSample
    sampleRead.initialize(schema)
    sampleRead.read(bytes, 0)

    generateInterval(rowGen(-10), rowGen(-1), startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(301), rowGen(400), startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(IndexScanner.DUMMY_KEY_START, IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.FULL_SCAN)

    generateInterval(IndexScanner.DUMMY_KEY_START, rowGen(0),
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(IndexScanner.DUMMY_KEY_START, rowGen(300),
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.FULL_SCAN)

    generateInterval(rowGen(0), IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.FULL_SCAN)

    generateInterval(rowGen(1), IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.FULL_SCAN)

    generateInterval(rowGen(300), IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(301), IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)
  }
}

