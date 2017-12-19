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

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.index.{IndexScanner, IndexUtils}
import org.apache.spark.sql.types.StructType


class SampleBasedStatisticsSuite extends StatisticsTest{

  class TestSample(schema: StructType) extends SampleBasedStatistics(schema) {
    override def takeSample(keys: ArrayBuffer[Key], size: Int): Array[Key] = keys.take(size).toArray
    def getSampleArray: Array[Key] = sampleArray
  }

  test("test write function") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray // keys needs to be sorted

    val testSample = new TestSample(schema)
    testSample.write(out, keys.to[ArrayBuffer])

    var offset = 0
    val fiber = wrapToFiberCache(out)
    assert(fiber.getInt(offset) == StatisticsType.TYPE_SAMPLE_BASE)
    offset += 4
    val size = fiber.getInt(offset)
    offset += 4

    var rowOffset = 0
    for (i <- 0 until size) {
      val row = nnkr.readKey(fiber, offset + size * 4 + rowOffset)._1
      rowOffset = fiber.getInt(offset + i * 4)
      assert(ordering.compare(row, keys(i)) == 0)
    }
  }

  test("read function test") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray // random order
    val size = (Random.nextInt() % 200 + 200) % 200 + 10 // assert nonEmpty sample
    assert(size >= 0 && size <= 300)

    IndexUtils.writeInt(out, StatisticsType.TYPE_SAMPLE_BASE)
    IndexUtils.writeInt(out, size)

    val tempWriter = new ByteArrayOutputStream()
    for (idx <- 0 until size) {
      nnkw.writeKey(tempWriter, keys(idx))
      IndexUtils.writeInt(out, tempWriter.size)
    }
    out.write(tempWriter.toByteArray)

    val fiber = wrapToFiberCache(out)

    val testSample = new TestSample(schema)
    testSample.read(fiber, 0)

    val array = testSample.getSampleArray

    for (i <- array.indices) {
      assert(ordering.compare(keys(i), array(i)) == 0)
    }
  }

  test("read and write") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    val sampleWrite = new TestSample(schema)
    sampleWrite.write(out, keys.to[ArrayBuffer])

    val fiber = wrapToFiberCache(out)

    val sampleRead = new TestSample(schema)
    sampleRead.read(fiber, 0)

    val array = sampleRead.getSampleArray

    for (i <- array.indices) {
      assert(ordering.compare(keys(i), array(i)) == 0)
    }
  }

  test("test analyze function") {
    // TODO: Give a seed to Random in here and in SampleBasedStatistics without losing coverage
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray
    val dummyStart = new JoinedRow(InternalRow(1), IndexScanner.DUMMY_KEY_START)
    val dummyEnd = new JoinedRow(InternalRow(300), IndexScanner.DUMMY_KEY_END)

    val sampleWrite = new TestSample(schema)
    sampleWrite.write(out, keys.to[ArrayBuffer])

    val fiber = wrapToFiberCache(out)

    val sampleRead = new TestSample(schema)
    sampleRead.read(fiber, 0)

    generateInterval(rowGen(-10), rowGen(-1), startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(301), rowGen(400), startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(dummyStart, dummyEnd,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.FULL_SCAN)

    generateInterval(dummyStart, rowGen(0),
      startInclude = true, endInclude = false)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(dummyStart, rowGen(300),
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.FULL_SCAN)

    generateInterval(rowGen(0), dummyEnd,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.FULL_SCAN)

    generateInterval(rowGen(1), dummyEnd,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.FULL_SCAN)

    generateInterval(rowGen(300), dummyEnd,
      startInclude = false, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(301), dummyEnd,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)
  }
}

