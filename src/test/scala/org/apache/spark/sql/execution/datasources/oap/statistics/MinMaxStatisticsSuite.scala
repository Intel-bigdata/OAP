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

import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.index.{IndexScanner, IndexUtils}
import org.apache.spark.sql.types.StructType

class MinMaxStatisticsSuite extends StatisticsTest {

  private class TestMinMaxWriter(schema: StructType)
    extends MinMaxStatisticsWriter(schema, new Configuration()) {
    def getMin: InternalRow = min
    def getMax: InternalRow = max
  }

  private class TestMinMaxReader(schema: StructType) extends MinMaxStatisticsReader(schema) {
    def getMin: InternalRow = min
    def getMax: InternalRow = max
  }

  test("write function test") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    val testMinMax = new TestMinMaxWriter(schema)
    for (key <- keys) testMinMax.addOapKey(key)
    testMinMax.write(out, null) // MinMax does not need sortKeys parameter

    val min = testMinMax.getMin
    val max = testMinMax.getMax
    for (key <- keys) {
      assert(ordering.compare(key, min) >= 0)
      assert(ordering.compare(key, max) <= 0)
    }

    val fiber = wrapToFiberCache(out)
    var offset = 0

    assert(fiber.getInt(0) == StatisticsType.TYPE_MIN_MAX)
    offset += 4
    val minSize = fiber.getInt(offset)
    offset += 4
    val totalSize = fiber.getInt(offset + 4)
    offset += 4
    val minFromFile = nnkr.readKey(fiber, offset)._1
    checkInternalRow(minFromFile, rowGen(1))

    val maxFromFile = nnkr.readKey(fiber, offset + minSize)._1
    checkInternalRow(maxFromFile, rowGen(300))
    offset += (4 + totalSize)
  }

  test("read function test") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    IndexUtils.writeInt(out, StatisticsType.TYPE_MIN_MAX)
    val tempWriter = new ByteArrayOutputStream()
    nnkw.writeKey(tempWriter, rowGen(1))
    IndexUtils.writeInt(out, tempWriter.size)
    nnkw.writeKey(tempWriter, rowGen(300))
    IndexUtils.writeInt(out, tempWriter.size)
    out.write(tempWriter.toByteArray)

    val fiber = wrapToFiberCache(out)

    val testMinMax = new TestMinMaxReader(schema)
    testMinMax.read(fiber, 0)

    val min = testMinMax.getMin
    val max = testMinMax.getMax

    for (key <- keys) {
      assert(ordering.compare(key, min) >= 0)
      assert(ordering.compare(key, max) <= 0)
    }

    assert(ordering.compare(rowGen(1), min) == 0)
    assert(ordering.compare(rowGen(300), max) == 0)
  }

  test("read and write") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    val minmaxWrite = new TestMinMaxWriter(schema)
    for (key <- keys) minmaxWrite.addOapKey(key)
    minmaxWrite.write(out, null) // MinMax does not need sortKeys parameter

    val fiber = wrapToFiberCache(out)

    val minmaxRead = new TestMinMaxReader(schema)
    minmaxRead.read(fiber, 0)
    val min = minmaxRead.getMin
    val max = minmaxRead.getMax

    for (key <- keys) {
      assert(ordering.compare(key, min) >= 0)
      assert(ordering.compare(key, max) <= 0)
    }

    assert(ordering.compare(rowGen(1), min) == 0)
    assert(ordering.compare(rowGen(300), max) == 0)
  }

  test("analyse function test") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    val minmaxWrite = new TestMinMaxWriter(schema)
    for (key <- keys) minmaxWrite.addOapKey(key)
    minmaxWrite.write(out, null) // MinMax does not need sortKeys parameter

    val fiber = wrapToFiberCache(out)

    val minmaxRead = new TestMinMaxReader(schema)
    minmaxRead.read(fiber, 0)

    generateInterval(rowGen(-10), rowGen(-1), startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(301), rowGen(400), startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(300), rowGen(400), startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(30), rowGen(40), startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(-10), rowGen(1), startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(IndexScanner.DUMMY_KEY_START, IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(IndexScanner.DUMMY_KEY_START, rowGen(1),
      startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(IndexScanner.DUMMY_KEY_START, rowGen(0),
      startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(IndexScanner.DUMMY_KEY_START, rowGen(300),
      startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(0), IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(1), IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(150), IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(300), IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(301), IndexScanner.DUMMY_KEY_END,
      startInclude = true, endInclude = true)
    assert(minmaxRead.analyse(intervalArray) == StaticsAnalysisResult.SKIP_INDEX)
  }
}
