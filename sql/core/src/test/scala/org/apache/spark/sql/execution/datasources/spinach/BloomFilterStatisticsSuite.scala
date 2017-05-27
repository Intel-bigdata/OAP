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

import scala.util.Random

import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.spinach.index.{BloomFilter, IndexUtils}
import org.apache.spark.sql.execution.datasources.spinach.statistics.{BloomFilterStatistics, BloomFilterStatisticsType, StaticsAnalysisResult, StatisticsManager}
import org.apache.spark.unsafe.Platform

class BloomFilterStatisticsSuite extends StatisticsTest {

  class TestBloomFilter extends BloomFilterStatistics {
    def getBloomFilter: BloomFilter = bfIndex
  }

  test("write function test") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    val maxBits = StatisticsManager.bloomFilterMaxBits
    val numOfHashFunc = StatisticsManager.bloomFilterHashFuncs
    val bfIndex = new BloomFilter(maxBits, numOfHashFunc)()
    val boundReference = schema.zipWithIndex.map(x =>
      BoundReference(x._2, x._1.dataType, nullable = true))
    val projectors = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
      UnsafeProjection.create(s.toArray)).toArray

    val testBloomFilter = new TestBloomFilter
    testBloomFilter.initialize(schema)
    for (key <- keys) {
      testBloomFilter.addSpinachKey(key)
      projectors.foreach(p => bfIndex.addValue(p(key).getBytes))
    }
    testBloomFilter.write(out, null)

    val bytes = out.buf.toByteArray
    var offset = 0L

    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      == BloomFilterStatisticsType.id)
    offset += 4

    val bitArrayLength = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    val numOfHashFuncFromFile = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset + 4)
    offset += 8
    assert(bitArrayLength == bfIndex.getBitMapLongArray.length)
    assert(numOfHashFuncFromFile== numOfHashFunc)

    val bfArray = new Array[Long](bitArrayLength)
    for (i <- 0 until bitArrayLength) {
      bfArray(i) = Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      offset += 8
    }
    val bfFromFile = BloomFilter(bfArray, numOfHashFunc)
    checkBloomFilter(bfFromFile, bfIndex)
  }

  test("read function test") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    val maxBits = StatisticsManager.bloomFilterMaxBits
    val numOfHashFunc = StatisticsManager.bloomFilterHashFuncs
    val bfIndex = new BloomFilter(maxBits, numOfHashFunc)()
    val boundReference = schema.zipWithIndex.map(x =>
      BoundReference(x._2, x._1.dataType, nullable = true))
    val projectors = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
      UnsafeProjection.create(s.toArray)).toArray

    for (key <- keys) {
      projectors.foreach(p => bfIndex.addValue(p(key).getBytes))
    }

    IndexUtils.writeInt(out, BloomFilterStatisticsType.id)
    IndexUtils.writeInt(out, bfIndex.getBitMapLongArray.length)
    IndexUtils.writeInt(out, bfIndex.getNumOfHashFunc)

    for (l <- bfIndex.getBitMapLongArray) IndexUtils.writeLong(out, l)

    val bytes = out.buf.toByteArray

    val testBloomFilter = new TestBloomFilter
    testBloomFilter.initialize(schema)
    testBloomFilter.read(bytes, 0)

    checkBloomFilter(testBloomFilter.getBloomFilter, bfIndex)
  }

  test("read AND write test") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    val maxBits = StatisticsManager.bloomFilterMaxBits
    val numOfHashFunc = StatisticsManager.bloomFilterHashFuncs
    val bfIndex = new BloomFilter(maxBits, numOfHashFunc)()
    val boundReference = schema.zipWithIndex.map(x =>
      BoundReference(x._2, x._1.dataType, nullable = true))
    val projectors = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
      UnsafeProjection.create(s.toArray)).toArray

    for (key <- keys) {
      projectors.foreach(p => bfIndex.addValue(p(key).getBytes))
    }

    val bloomFilterWrite = new TestBloomFilter
    bloomFilterWrite.initialize(schema)
    for (key <- keys) {
      bloomFilterWrite.addSpinachKey(key)
    }
    bloomFilterWrite.write(out, null)

    val bytes = out.buf.toByteArray

    val bloomFilterRead = new TestBloomFilter
    bloomFilterRead.initialize(schema)
    bloomFilterRead.read(bytes, 0)

    val bfIndexFromFile = bloomFilterRead.getBloomFilter
    assert(bfIndex.getBitMapLongArray.length == bfIndexFromFile.getBitMapLongArray.length)
    assert(bfIndex.getNumOfHashFunc == bfIndexFromFile.getNumOfHashFunc)

    checkBloomFilter(bfIndex, bloomFilterRead.getBloomFilter)
  }

  test("test analyze function") {
    val keys = Random.shuffle(1 to 300).map(i => rowGen(i)).toArray

    val maxBits = StatisticsManager.bloomFilterMaxBits
    val numOfHashFunc = StatisticsManager.bloomFilterHashFuncs
    val bfIndex = new BloomFilter(maxBits, numOfHashFunc)()
    val boundReference = schema.zipWithIndex.map(x =>
      BoundReference(x._2, x._1.dataType, nullable = true))
    val projectors = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
      UnsafeProjection.create(s.toArray)).toArray

    for (key <- keys) {
      projectors.foreach(p => bfIndex.addValue(p(key).getBytes))
    }

    val bloomFilterWrite = new TestBloomFilter
    bloomFilterWrite.initialize(schema)
    for (key <- keys) {
      bloomFilterWrite.addSpinachKey(key)
    }
    bloomFilterWrite.write(out, null)

    val bytes = out.buf.toByteArray

    val bloomFilterRead = new TestBloomFilter
    bloomFilterRead.initialize(schema)
    bloomFilterRead.read(bytes, 0)

    for (i <- 1 until 300) {
      generateInterval(rowGen(i), rowGen(i), true, true)
      assert(bloomFilterRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)
    }

    generateInterval(rowGen(10), rowGen(20), true, true)
    assert(bloomFilterRead.analyse(intervalArray) == StaticsAnalysisResult.USE_INDEX)
  }
}
