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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.oap.index.{IndexScanner, IndexUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType


class PartByValueStatisticsSuite extends StatisticsTest {

  class TestPartByValueWriter(schema: StructType)
    extends PartByValueStatisticsWriter(schema, new Configuration()) {
    def getMetas: ArrayBuffer[PartedByValueMeta] = metas
  }

  class TestPartByValueReader(schema: StructType) extends PartByValueStatisticsReader(schema) {
    def getMetas: ArrayBuffer[PartedByValueMeta] = metas
  }

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
    val keys = (1 to 300).map(i => rowGen(i)).toArray // keys needs to be sorted

    val testPartByValueWriter = new TestPartByValueWriter(schema)
    testPartByValueWriter.write(out, keys.to[ArrayBuffer])

    var offset = 0
    val fiber = wrapToFiberCache(out)
    assert(fiber.getInt(offset) == StatisticsType.TYPE_PART_BY_VALUE)
    offset += 4

    val part = SQLConf.OAP_STATISTICS_PART_NUM.defaultValue.get + 1
    val cntPerPart = keys.length / (part - 1)
    assert(fiber.getInt(offset) == part)
    offset += 4

    var rowOffset = 0
    for (i <- 0 until part) {
      val curMaxIdx = Math.min(i * cntPerPart, keys.length - 1)
      assert(fiber.getInt(offset + i * 12) == curMaxIdx) // index
      assert(fiber.getInt(offset + i * 12 + 4) == (curMaxIdx + 1)) // count

      val row = nnkr.readKey(fiber, offset + part * 12 + rowOffset)._1
      rowOffset = fiber.getInt(offset + i * 12 + 8)
      checkInternalRow(row, keys(curMaxIdx)) // row
    }
  }

  test("read function test") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray // keys needs to be sorted
    val content = Array(1, 61, 121, 181, 241, 300)
    val curMaxId = Array(0, 60, 120, 180, 240, 299)
    val curAccumuCount = Array(1, 61, 121, 181, 241, 300)

    val partNum = 6

    IndexUtils.writeInt(out, StatisticsType.TYPE_PART_BY_VALUE)
    IndexUtils.writeInt(out, partNum)
    val tempWriter = new ByteArrayOutputStream()
    for (i <- content.indices) {
      nnkw.writeKey(tempWriter, rowGen(content(i)))
      IndexUtils.writeInt(out, curMaxId(i))
      IndexUtils.writeInt(out, curAccumuCount(i))
      IndexUtils.writeInt(out, tempWriter.size())
    }
    out.write(tempWriter.toByteArray)

    val fiber = wrapToFiberCache(out)
    val testPartByValueReader = new TestPartByValueReader(schema)
    testPartByValueReader.read(fiber, 0)

    val metas = testPartByValueReader.getMetas
    for (i <- metas.indices) {
      assert(metas(i).idx == i)
      assert(ordering.compare(metas(i).row, keys(curMaxId(i))) == 0)
      assert(ordering.compare(metas(i).row, rowGen(content(i))) == 0)
      assert(metas(i).curMaxId == curMaxId(i))
      assert(metas(i).accumulatorCnt == curAccumuCount(i))
    }

  }

  test("read and write") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray // keys needs to be sorted

    val partByValueWrite = new TestPartByValueWriter(schema)
    partByValueWrite.write(out, keys.to[ArrayBuffer])

    val fiber = wrapToFiberCache(out)

    val partByValueRead = new TestPartByValueReader(schema)
    partByValueRead.read(fiber, 0)

    val content = Array(1, 61, 121, 181, 241, 300)
    val curMaxId = Array(0, 60, 120, 180, 240, 299)
    val curAccumuCount = Array(1, 61, 121, 181, 241, 300)

    val metas = partByValueRead.getMetas
    for (i <- metas.indices) {
      assert(metas(i).idx == i)
      assert(ordering.compare(metas(i).row, keys(curMaxId(i))) == 0)
      assert(ordering.compare(metas(i).row, rowGen(content(i))) == 0)
      assert(metas(i).curMaxId == curMaxId(i))
      assert(metas(i).accumulatorCnt == curAccumuCount(i))
    }
  }

  test("test analyze function") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray // keys needs to be sorted
    val dummyStart = new JoinedRow(InternalRow(1), IndexScanner.DUMMY_KEY_START)
    val dummyEnd = new JoinedRow(InternalRow(300), IndexScanner.DUMMY_KEY_END)

    val partByValueWrite = new TestPartByValueWriter(schema)
    partByValueWrite.write(out, keys.to[ArrayBuffer])

    val fiber = wrapToFiberCache(out)

    val partByValueRead = new TestPartByValueReader(schema)
    partByValueRead.read(fiber, 0)

    generateInterval(dummyStart, dummyEnd, true, true)
    assert(partByValueRead.analyse(intervalArray) == 1.0)

    generateInterval(rowGen(1), rowGen(301), true, true)
    assert(partByValueRead.analyse(intervalArray) == 1.0)

    generateInterval(rowGen(10), rowGen(70), true, true)
    assert(partByValueRead.analyse(intervalArray) == 0.4)

    generateInterval(rowGen(10), rowGen(190), true, true)
    assert(partByValueRead.analyse(intervalArray) == 0.8)

    generateInterval(rowGen(-10), rowGen(10), true, true)
    assert(partByValueRead.analyse(intervalArray) == 31.0 / 300)

    generateInterval(rowGen(-10), rowGen(0), true, true)
    assert(partByValueRead.analyse(intervalArray) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(310), rowGen(400), true, true)
    assert(partByValueRead.analyse(intervalArray) == StaticsAnalysisResult.SKIP_INDEX)

    generateInterval(rowGen(-10), rowGen(0), true, true)
    assert(partByValueRead.analyse(intervalArray) == StaticsAnalysisResult.SKIP_INDEX)
  }
}
