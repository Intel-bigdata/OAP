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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.index.{IndexScanner, IndexUtils, RangeInterval}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class StatisticsSuite extends StatisticsTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    schema = StructType(StructField("test", DoubleType, nullable = true) :: Nil)
  }

  val row1 = InternalRow(1.0)
  val row2 = InternalRow(2.0)
  val row3 = InternalRow(3.0)

  class TestStatistics(schema: StructType) extends Statistics(schema) {
    override val id: Int = 6662
  }

  test("Statistics write function test") {
    val test = new TestStatistics(schema)
    val writtenBytes = test.write(out, null)
    assert(writtenBytes == 4)

    val fiber = wrapToFiberCache(out)
    assert(fiber.getInt(0) == test.id)
    out.close()
  }

  test("Statistics read function test") {
    val test = new TestStatistics(schema)
    IndexUtils.writeInt(out, test.id)

    val fiber = wrapToFiberCache(out)

    val readBytes = test.read(fiber, 0)
    assert(readBytes == 4)
  }

  test("Statistics default analyzer test") {
    val test = new TestStatistics(schema)
    IndexUtils.writeInt(out, test.id)

    val fiber = wrapToFiberCache(out)

    val readBytes = test.read(fiber, 0)
    assert(readBytes == 4)

    val analyzeResult = test.analyse(new ArrayBuffer[RangeInterval]())
    assert(analyzeResult == StaticsAnalysisResult.USE_INDEX)
  }


  test("rowInSingleInterval: normal test") {
    assert(Statistics.rowInSingleInterval(row2,
      RangeInterval(row1, row3, true, true), ordering, ordering), "2.0 is in [1.0, 3.0]")
    assert(!Statistics.rowInSingleInterval(row3,
      RangeInterval(row1, row2, true, true), ordering, ordering), "3.0 is not in [1.0, 2.0]")
    assert(Statistics.rowInSingleInterval(row1,
      RangeInterval(IndexScanner.DUMMY_KEY_START, row2, false, true), partialOrdering, ordering),
      "1.0 is in (-inf, 2.0]")
    assert(Statistics.rowInSingleInterval(row2,
      RangeInterval(IndexScanner.DUMMY_KEY_START, row2, false, true), partialOrdering, ordering),
      "2.0 is in (-inf, 2.0]")
    assert(!Statistics.rowInSingleInterval(row2,
      RangeInterval(IndexScanner.DUMMY_KEY_START, row2, false, false), partialOrdering, ordering),
      "2.0 is not in (-inf, 2.0)")
    assert(!Statistics.rowInSingleInterval(row3,
      RangeInterval(IndexScanner.DUMMY_KEY_START, row2, false, true), partialOrdering, ordering),
      "3.0 is not in (-inf, 2.0]")
    assert(!Statistics.rowInSingleInterval(row1,
      RangeInterval(row2, IndexScanner.DUMMY_KEY_END, true, false), ordering, partialOrdering),
      "1.0 is in [2, +inf)")
    assert(Statistics.rowInSingleInterval(row2,
      RangeInterval(row2, IndexScanner.DUMMY_KEY_END, true, false), ordering, partialOrdering),
      "2.0 is in [2, +inf)")
    assert(!Statistics.rowInSingleInterval(row2,
      RangeInterval(row2, IndexScanner.DUMMY_KEY_END, false, false), ordering, partialOrdering),
      "2.0 is not in (2, +inf)")
    assert(Statistics.rowInSingleInterval((row3),
      RangeInterval(row2, IndexScanner.DUMMY_KEY_END, true, false), ordering, partialOrdering),
      "3.0 is in [2, +inf)")
    assert(Statistics.rowInSingleInterval(row3,
      RangeInterval(IndexScanner.DUMMY_KEY_START, IndexScanner.DUMMY_KEY_END, false, false),
      partialOrdering, partialOrdering), "3.0 is in (-inf, +inf)")
  }

  test("rowInSingleInterval: bound test") {
    assert(!Statistics.rowInSingleInterval(row1,
      RangeInterval(row1, row1, false, false), ordering, ordering), "1.0 is not in (1.0, 1.0)")
    assert(!Statistics.rowInSingleInterval(row1,
      RangeInterval(row1, row1, false, true), ordering, ordering), "1.0 is not in (1.0, 1.0]")
    assert(!Statistics.rowInSingleInterval(row1,
      RangeInterval(row1, row1, true, false), ordering, ordering), "1.0 is not in [1.0, 1.0)")
    assert(Statistics.rowInSingleInterval(row1,
      RangeInterval(row1, row1, true, true), ordering, ordering), "1.0 is in [1.0, 1.0]")
  }

  test("rowInSingleInterval: wrong interval test") {
    assert(!Statistics.rowInSingleInterval(row2,
      RangeInterval(row3, row2, false, false), ordering, ordering), "2.0 is not in (3.0, 2.0)")
    assert(!Statistics.rowInSingleInterval(row2,
      RangeInterval(row3, row2, false, true), ordering, ordering), "2.0 is not in (3.0, 2.0]")
    assert(!Statistics.rowInSingleInterval(row2,
      RangeInterval(row3, row2, true, false), ordering, ordering), "2.0 is not in [3.0, 2.0)")
    assert(!Statistics.rowInSingleInterval(row2,
      RangeInterval(row3, row2, true, true), ordering, ordering), "2.0 is not in [3.0, 2.0]")
  }

  test("rowInIntervalArray") {
    assert(!Statistics.rowInIntervalArray(row1,
      null, ordering, ordering), "intervalArray is null")
    assert(Statistics.rowInIntervalArray(InternalRow(1.5),
      ArrayBuffer(RangeInterval(row1, row2, false, false),
        RangeInterval(row2, row3, false, false)), ordering, partialOrdering),
      "1.5 is in (1,2) union (2,3)")
    assert(!Statistics.rowInIntervalArray(InternalRow(-1.0),
      ArrayBuffer(RangeInterval(row1, row2, false, false),
        RangeInterval(row2, row3, false, false)), ordering, partialOrdering),
      "-1.0 is not in (1,2) union (2,3)")
  }
}
