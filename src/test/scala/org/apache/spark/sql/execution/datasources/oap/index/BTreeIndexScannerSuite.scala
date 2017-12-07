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

package org.apache.spark.sql.execution.datasources.oap.index

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils


class BTreeIndexScannerSuite extends SharedOapContext {

  // Override afterEach because we do not want to check open streams
  override def beforeEach(): Unit = {}
  override def afterEach(): Unit = {}

  test("test rowOrdering") {
    // Only check Integer is enough. We use [[GenerateOrdering]] to handle different data types.
    val fields = StructField("col1", IntegerType) :: StructField("col2", IntegerType) :: Nil
    val singleColumnReader = BTreeIndexRecordReader(configuration, StructType(fields.take(1)))
    val multiColumnReader = BTreeIndexRecordReader(configuration, StructType(fields))
    // Compare DUMMY_START
    val x1 = IndexScanner.DUMMY_KEY_START
    val y1 = InternalRow(Int.MinValue)
    assert(singleColumnReader.rowOrdering(x1, y1, isStart = true) === -1)
    assert(singleColumnReader.rowOrdering(y1, x1, isStart = true) === 1)
    // Compare DUMMY_END
    val x2 = IndexScanner.DUMMY_KEY_END
    val y2 = InternalRow(Int.MaxValue)
    assert(singleColumnReader.rowOrdering(x2, y2, isStart = false) === 1)
    assert(singleColumnReader.rowOrdering(y2, x2, isStart = false) === -1)
    // Compare multiple numFields with DUMMY_START
    val x3 = new JoinedRow(InternalRow(1), IndexScanner.DUMMY_KEY_START)
    val y3 = new JoinedRow(InternalRow(1), InternalRow(Int.MinValue))
    assert(multiColumnReader.rowOrdering(x3, y3, isStart = true) === -1)
    assert(multiColumnReader.rowOrdering(y3, x3, isStart = true) === 1)
    // Compare multiple numFields with DUMMY_END
    val x4 = new JoinedRow(InternalRow(1), IndexScanner.DUMMY_KEY_END)
    val y4 = new JoinedRow(InternalRow(1), InternalRow(Int.MaxValue))
    assert(multiColumnReader.rowOrdering(x4, y4, isStart = false) === 1)
    assert(multiColumnReader.rowOrdering(y4, x4, isStart = false) === -1)
    // Compare normal single row
    val x5 = InternalRow(1)
    val y5 = InternalRow(5)
    assert(singleColumnReader.rowOrdering(x5, y5, isStart = false) === -1)
    assert(singleColumnReader.rowOrdering(x5, x5, isStart = false) === 0)
    assert(singleColumnReader.rowOrdering(y5, x5, isStart = false) === 1)
    // Compare normal multiple row
    val x6 = new JoinedRow(InternalRow(1), InternalRow(5))
    val y6 = new JoinedRow(InternalRow(1), InternalRow(100))
    assert(multiColumnReader.rowOrdering(x6, y6, isStart = false) === -1)
    assert(multiColumnReader.rowOrdering(x6, x6, isStart = false) === 0)
    assert(multiColumnReader.rowOrdering(y6, x6, isStart = false) === 1)
  }

  test("test binarySearch") {
    val schema = StructType(StructField("col1", IntegerType) :: Nil)
    val reader = BTreeIndexRecordReader(configuration, schema)
    val values = Seq(1, 11, 21, 31, 41, 51, 61, 71, 81, 91)
    def keyAt(idx: Int): InternalRow = InternalRow(values(idx))
    val ordering = GenerateOrdering.create(schema)

    def assertPosition(candidate: Int, position: Int, exists: Boolean): Unit = {
      assert(
        reader.binarySearch(
          0, values.size, keyAt, InternalRow(candidate), ordering.compare) === (position, exists))
    }
    assertPosition(1, 0, exists = true)
    assertPosition(10, 1, exists = false)
    assertPosition(11, 1, exists = true)
    assertPosition(20, 2, exists = false)
    assertPosition(21, 2, exists = true)
    assertPosition(30, 3, exists = false)
    assertPosition(31, 3, exists = true)
    assertPosition(40, 4, exists = false)
    assertPosition(41, 4, exists = true)
    assertPosition(50, 5, exists = false)
    assertPosition(51, 5, exists = true)
    assertPosition(60, 6, exists = false)
    assertPosition(61, 6, exists = true)
    assertPosition(70, 7, exists = false)
    assertPosition(71, 7, exists = true)
    assertPosition(80, 8, exists = false)
    assertPosition(81, 8, exists = true)
    assertPosition(90, 9, exists = false)
    assertPosition(91, 9, exists = true)
  }

  test("test findRowIdRange for normal case") {
    val schema = StructType(StructField("col", IntegerType) :: Nil)
    val path = new Path(Utils.createTempDir().getAbsolutePath, "tempIndexFile")
    val fileWriter = BTreeIndexFileWriter(configuration, path)
    val writer = BTreeIndexRecordWriter(configuration, fileWriter, schema)
    // Values structure depends on BTreeUtils.generate2()
    // Count = 150, node = 5, (1 - 59), (61 - 119), (121 - 179), (181 - 239) (241 - 299)
    (1 to 300 by 2).map(InternalRow(_)).foreach(writer.write(null, _))
    writer.close(null)

    val reader = BTreeIndexRecordReader(configuration, schema)
    reader.initialize(path, new ArrayBuffer[RangeInterval]())

    // DUMMY_START <= x <= DUMMY_END
    assert(reader.findRowIdRange(RangeInterval(
      IndexScanner.DUMMY_KEY_START, IndexScanner.DUMMY_KEY_END,
      includeStart = true, includeEnd = true)) === (0, 150))
    // DUMMY_START < x <= Max
    assert(reader.findRowIdRange(RangeInterval(
      IndexScanner.DUMMY_KEY_START, InternalRow(299),
      includeStart = true, includeEnd = true)) === (0, 150))
    // DUMMY_START < x < Max
    assert(reader.findRowIdRange(RangeInterval(
      IndexScanner.DUMMY_KEY_START, InternalRow(299),
      includeStart = true, includeEnd = false)) === (0, 149))
    // Min <= x < DUMMY_END
    assert(reader.findRowIdRange(RangeInterval(
      InternalRow(1), IndexScanner.DUMMY_KEY_END,
      includeStart = true, includeEnd = false)) === (0, 150))
    // Min < x < DUMMY_END
    assert(reader.findRowIdRange(RangeInterval(
      InternalRow(1), IndexScanner.DUMMY_KEY_END,
      includeStart = false, includeEnd = false)) === (1, 150))
    // 2 < x <= 4 (Target not exist in values)
    assert(reader.findRowIdRange(RangeInterval(
      InternalRow(2), InternalRow(4), includeStart = false, includeEnd = true)) === (1, 2))
    // 59 < x <= 119 (get the next position of target value)
    assert(reader.findRowIdRange(RangeInterval(
      InternalRow(59), InternalRow(119), includeStart = false, includeEnd = true)) === (30, 60))
    // 1 < x < 1
    assert(reader.findRowIdRange(RangeInterval(
      InternalRow(1), InternalRow(1), includeStart = false, includeEnd = false)) === (1, 0))
    // 1 <= x <= 1
    assert(reader.findRowIdRange(RangeInterval(
      InternalRow(1), InternalRow(1), includeStart = true, includeEnd = true)) === (0, 1))
  }

  test("findRowIdRange for isNull filter predicate: empty result") {
    val schema = StructType(StructField("col", IntegerType) :: Nil)
    val path = new Path(Utils.createTempDir().getAbsolutePath, "tempIndexFile")
    val fileWriter = BTreeIndexFileWriter(configuration, path)
    val writer = BTreeIndexRecordWriter(configuration, fileWriter, schema)

    (1 to 300 by 2).map(InternalRow(_)).foreach(writer.write(null, _))
    writer.close(null)

    val reader = BTreeIndexRecordReader(configuration, schema)
    reader.initialize(path, new ArrayBuffer[RangeInterval]())

    assert(
      reader.findRowIdRange(
        RangeInterval(
          IndexScanner.DUMMY_KEY_START,
          IndexScanner.DUMMY_KEY_END,
          includeStart = true,
          includeEnd = true,
          isNull = true)) === (150, 150))
  }

  test("findRowIdRange for isNull filter predicate") {
    val schema = StructType(StructField("col", IntegerType) :: Nil)
    val path = new Path(Utils.createTempDir().getAbsolutePath, "tempIndexFile")
    val fileWriter = BTreeIndexFileWriter(configuration, path)
    val writer = BTreeIndexRecordWriter(configuration, fileWriter, schema)

    (1 to 300 by 2).map(InternalRow(_)).foreach(writer.write(null, _))
    (1 to 5).map(_ => InternalRow(null)).foreach(writer.write(null, _))
    writer.close(null)

    val reader = BTreeIndexRecordReader(configuration, schema)
    reader.initialize(path, new ArrayBuffer[RangeInterval]())

    assert(
      reader.findRowIdRange(
        RangeInterval(
          IndexScanner.DUMMY_KEY_START,
          IndexScanner.DUMMY_KEY_END,
          includeStart = true,
          includeEnd = true,
          isNull = true)) === (150, 155))
  }

  test("findRowIdRange for isNotNull filter predicate: empty result") {
    val schema = StructType(StructField("col", IntegerType) :: Nil)
    val path = new Path(Utils.createTempDir().getAbsolutePath, "tempIndexFile")
    val fileWriter = BTreeIndexFileWriter(configuration, path)
    val writer = BTreeIndexRecordWriter(configuration, fileWriter, schema)

    (1 to 5).map(_ => InternalRow(null)).foreach(writer.write(null, _))
    writer.close(null)

    val reader = BTreeIndexRecordReader(configuration, schema)
    reader.initialize(path, new ArrayBuffer[RangeInterval]())

    assert(
      reader.findRowIdRange(
        RangeInterval(
          IndexScanner.DUMMY_KEY_START,
          IndexScanner.DUMMY_KEY_END,
          includeStart = true,
          includeEnd = true)) === (0, 0))
  }

  test("findRowIdRange for isNotNull filter predicate") {
    val schema = StructType(StructField("col", IntegerType) :: Nil)
    val path = new Path(Utils.createTempDir().getAbsolutePath, "tempIndexFile")
    val fileWriter = BTreeIndexFileWriter(configuration, path)
    val writer = BTreeIndexRecordWriter(configuration, fileWriter, schema)

    (1 to 300 by 2).map(InternalRow(_)).foreach(writer.write(null, _))
    (1 to 5).map(_ => InternalRow(null)).foreach(writer.write(null, _))
    writer.close(null)

    val reader = BTreeIndexRecordReader(configuration, schema)
    reader.initialize(path, new ArrayBuffer[RangeInterval]())

    assert(
      reader.findRowIdRange(
        RangeInterval(
          IndexScanner.DUMMY_KEY_START,
          IndexScanner.DUMMY_KEY_END,
          includeStart = true,
          includeEnd = true)) === (0, 150))
  }
}
