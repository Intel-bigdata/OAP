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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class BTreeIndexScannerSuite extends SparkFunSuite {

  test("test rowOrdering") {
    // Only check Integer is enough. We use [[GenerateOrdering]] to handle different data types.
    val fields = StructField("col1", IntegerType) :: StructField("col2", IntegerType) :: Nil
    val singleColumnReader = BTreeIndexRecordReader(new Configuration(), StructType(fields.take(1)))
    val multiColumnReader = BTreeIndexRecordReader(new Configuration(), StructType(fields))
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
    val reader = BTreeIndexRecordReader(new Configuration(), schema)
    val values = Seq(1, 11, 21, 31, 41, 51, 61, 71, 81, 91)
    def keyAt(idx: Int): InternalRow = InternalRow(values(idx))
    val ordering = GenerateOrdering.create(schema)

    assert(
      reader.binarySearch(0, values.size, keyAt, InternalRow(10), ordering.compare) === (1, false))
  }
}
