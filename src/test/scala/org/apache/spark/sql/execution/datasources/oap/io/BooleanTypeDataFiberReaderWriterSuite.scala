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

package org.apache.spark.sql.execution.datasources.oap.io

import org.apache.parquet.column.Dictionary

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.execution.vectorized.{ColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.BooleanType

/**
 * Boolean Type not support dic encode.
 */
class BooleanTypeDataFiberReaderWriterSuite extends DataFiberReaderWriterSuite {

  protected def dictionary: Dictionary =
    throw new UnsupportedOperationException("Boolean Type not support dic encode")

  test("no dic no nulls") {
    val column = ColumnVector.allocate(total, BooleanType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    (0 until total).foreach(i => column.putBoolean(i, i % 2 == 0))
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    val address = fiberCache.getBaseOffset

    // read use batch api
    val ret1 = ColumnVector.allocate(num, BooleanType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    val reader = ParquetDataFiberReader(address, BooleanType, total)
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => assert(ret1.getBoolean(i) == (((i + start) % 2) == 0)))

    // read use random access api
    val ret2 = ColumnVector.allocate(num, BooleanType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => assert(ret2.getBoolean(i) == ((ints(i) % 2) == 0)))
  }

  test("no dic all nulls") {
    val column = ColumnVector.allocate(total, BooleanType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    column.putNulls(0, total)
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    val address = fiberCache.getBaseOffset

    // read use batch api
    val ret1 = ColumnVector.allocate(num, BooleanType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    val reader = ParquetDataFiberReader(address, BooleanType, total)
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => assert(ret1.isNullAt(i)))

    // read use random access api
    val ret2 = ColumnVector.allocate(num, BooleanType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => assert(ret2.isNullAt(i)))
  }

  test("no dic") {
    val column = ColumnVector.allocate(total, BooleanType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    (0 until total).foreach(i => {
      if (i % 3 == 0) column.putNull(i)
      else column.putBoolean(i, i % 2 == 0)
    })
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    val address = fiberCache.getBaseOffset

    // read use batch api
    val ret1 = ColumnVector.allocate(num, BooleanType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    val reader = ParquetDataFiberReader(address, BooleanType, total)
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => {
      if ((i + start) % 3 == 0) assert(ret1.isNullAt(i))
      else assert(ret1.getBoolean(i) == (((i + start) % 2) == 0))
    })

    // read use random access api
    val ret2 = ColumnVector.allocate(num, BooleanType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => {
      if ((i + start) % 3 == 0) assert(ret2.isNullAt(i))
      else assert(ret2.getBoolean(i) == ((ints(i) % 2) == 0))
    })
  }
}
