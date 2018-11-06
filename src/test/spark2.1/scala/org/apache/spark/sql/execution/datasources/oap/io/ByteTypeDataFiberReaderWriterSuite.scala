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
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, TestFiberCache}
import org.apache.spark.sql.execution.vectorized.{ColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.ByteType

class ByteTypeDataFiberReaderWriterSuite extends DataFiberReaderWriterSuite {

  // byte data use IntegerDictionary
  protected val dictionary: Dictionary = IntegerDictionary(Array(0, 1, 2))

  test("no dic no nulls") {
    // write data
    val column = ColumnVector.allocate(total, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    (0 until total).foreach(i => column.putByte(i, i.toByte))
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, ByteType, total)
    reader.readRowGroupMetas()

    // read use batch api
    val ret1 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => assert(ret1.getByte(i) == (i + start).toByte))

    // read use random access api
    val ret2 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => assert(ret2.getByte(i) == ints(i).toByte))
  }

  test("with dic no nulls") {
    // write data
    val column = ColumnVector.allocate(total, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    column.reserveDictionaryIds(total)
    val dictionaryIds = column.getDictionaryIds.asInstanceOf[OnHeapColumnVector]
    column.setDictionary(dictionary)
    (0 until total).foreach(i => dictionaryIds.putInt(i, i % column.dictionaryLength ))
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, ByteType, total)
    reader.readRowGroupMetas()

    // read use batch api
    val ret1 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i =>
      assert(ret1.getByte(i) == ((i + start) % column.dictionaryLength).toByte))

    // read use random access api
    val ret2 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i =>
      assert(ret2.getByte(i) == (ints(i) % column.dictionaryLength).toByte))
  }

  test("no dic all nulls") {
    // write data
    val column = ColumnVector.allocate(total, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    column.putNulls(0, total)
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, ByteType, total)
    reader.readRowGroupMetas()

    // read use batch api
    val ret1 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => assert(ret1.isNullAt(i)))

    // read use random access api
    val ret2 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => assert(ret2.isNullAt(i)))
  }

  test("with dic all nulls") {
    // write data
    val column = ColumnVector.allocate(total, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    column.reserveDictionaryIds(total)
    column.setDictionary(dictionary)
    column.putNulls(0, total)
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, ByteType, total)
    reader.readRowGroupMetas()

    // read use batch api
    val ret1 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => assert(ret1.isNullAt(i)))

    // read use random access api
    val ret2 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => assert(ret2.isNullAt(i)))
  }

  test("no dic") {
    // write data
    val column = ColumnVector.allocate(total, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    (0 until total).foreach(i => {
      if (i % 3 == 0) column.putNull(i)
      else column.putByte(i, i.toByte)
    })
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, ByteType, total)
    reader.readRowGroupMetas()

    // read use batch api
    val ret1 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => {
      if ((i + start) % 3 == 0) assert(ret1.isNullAt(i))
      else assert(ret1.getByte(i) == (i + start).toByte)
    })

    // read use random access api
    val ret2 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => {
      if ((i + start) % 3 == 0) assert(ret2.isNullAt(i))
      else assert(ret2.getByte(i) == ints(i).toByte)
    })
  }

  test("with dic") {
    // write data
    val column = ColumnVector.allocate(total, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    column.reserveDictionaryIds(total)
    val dictionaryIds = column.getDictionaryIds.asInstanceOf[OnHeapColumnVector]
    column.setDictionary(dictionary)
    (0 until total).foreach(i => {
      if (i % 3 == 0) column.putNull(i)
      else dictionaryIds.putInt(i, i % column.dictionaryLength)
    })
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, ByteType, total)
    reader.readRowGroupMetas()

    // read use batch api
    val ret1 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => {
      if ((i + start) % 3 == 0) assert(ret1.isNullAt(i))
      else assert(ret1.getByte(i) == ((i + start) % column.dictionaryLength).toByte)
    })

    // read use random access api
    val ret2 = ColumnVector.allocate(num, ByteType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => {
      if ((i + start) % 3 == 0) assert(ret2.isNullAt(i))
      else assert(ret2.getByte(i) == (ints(i) % column.dictionaryLength).toByte)
    })
  }
}
