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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.execution.vectorized.{ColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.IntegerType

class ParquetDataFiberHeaderSuite extends SparkFunSuite with SharedOapContext
  with BeforeAndAfterEach with Logging {

  test("construct no dic no nulls") {
    val total = 10
    val column = ColumnVector.allocate(12, IntegerType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    val data = (0 until 10).toArray
    column.putInts(0, total, data, 0)
    val header = ParquetDataFiberHeader(column, total)
    assert(header.noNulls)
    assert(!header.allNulls)
    assert(header.dicLength == 0)

    val fiberCache = OapRuntime.getOrCreate.memoryManager
      .getEmptyDataFiberCache(ParquetDataFiberHeader.defaultSize)
    val address = fiberCache.getBaseOffset
    header.writeToCache(address)
    val headerLoadFromCache = ParquetDataFiberHeader(address)
    assert(headerLoadFromCache.noNulls)
    assert(!headerLoadFromCache.allNulls)
    assert(headerLoadFromCache.dicLength == 0)
  }

  test("construct with dic no nulls") {
    val total = 10
    val column = ColumnVector.allocate(12, IntegerType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    val dictionary = IntegerDictionary(Array(3, 7, 9))
    column.setDictionary(dictionary)
    val dictionaryIds = column.reserveDictionaryIds(total).asInstanceOf[OnHeapColumnVector]

    (0 until 10).foreach(i => {
      dictionaryIds.putInt(i, i % 3)
    })

    val header = ParquetDataFiberHeader(column, total)
    assert(header.noNulls)
    assert(!header.allNulls)
    assert(header.dicLength == 3)

    val fiberCache = OapRuntime.getOrCreate.memoryManager
      .getEmptyDataFiberCache(ParquetDataFiberHeader.defaultSize)
    val address = fiberCache.getBaseOffset
    header.writeToCache(address)
    val headerLoadFromCache = ParquetDataFiberHeader(address)
    assert(headerLoadFromCache.noNulls)
    assert(!headerLoadFromCache.allNulls)
    assert(headerLoadFromCache.dicLength == 3)
  }

  test("construct no dic all nulls") {
    val total = 10
    val column = ColumnVector.allocate(12, IntegerType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    column.putNulls(0, total)
    val header = ParquetDataFiberHeader(column, total)
    assert(!header.noNulls)
    assert(header.allNulls)
    assert(header.dicLength == 0)

    val fiberCache = OapRuntime.getOrCreate.memoryManager
      .getEmptyDataFiberCache(ParquetDataFiberHeader.defaultSize)
    val address = fiberCache.getBaseOffset
    header.writeToCache(address)
    val headerLoadFromCache = ParquetDataFiberHeader(address)
    assert(!headerLoadFromCache.noNulls)
    assert(headerLoadFromCache.allNulls)
    assert(headerLoadFromCache.dicLength == 0)
  }

  test("construct with dic all nulls") {
    val total = 10
    val column = ColumnVector.allocate(12, IntegerType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    val dictionary = IntegerDictionary(Array(3, 7, 9))
    column.setDictionary(dictionary)
    val dictionaryIds = column.reserveDictionaryIds(total).asInstanceOf[OnHeapColumnVector]
    column.putNulls(0, total)

    val header = ParquetDataFiberHeader(column, total)
    assert(!header.noNulls)
    assert(header.allNulls)
    assert(header.dicLength == 3)

    val fiberCache = OapRuntime.getOrCreate.memoryManager
      .getEmptyDataFiberCache(ParquetDataFiberHeader.defaultSize)
    val address = fiberCache.getBaseOffset
    header.writeToCache(address)
    val headerLoadFromCache = ParquetDataFiberHeader(address)
    assert(!headerLoadFromCache.noNulls)
    assert(headerLoadFromCache.allNulls)
    assert(headerLoadFromCache.dicLength == 3)
  }

  test("construct no dic") {
    val total = 10
    val column = ColumnVector.allocate(12, IntegerType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    (0 until 10).foreach(i => {
      if (i % 2 == 0) column.putInt(i, i)
      else column.putNull(i)
    })
    val header = ParquetDataFiberHeader(column, total)
    assert(!header.noNulls)
    assert(!header.allNulls)
    assert(header.dicLength == 0)

    val fiberCache = OapRuntime.getOrCreate.memoryManager
      .getEmptyDataFiberCache(ParquetDataFiberHeader.defaultSize)
    val address = fiberCache.getBaseOffset
    header.writeToCache(address)
    val headerLoadFromCache = ParquetDataFiberHeader(address)
    assert(!headerLoadFromCache.noNulls)
    assert(!headerLoadFromCache.allNulls)
    assert(headerLoadFromCache.dicLength == 0)
  }

  test("construct with dic") {
    val total = 10
    val column = ColumnVector.allocate(12, IntegerType, MemoryMode.ON_HEAP)
      .asInstanceOf[OnHeapColumnVector]
    val dictionary = IntegerDictionary(Array(3, 7, 9))
    column.setDictionary(dictionary)
    val dictionaryIds = column.reserveDictionaryIds(total).asInstanceOf[OnHeapColumnVector]

    (0 until 10).foreach(i => {
      if (i % 5 != 0) dictionaryIds.putInt(i, i % 3)
      else column.putNull(i)
    })

    val header = ParquetDataFiberHeader(column, total)
    assert(!header.noNulls)
    assert(!header.allNulls)
    assert(header.dicLength == 3)

    val fiberCache = OapRuntime.getOrCreate.memoryManager
      .getEmptyDataFiberCache(ParquetDataFiberHeader.defaultSize)
    val address = fiberCache.getBaseOffset
    header.writeToCache(address)
    val headerLoadFromCache = ParquetDataFiberHeader(address)
    assert(!headerLoadFromCache.noNulls)
    assert(!headerLoadFromCache.allNulls)
    assert(headerLoadFromCache.dicLength == 3)
  }
}
