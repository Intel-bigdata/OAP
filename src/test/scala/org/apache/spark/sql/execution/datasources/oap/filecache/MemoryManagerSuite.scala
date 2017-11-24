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

package org.apache.spark.sql.execution.datasources.oap.filecache

import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class MemoryManagerSuite extends SparkFunSuite {

  private val random = new Random(0)
  def generateData(size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    random.nextBytes(bytes)
    bytes
  }

  // TODO: can only run alone since MemoryManager is an object.
  ignore("allocate/free off-heap memory") {
    new SparkContext(
      "local[2]",
      "MemoryManagerSuite",
      new SparkConf().set("spark.memory.offHeap.size", "2k"))

    assert(MemoryManager.memoryUsed === 0)
    // Check data in off-heap
    val dataSmall = generateData(1024)
    val fiberCacheSmall = MemoryManager.putToFiberCache(dataSmall)
    assert(fiberCacheSmall.isOffHeap)
    assert(MemoryManager.memoryUsed === 1024)
    assert(fiberCacheSmall.toArray sameElements dataSmall)
    fiberCacheSmall.dispose()

    // Check data in on-heap
    val dataLarge = generateData(2048)
    val fiberCacheLarge = MemoryManager.putToFiberCache(dataLarge)
    assert(MemoryManager.memoryUsed === 0)
    assert(!fiberCacheLarge.isOffHeap)
    assert(fiberCacheLarge.toArray sameElements dataLarge)
    fiberCacheLarge.dispose()
  }
}
