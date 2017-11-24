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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class CacheManagerSuite extends SparkFunSuite {

  val random = new Random(0)
  def generateData(size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    random.nextBytes(bytes)
    bytes
  }

  // TODO: can only run alone since CacheManager is an object.
  ignore("test FiberCacheManager") {

    new SparkContext(
      "local[2]",
      "MemoryManagerSuite",
      new SparkConf().set("spark.memory.offHeap.size", "2k"))

    val configuration = new Configuration()

    val data1 = generateData(256)
    val data2 = generateData(512)
    val data3 = generateData(665)
    val data4 = generateData(1433)
    val data5 = generateData(10240)

    val fiber1 = TestFiber(() => data1, s"test fiber #1, size 256")
    val fiber2 = TestFiber(() => data2, s"test fiber #2, size 512")
    val fiber3 = TestFiber(() => data3, s"test fiber #3, size 665")
    val fiber4 = TestFiber(() => data4, s"test fiber #4, size 7168")
    val fiber5 = TestFiber(() => data5, s"test fiber #5, size 10240")

    // Put 3 fibers into empty cache
    FiberCacheManager.getOrElseUpdate(fiber1, configuration)
    FiberCacheManager.getOrElseUpdate(fiber2, configuration)
    FiberCacheManager.getOrElseUpdate(fiber3, configuration)
    assert(FiberCacheManager.getMissCount == 3)

    // Get 3 fibers from cache
    FiberCacheManager.getOrElseUpdate(fiber1, configuration)
    FiberCacheManager.getOrElseUpdate(fiber2, configuration)
    FiberCacheManager.getOrElseUpdate(fiber3, configuration)
    assert(FiberCacheManager.getHitCount == 3)

    // Put 1 large fiber into cache will cause eviction
    FiberCacheManager.getOrElseUpdate(fiber4, configuration)
    assert(FiberCacheManager.getMissCount == 4)
    // Get large fiber from cache
    FiberCacheManager.getOrElseUpdate(fiber4, configuration)
    assert(FiberCacheManager.getHitCount == 4)

    // Get 3 removed fibers from cache
    FiberCacheManager.getOrElseUpdate(fiber1, configuration)
    FiberCacheManager.getOrElseUpdate(fiber2, configuration)
    FiberCacheManager.getOrElseUpdate(fiber3, configuration)
    assert(FiberCacheManager.getMissCount == 7)

    // Put too large fiber into cache, will not remove other fibers
    FiberCacheManager.getOrElseUpdate(fiber5, configuration)
    assert(FiberCacheManager.getMissCount == 8)
    FiberCacheManager.getOrElseUpdate(fiber2, configuration)
    assert(FiberCacheManager.getMissCount == 8)
  }
}
