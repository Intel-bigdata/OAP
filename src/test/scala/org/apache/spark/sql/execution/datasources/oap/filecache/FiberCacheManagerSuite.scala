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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

import scala.util.Random

class FiberCacheManagerSuite extends SparkFunSuite {
  private val random = new Random(0)
  private def generateData(size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    random.nextBytes(bytes)
    bytes
  }

  // scalastyle:off println
  test("unit test") {
    new SparkContext(
      "local[2]",
      "FiberCacheManagerSuite",
      new SparkConf().set("spark.memory.offHeap.size", "100m")
    )
    val configuration = new Configuration()
    val MB: Double = 1024 * 1024
    val memorySizeInMB = (MemoryManager.maxMemory / MB).toInt
    val origStats = FiberCacheManager.getStats
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(1024)
      val fiber = TestFiber(() => data, s"test fiber #$i")
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      val fiberCache2 = FiberCacheManager.get(fiber, configuration)
      assert(fiberCache.toArray sameElements data)
      assert(fiberCache2.toArray sameElements data)
    }
    val stats = FiberCacheManager.getStats.minus(origStats)
    assert(stats.missCount() == memorySizeInMB * 2)
    assert(stats.hitCount() == memorySizeInMB * 2)
    assert(stats.evictionCount() >= memorySizeInMB)
  }
  test("remove a fiber is in use") {
    new SparkContext(
      "local[2]",
      "FiberCacheManagerSuite",
      new SparkConf().set("spark.memory.offHeap.size", "100m")
    )
    val configuration = new Configuration()
    val MB: Double = 1024 * 1024
    val memorySizeInMB = (MemoryManager.maxMemory / MB).toInt

    val origStats = FiberCacheManager.getStats
    val dataInUse = generateData(1024)
    val fiberInUse = TestFiber(() => dataInUse, s"test fiber #0")
    val fiberCacheInUse = FiberCacheManager.get(fiberInUse, configuration)
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(1024)
      val fiber = TestFiber(() => data, s"test fiber #$i")
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      assert(fiberCache.toArray sameElements data)
    }
    val stats = FiberCacheManager.getStats.minus(origStats)
    println(stats.evictionCount())
    assert(fiberCacheInUse.isDisposed)
  }
}
