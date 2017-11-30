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

import java.io.ByteArrayInputStream

import scala.util.Random
import org.apache.spark.sql.execution.datasources.oap.OapEnv
import org.apache.spark.sql.test.SharedSQLContext

class FiberCacheManagerSuite extends SharedSQLContext {
  private val random = new Random(0)
  private def generateData(size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    random.nextBytes(bytes)
    bytes
  }
  sparkConf.set("spark.memory.offHeap.size", "100m")

  private def memoryManager = OapEnv.get.memoryManager
  private def fiberCacheManager = OapEnv.get.fiberCacheManager
  private def toInputStream(data: Array[Byte]): FiberInputStream = {
    FiberInputStream(new ByteArrayInputStream(data), 0, data.length)
  }
  private val MB: Double = 1024 * 1024
  private def maxMemoryInMB = (memoryManager.maxMemory / MB).toInt

  test("unit test") {
    val MB: Double = 1024 * 1024
    val memorySizeInMB = (memoryManager.maxMemory / MB).toInt
    val origStats = fiberCacheManager.getStats
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(1024)
      val fiber =
        TestFiber(s"test fiber #$i")
      val fiberCache = fiberCacheManager.get(fiber, toInputStream(data))
      val fiberCache2 = fiberCacheManager.get(fiber, toInputStream(data))
      assert(fiberCache.toArray sameElements data)
      assert(fiberCache2.toArray sameElements data)
      fiberCache.release()
      fiberCache2.release()
    }
    val stats = fiberCacheManager.getStats.minus(origStats)
    assert(stats.missCount() == memorySizeInMB * 2)
    assert(stats.hitCount() == memorySizeInMB * 2)
    assert(stats.evictionCount() >= memorySizeInMB)
  }

  test("remove a fiber is in use") {
    val MB: Double = 1024 * 1024
    val memorySizeInMB = (memoryManager.maxMemory / MB).toInt

    val dataInUse = generateData(1024)
    val fiberInUse =
      TestFiber(s"test fiber #0")
    val fiberCacheInUse = fiberCacheManager.get(fiberInUse, toInputStream(dataInUse))
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(1024)
      val fiber = TestFiber(s"test fiber #$i")
      val fiberCache = fiberCacheManager.get(fiber, toInputStream(data))
      assert(fiberCache.toArray sameElements data)
      fiberCache.release()
    }
    assert(!fiberCacheInUse.isDisposed)
    fiberCacheInUse.release()
  }

  test("add a very large fiber") {
    val MB: Double = 1024 * 1024
    val memorySizeInMB = (memoryManager.maxMemory / MB).toInt
    // Cache concurrency is 4, means maximum ENTRY size is memory size / 4
    val data = generateData(memorySizeInMB * 1024 * 1024 / 8)
    val fiber =
      TestFiber(s"test fiber #0")
    val fiberCache = fiberCacheManager.get(fiber, toInputStream(data))
    fiberCache.release()
    assert(!fiberCache.isDisposed)

    val data1 = generateData(memorySizeInMB * 1024 * 1024 / 2)
    val fiber1 =
      TestFiber(s"test fiber #1")
    val fiberCache1 = fiberCacheManager.get(fiber1, toInputStream(data1))
    assert(fiberCache1 == null)
  }

  test("lock") {
    // 1. in-used fiberCache can't be freed until the read lock is released.
    // 2. current thread causes an eviction, and evicted fiberCache's read lock is hold by itself.
    // 3. if fiberCache can't put into Cache Manager, caller should know.
    // 4. Cache Manager can't store caches exceed the max memory.
    //     cache is waiting on freeing memory should be counted or not?
    //     no enough memory for cache will trigger cache eviction.
    //     cache should be put failed if eviction can't free enough memory.
    // put fiberCache #1, #2, #3, #1 is using, #3 will cause eviction.
    // expect result, #1 should be disposed, no dead lock.
    val originalConfig1 = sparkContext.conf.get(
      MemoryManager.OAP_OFF_HEAP_MEMORY_FRACTION,
      MemoryManager.OAP_OFF_HEAP_MEMORY_FRACTION_DEFAULT.toString)

    val originalConfig2 = sparkContext.conf.get(
      FiberCacheManager.OAP_FIBER_CACHE_CONCURRENCY,
      FiberCacheManager.OAP_FIBER_CACHE_CONCURRENCY_DEFAULT.toString)

    sparkContext.conf.set(FiberCacheManager.OAP_FIBER_CACHE_CONCURRENCY, "1")
    sparkContext.conf.set(MemoryManager.OAP_OFF_HEAP_MEMORY_FRACTION, "0.07")
    val data2m = generateData(2 * MB.toInt)
    val data4m = generateData(4 * MB.toInt)
    val data8m = generateData(8 * MB.toInt)

    val fiber1 = TestFiber("test fiber #1")
    val fiber2 = TestFiber("test fiber #2")
    val fiber3 = TestFiber("test fiber #3")
    val fiber4 = TestFiber("test fiber #4")

    OapEnv.restart()
    assert(maxMemoryInMB == 7)
    // =========== TEST #1 ==============
    val fiberCache1 = fiberCacheManager.get(fiber1, toInputStream(data2m))
    val fiberCache2 = fiberCacheManager.get(fiber2, toInputStream(data2m))
    fiberCache2.release()
    val fiberCache3 = fiberCacheManager.get(fiber3, toInputStream(data4m))

    // lock hold, should not be freed
    assert(!fiberCache1.isDisposed)
    // lock hold, eviction should happen, should be freed
    assert(fiberCache2.isDisposed)
    // new fiberCache, should cause eviction, should be put into cache after eviction
    assert(fiberCache3 != null && !fiberCache3.isDisposed)

    OapEnv.restart()
    // ============== TEST #2 ============
    val fiberCache21 = fiberCacheManager.get(fiber1, toInputStream(data2m))
    val fiberCache22 = fiberCacheManager.get(fiber2, toInputStream(data2m))
    val fiberCache23 = fiberCacheManager.get(fiber3, toInputStream(data4m))
    assert(fiberCache23 == null)

    OapEnv.restart()
    // ============== TEST #2 ============
    val fiberCache31 = fiberCacheManager.get(fiber4, toInputStream(data8m))
    assert(fiberCache31 == null)

    // restore back. TODO: use a function to update/restore config
    sparkContext.conf.set(MemoryManager.OAP_OFF_HEAP_MEMORY_FRACTION, originalConfig1)
    sparkContext.conf.set(FiberCacheManager.OAP_FIBER_CACHE_CONCURRENCY, originalConfig2)
    OapEnv.restart()
  }
}
