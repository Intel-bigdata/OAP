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

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class FiberCacheManagerSuite extends SharedOapContext {

  private val kbSize = 1024
  private val mbSize = kbSize * kbSize

  private def generateData(size: Int): Array[Byte] =
    Utils.randomizeInPlace(new Array[Byte](size))


  test("unit test") {
    val memorySizeInMB = (MemoryManager.maxMemory / mbSize).toInt
    val origStats = FiberCacheManager.cacheStats
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(kbSize)
      val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber #0.$i")
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      val fiberCache2 = FiberCacheManager.get(fiber, configuration)
      assert(fiberCache.toArray sameElements data)
      assert(fiberCache2.toArray sameElements data)
      fiberCache.release()
      fiberCache2.release()
    }
    val stats = FiberCacheManager.cacheStats.minus(origStats)
    assert(stats.missCount() == memorySizeInMB * 2)
    assert(stats.hitCount() == memorySizeInMB * 2)
    assert(stats.evictionCount() >= memorySizeInMB)
  }

  test("remove a fiber is in use") {
    val memorySizeInMB = (MemoryManager.maxMemory / mbSize).toInt
    val dataInUse = generateData(kbSize)
    val fiberInUse =
      TestFiber(() => MemoryManager.putToDataFiberCache(dataInUse), s"test fiber #1.0")
    val fiberCacheInUse = FiberCacheManager.get(fiberInUse, configuration)
    val exception = intercept[OapException] {
      (1 to memorySizeInMB * 2).foreach { i =>
        val data = generateData(1024)
        val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber #1.$i")
        val fiberCache = FiberCacheManager.get(fiber, configuration)
        assert(fiberCache.toArray sameElements data)
        fiberCache.release()
      }
    }
    assert(exception.getMessage == "Can't remove in-used fiber within 3 seconds")
    // Set this flag to false for following test
    FiberCacheManager.exceptionFlag = false
    fiberCacheInUse.release()
  }

  test("wait for other thread release the fiber") {
    class FiberTestRunner(i: Int) extends Thread {
      override def run(): Unit = {
        val memorySizeInMB = (MemoryManager.maxMemory / mbSize).toInt
        val data = generateData(memorySizeInMB / 4 * mbSize)
        val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber #2.$i")
        val fiberCache = FiberCacheManager.get(fiber, configuration)
        Thread.sleep(2000)
        fiberCache.release()
      }
    }
    val threads = (0 until 5).map(i => new FiberTestRunner(i))
    threads.foreach(_.start())
    threads.foreach(_.join())
  }

  test("add a very large fiber") {
    val memorySizeInMB = (MemoryManager.maxMemory / mbSize).toInt
    // Cache concurrency is 4, means maximum ENTRY size is memory size / 4
    val data = generateData(memorySizeInMB * mbSize / 8)
    val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber #3.0")
    val fiberCache = FiberCacheManager.get(fiber, configuration)
    fiberCache.release()
    assert(!fiberCache.isDisposed)

    val data1 = generateData(memorySizeInMB * mbSize / 2)
    val fiber1 = TestFiber(() => MemoryManager.putToDataFiberCache(data1), s"test fiber #3.1")
    val fiberCache1 = FiberCacheManager.get(fiber1, configuration)
    fiberCache1.release()
    assert(fiberCache1.isDisposed)
  }

  test("fiber key equality test") {
    val data = generateData(kbSize)
    val origStats = FiberCacheManager.cacheStats
    val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber")
    val fiberCache1 = FiberCacheManager.get(fiber, configuration)
    assert(FiberCacheManager.cacheStats.minus(origStats).missCount() == 1)
    val sameFiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber")
    val fiberCache2 = FiberCacheManager.get(sameFiber, configuration)
    assert(FiberCacheManager.cacheStats.minus(origStats).hitCount() == 1)
    fiberCache1.release()
    fiberCache2.release()
  }
}
