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

import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class FiberCacheManagerSuite extends SharedOapContext {

  private val kbSize = 1024
  private val mbSize = kbSize * kbSize

  private def generateData(size: Int): Array[Byte] =
    Utils.randomizeInPlace(new Array[Byte](size))

  private var fiberGroupId: Int = 0

  // Each test calls this to create a new fiber group Id.
  // To avoid cache hit by mistake.
  private def newFiberGroup = {
    fiberGroupId += 1
    fiberGroupId
  }

  test("unit test") {
    val memorySizeInMB = (MemoryManager.cacheMemory / mbSize).toInt
    val origStats = FiberCacheManager.cacheStats
    newFiberGroup
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(mbSize)
      val fiber =
        TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber #$fiberGroupId.$i")
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      val fiberCache2 = FiberCacheManager.get(fiber, configuration)
      assert(fiberCache.toArray sameElements data)
      assert(fiberCache2.toArray sameElements data)
      fiberCache.release()
      fiberCache2.release()
    }
    val stats = FiberCacheManager.cacheStats.minus(origStats)
    assert(stats.missCount == memorySizeInMB * 2)
    assert(stats.hitCount == memorySizeInMB * 2)
    assert(stats.evictionCount >= memorySizeInMB)
  }

  test("remove a fiber is in use") {
    val memorySizeInMB = (MemoryManager.cacheMemory / mbSize).toInt
    val dataInUse = generateData(mbSize)
    val fiberInUse = TestFiber(
        () => MemoryManager.putToDataFiberCache(dataInUse), s"test fiber #${newFiberGroup}.0")
    val fiberCacheInUse = FiberCacheManager.get(fiberInUse, configuration)
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(mbSize)
      val fiber =
        TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber #$fiberGroupId.$i")
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      assert(fiberCache.toArray sameElements data)
      fiberCache.release()
    }
    assert(fiberCacheInUse.toArray sameElements dataInUse)
    fiberCacheInUse.release()
  }

  test("wait for other thread release the fiber") {
    newFiberGroup
    class FiberTestRunner(i: Int) extends Thread {
      override def run(): Unit = {
        val memorySizeInMB = (MemoryManager.cacheMemory / mbSize).toInt
        val data = generateData(memorySizeInMB / 4 * mbSize)
        val fiber = TestFiber(
          () => MemoryManager.putToDataFiberCache(data), s"test fiber #$fiberGroupId.$i")
        val fiberCache = FiberCacheManager.get(fiber, configuration)
        Thread.sleep(2000)
        fiberCache.release()
      }
    }
    val threads = (0 until 5).map(i => new FiberTestRunner(i))
    threads.foreach(_.start())
    threads.foreach(_.join(10000))
    threads.foreach(t => assert(!t.isAlive))
  }

  test("add a very large fiber") {
    val memorySizeInMB = (MemoryManager.cacheMemory / mbSize).toInt
    val ASSERT_MESSAGE_REGEX =
      ("""assertion failed: Failed to cache fiber\(\d+\.\d [TGMK]?B\) """ +
        """with cache's MAX_WEIGHT\(\d+\.\d [TGMK]?B\) / 4""").r
    val exception = intercept[AssertionError] {
      val data = generateData(memorySizeInMB * mbSize / 2)
      val fiber = TestFiber(
        () => MemoryManager.putToDataFiberCache(data), s"test fiber #${newFiberGroup}.1")
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      fiberCache.release()
    }

    exception.getMessage match {
      case ASSERT_MESSAGE_REGEX() =>
      case msg => assert(false, msg + " Not Match " + ASSERT_MESSAGE_REGEX.toString())
    }
  }

  test("fiber key equality test") {
    newFiberGroup
    val data = generateData(kbSize)
    val origStats = FiberCacheManager.cacheStats
    val fiber = TestFiber(
      () => MemoryManager.putToDataFiberCache(data), s"test fiber #$fiberGroupId.0")
    val fiberCache1 = FiberCacheManager.get(fiber, configuration)
    assert(FiberCacheManager.cacheStats.minus(origStats).missCount == 1)
    val sameFiber = TestFiber(
      () => MemoryManager.putToDataFiberCache(data), s"test fiber #$fiberGroupId.0")
    val fiberCache2 = FiberCacheManager.get(sameFiber, configuration)
    assert(FiberCacheManager.cacheStats.minus(origStats).hitCount == 1)
    fiberCache1.release()
    fiberCache2.release()
  }

  test("cache guardian remove pending fibers") {
    newFiberGroup
    Thread.sleep(1000) // Wait some time for CacheGuardian to remove pending fibers
    val memorySizeInMB = (MemoryManager.cacheMemory / mbSize).toInt
    val fibers = (1 to memorySizeInMB * 2).map { i =>
      val data = generateData(mbSize)
      TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber #$fiberGroupId.$i")
    }
    // release fibers so it has chance to be disposed immediately
    fibers.foreach(FiberCacheManager.get(_, configuration).release())
    Thread.sleep(1000)
    assert(FiberCacheManager.pendingCount == 0)
    // Hold the fiber, so it can't be disposed until release
    val fiberCaches = fibers.map(FiberCacheManager.get(_, configuration))
    Thread.sleep(1000)
    assert(FiberCacheManager.pendingCount > 0)
    // After release, CacheGuardian should be back to work
    fiberCaches.foreach(_.release())
    // Wait some time for CacheGuardian being waken-up
    Thread.sleep(1000)
    assert(FiberCacheManager.pendingCount == 0)
  }

  class TestRunner(work: () => Unit) extends Runnable {
    override def run(): Unit = work()
  }

  class TestCaller(work: () => Boolean) extends Callable[Boolean] {
    override def call(): Boolean = work()
  }

  // Fiber should only load once
  test("get same fiber simultaneously") {
    val data = generateData(kbSize)
    var loadTimes = 0
    val fiber = TestFiber(() => {
      loadTimes += 1
      MemoryManager.putToDataFiberCache(data)
    }, s"same fiber test")
    def work(): Unit = {
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      Thread.sleep(100)
      fiberCache.release()
    }
    val runner = new TestRunner(work)
    val pool = Executors.newCachedThreadPool
    (1 to 10).foreach(_ => pool.execute(runner))
    pool.shutdown()
    pool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    assert(loadTimes == 1)
  }

  // request fibers exceed max memory at the same time
  test("get different fiber simultaneously") {
    val memorySizeInMB = (MemoryManager.cacheMemory / mbSize).toInt
    val pool = Executors.newCachedThreadPool()
    val runners = (1 to 6).map { i =>
      val data = generateData(memorySizeInMB / 5 * mbSize)
      val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"different test $i")
      def work(): Boolean = {
        val fiberCache = FiberCacheManager.get(fiber, configuration)
        val flag = fiberCache.toArray sameElements data
        Thread.sleep(100)
        fiberCache.release()
        flag
      }
      new TestCaller(work)
    }
    val results = runners.map(t => pool.submit(t))
    pool.shutdown()
    pool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    Thread.sleep(100)
    results.foreach(r => r.get())
    assert(FiberCacheManager.pendingCount == 0)
  }

  // refCount should be correct
  test("release same fiber simultaneously") {
    val pool = Executors.newCachedThreadPool()
    val data = generateData(kbSize)
    val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"release test")
    val fiberCaches = (1 to 5).map(_ => FiberCacheManager.get(fiber, configuration))
    assert(fiberCaches.head.refCount == 5)
    fiberCaches.foreach { fiberCache =>
      pool.execute(new TestRunner(() => fiberCache.release()))
    }
    pool.shutdown()
    pool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    assert(fiberCaches.head.refCount == 0)
  }

  // refCount should be correct, and fiber can be disposed after get
  test("get and release fiber simultaneously") {
    val pool = Executors.newCachedThreadPool()
    val data = generateData(kbSize)
    val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"get release test")
    def work(): Boolean = {
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      val flag = fiberCache.refCount > 0 || !fiberCache.isDisposed
      fiberCache.release()
      flag
    }
    val results = (1 to 10).map(_ => pool.submit(new TestCaller(work)))
    pool.shutdown()
    pool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    results.foreach(r => assert(r.get()))
  }

  // fiber must not be removed during get
  test("get and remove fiber simultaneously") {
    val pool = Executors.newCachedThreadPool()
    val data = generateData(kbSize)
    val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"get remove test")
    def occupyWork(): Boolean = {
      (1 to 100).foreach { _ =>
        val fiberCache = FiberCacheManager.get(fiber, configuration)
        if (fiberCache.isDisposed) {
          fiberCache.release()
          return false
        }
        fiberCache.release()
      }
      true
    }
    def removeWork(): Unit = {
      (1 to 100000).foreach { _ =>
        FiberCacheManager.removeFiber(fiber)
      }
    }
    val result = pool.submit(new TestCaller(occupyWork))
    pool.execute(new TestRunner(removeWork))
    pool.shutdown()
    pool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    assert(result.get())
  }

  test("test Simple Cache Strategy") {
    val cache = new SimpleOapCache()
    val data = generateData(10 * kbSize)
    val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), "test simple cache fiber")
    val fiberCache = cache.get(fiber, configuration)
    assert(fiberCache.toArray sameElements data)
    fiberCache.release()
    Thread.sleep(500)
    assert(fiberCache.isDisposed)
  }

  test("LRU blocks memory free") {
    val memorySizeInMB = (MemoryManager.cacheMemory / mbSize).toInt
    val dataInUse = generateData(mbSize)
    val fiberInUse = TestFiber(
      () => MemoryManager.putToDataFiberCache(dataInUse), s"test fiber #${newFiberGroup}.0")

    // Put into cache and make it use
    val fiberCacheInUse = FiberCacheManager.get(fiberInUse, configuration)
    assert(FiberCacheManager.pendingCount == 0)

    // make fiber in use the 1st element in release queue.
    FiberCacheManager.removeFiber(fiberInUse)

    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(mbSize)
      val fiber = TestFiber(
        () => MemoryManager.putToDataFiberCache(data), s"test fiber #$fiberGroupId.$i")
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      assert(fiberCache.toArray sameElements data)
      fiberCache.release()
    }

    // Wait for clean.
    Thread.sleep(6000)
    // There should be only one in-use fiber.
    assert(FiberCacheManager.pendingCount == 1)
    fiberCacheInUse.release()
    Thread.sleep(6000)
    assert(FiberCacheManager.pendingCount == 0)
  }
}
