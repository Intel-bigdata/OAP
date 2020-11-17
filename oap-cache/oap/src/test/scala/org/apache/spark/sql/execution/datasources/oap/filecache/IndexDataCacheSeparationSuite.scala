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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class IndexDataCacheSeparationSuite extends SharedOapContext with BeforeAndAfterEach{

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

  oapSparkConf.set("spark.sql.oap.index.data.cache.separation.enabled", "true")
  oapSparkConf.set("spark.oap.cache.strategy", "mix")
  oapSparkConf.set("spark.sql.oap.mix.data.memory.manager", "offheap")

  private def fiberCacheManager = OapRuntime.getOrCreate.fiberCacheManager

  private def dataCacheMemorySize = fiberCacheManager.dataCacheMemorySize
  private def indexCacheMemorySize = fiberCacheManager.indexCacheMemorySize

  override def afterEach(): Unit = {
    fiberCacheManager.clearAllFibers()
  }

  override def afterAll(): Unit = {
    // restore oapSparkConf to default
    oapSparkConf.set("spark.oap.cache.strategy", "guava")
    oapSparkConf.set("spark.sql.oap.cache.memory.manager", "offheap")
    oapSparkConf.set("spark.sql.oap.index.data.cache.separation.enabled", "false")
    oapSparkConf.set("spark.sql.oap.mix.data.memory.manager", "pm")
  }

  test("unit test") {
    val dataMemorySizeInMB = (dataCacheMemorySize / mbSize).toInt
    val origStats = fiberCacheManager.cacheStats
    newFiberGroup
    (1 to dataMemorySizeInMB * 2).foreach { i =>
      val data = generateData(mbSize)
      val dataFiber =
        TestDataFiberId(
          () => fiberCacheManager.toDataFiberCache(data),
          s"test data fiber #$fiberGroupId.$i")
      val dataFiberCache = fiberCacheManager.get(dataFiber)
      val dataFiberCache2 = fiberCacheManager.get(dataFiber)
      assert(dataFiberCache.toArray sameElements data)
      assert(dataFiberCache2.toArray sameElements data)
      dataFiberCache.release()
      dataFiberCache2.release()
    }

    val indexMemorySizeInMB = (indexCacheMemorySize / mbSize).toInt
    (1 to indexMemorySizeInMB * 2).foreach { i =>
      val data = generateData(mbSize)
      val indexFiber =
        TestIndexFiberId(
          () => fiberCacheManager.toDataFiberCache(data),
          s"test index fiber #$fiberGroupId.$i")
      val indexFiberCache = fiberCacheManager.get(indexFiber)
      val indexFiberCache2 = fiberCacheManager.get(indexFiber)
      assert(indexFiberCache.toArray sameElements data)
      assert(indexFiberCache2.toArray sameElements data)
      indexFiberCache.release()
      indexFiberCache2.release()
    }

    val stats = fiberCacheManager.cacheStats.minus(origStats)
    assert(stats.dataFiberMissCount == dataMemorySizeInMB * 2)
    assert(stats.dataFiberHitCount == dataMemorySizeInMB * 2)
    assert(stats.dataEvictionCount >= dataMemorySizeInMB)
    assert(stats.indexFiberMissCount == indexMemorySizeInMB * 2)
    assert(stats.indexFiberHitCount == indexMemorySizeInMB * 2)
    assert(stats.indexEvictionCount >= indexMemorySizeInMB)
    Thread.sleep(1000)
  }

  test("remove a fiber is in use") {
    val dataMemorySizeInMB = (dataCacheMemorySize / mbSize).toInt
    val dataInUse = generateData(mbSize)
    val dataFiberInUse = TestDataFiberId(
        () => fiberCacheManager.toDataFiberCache(dataInUse),
        s"test data fiber #${newFiberGroup}.0")
    val dataFiberCacheInUse = fiberCacheManager.get(dataFiberInUse)
    (1 to dataMemorySizeInMB * 2).foreach { i =>
      val data = generateData(mbSize)
      val fiber = TestDataFiberId(
        () => fiberCacheManager.toDataFiberCache(data),
        s"test data fiber #$fiberGroupId.$i")
      val fiberCache = fiberCacheManager.get(fiber)
      assert(fiberCache.toArray sameElements data)
      fiberCache.release()
    }
    assert(dataFiberCacheInUse.toArray sameElements dataInUse)
    dataFiberCacheInUse.release()

    val indexMemorySizeInMB = (indexCacheMemorySize / mbSize).toInt
    val indexFiberInUse = TestIndexFiberId(
      () => fiberCacheManager.toDataFiberCache(dataInUse),
      s"test index fiber #${newFiberGroup}.0")
    val indexFiberCacheInUse = fiberCacheManager.get(indexFiberInUse)
    (1 to indexMemorySizeInMB * 2).foreach { i =>
      val data = generateData(mbSize)
      val fiber = TestIndexFiberId(
        () => fiberCacheManager.toDataFiberCache(data),
        s"test index fiber #$fiberGroupId.$i")
      val fiberCache = fiberCacheManager.get(fiber)
      assert(fiberCache.toArray sameElements data)
      fiberCache.release()
    }
    assert(indexFiberCacheInUse.toArray sameElements dataInUse)
    indexFiberCacheInUse.release()
    Thread.sleep(1000)
  }

  test("wait for other thread release the fiber") {
    newFiberGroup
    class DataFiberTestRunner(i: Int) extends Thread {
      override def run(): Unit = {
        val dataMemorySizeInMB = (dataCacheMemorySize / mbSize).toInt
        val data = generateData(dataMemorySizeInMB / 8 * mbSize)
        val fiber = TestDataFiberId(
          () => fiberCacheManager.toDataFiberCache(data),
          s"test data fiber #$fiberGroupId.$i")
        val fiberCache = fiberCacheManager.get(fiber)
        Thread.sleep(2000)
        fiberCache.release()
      }
    }

    class IndexFiberTestRunner(i: Int) extends Thread {
      override def run(): Unit = {
        val indexMemorySizeInMB = (indexCacheMemorySize / mbSize).toInt
        val data = generateData(indexMemorySizeInMB / 4 * mbSize)
        val fiber = TestIndexFiberId(
          () => fiberCacheManager.toDataFiberCache(data),
          s"test index fiber #$fiberGroupId.$i")
        val fiberCache = fiberCacheManager.get(fiber)
        Thread.sleep(2000)
        fiberCache.release()
      }
    }

    val dataThreads = (0 until 5).map(i => new DataFiberTestRunner(i))
    val indexThreads = (0 until 5).map(i => new IndexFiberTestRunner(i))
    dataThreads.foreach(_.start())
    indexThreads.foreach(_.start())
    dataThreads.foreach(_.join(10000))
    indexThreads.foreach(_.join(10000))
    Thread.sleep(1000)
    dataThreads.foreach(t => assert(!t.isAlive))
    indexThreads.foreach(t => assert(!t.isAlive))
  }

  test("add a very large fiber") {
    val ASSERT_MESSAGE_REGEX =
      ("""assertion failed: Failed to cache fiber\(\d+\.\d [TGMK]?iB\) """ +
        """with cache's MAX_WEIGHT\(\d+\.\d [TGMK]?iB\) / 4""").r
    val dataMemorySizeInMB = (dataCacheMemorySize / mbSize).toInt
    val dataException = intercept[AssertionError] {
      val data = generateData(dataMemorySizeInMB * mbSize / 2)
      val fiber = TestDataFiberId(
        () => fiberCacheManager.toDataFiberCache(data),
        s"test fiber #${newFiberGroup}.1")
      val fiberCache = fiberCacheManager.get(fiber)
      fiberCache.release()
    }

    dataException.getMessage match {
      case ASSERT_MESSAGE_REGEX() =>
      case msg => assert(false, msg + " Not Match " + ASSERT_MESSAGE_REGEX.toString())
    }

    val indexMemorySizeInMB = (indexCacheMemorySize / mbSize).toInt
    val indexException = intercept[AssertionError] {
      val data = generateData(indexMemorySizeInMB * mbSize / 2)
      val fiber = TestIndexFiberId(
        () => fiberCacheManager.toDataFiberCache(data),
        s"test fiber #${newFiberGroup}.1")
      val fiberCache = fiberCacheManager.get(fiber)
      fiberCache.release()
    }

    indexException.getMessage match {
      case ASSERT_MESSAGE_REGEX() =>
      case msg => assert(false, msg + " Not Match " + ASSERT_MESSAGE_REGEX.toString())
    }
  }

  test("fiber key equality test") {
    newFiberGroup
    val data = generateData(kbSize)
    val origStats = fiberCacheManager.cacheStats
    val dataFiber = TestDataFiberId(
      () => fiberCacheManager.toDataFiberCache(data),
      s"test data fiber #$fiberGroupId.0")
    val dataFiberCache1 = fiberCacheManager.get(dataFiber)
    assert(fiberCacheManager.cacheStats.minus(origStats).dataFiberMissCount == 1)
    val sameDataFiber = TestDataFiberId(
      () => fiberCacheManager.toDataFiberCache(data),
      s"test data fiber #$fiberGroupId.0")
    val dataFiberCache2 = fiberCacheManager.get(sameDataFiber)
    assert(fiberCacheManager.cacheStats.minus(origStats).dataFiberHitCount == 1)
    dataFiberCache1.release()
    dataFiberCache2.release()

    val indexFiber = TestIndexFiberId(
      () => fiberCacheManager.toDataFiberCache(data),
      s"test index fiber #$fiberGroupId.0")
    val indexFiberCache1 = fiberCacheManager.get(indexFiber)
    assert(fiberCacheManager.cacheStats.minus(origStats).dataFiberMissCount == 1)
    val sameFiber = TestIndexFiberId(
      () => fiberCacheManager.toDataFiberCache(data),
      s"test index fiber #$fiberGroupId.0")
    val indexFiberCache2 = fiberCacheManager.get(sameFiber)
    assert(fiberCacheManager.cacheStats.minus(origStats).dataFiberHitCount == 1)
    indexFiberCache1.release()
    indexFiberCache2.release()
  }

  test("cache guardian remove pending fibers") {
    newFiberGroup
    Thread.sleep(1000) // Wait some time for CacheGuardian to remove pending fibers
    val dataMemorySizeInMB = (dataCacheMemorySize / mbSize).toInt
    val dataFibers = (1 to dataMemorySizeInMB).map { i =>
      val data = generateData(mbSize)
      TestDataFiberId(() => fiberCacheManager.toDataFiberCache(data),
      s"test fiber #$fiberGroupId.$i")
    }

    val indexMemorySizeInMB = (indexCacheMemorySize / mbSize).toInt
    val indexFibers = (1 to indexMemorySizeInMB).map { i =>
      val data = generateData(mbSize)
      TestIndexFiberId(() => fiberCacheManager.toDataFiberCache(data),
      s"test fiber #$fiberGroupId.$i")
    }
    // release fibers so it has chance to be disposed immediately
    dataFibers.foreach(fiberCacheManager.get(_).release())
    indexFibers.foreach(fiberCacheManager.get(_).release())
    Thread.sleep(1000)
    assert(fiberCacheManager.pendingCount == 0)
    // Hold the fiber, so it can't be disposed until release
    val dataFiberCaches = dataFibers.map(fiberCacheManager.get(_))
    val indexFiberCaches = indexFibers.map(fiberCacheManager.get(_))
    Thread.sleep(1000)
    assert(fiberCacheManager.pendingCount > 0)
    // After release, CacheGuardian should be back to work
    dataFiberCaches.foreach(_.release())
    indexFiberCaches.foreach(_.release())
    // Wait some time for CacheGuardian being waken-up
    Thread.sleep(1000)
    assert(fiberCacheManager.pendingCount == 0)
    Thread.sleep(1000)
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
    var dataLoadTimes = 0
    val dataFiber = TestDataFiberId(() => {
      dataLoadTimes += 1
      fiberCacheManager.toDataFiberCache(data)
    }, s"same fiber test")
    def dataWork(): Unit = {
      val fiberCache = fiberCacheManager.get(dataFiber)
      Thread.sleep(100)
      fiberCache.release()
    }
    val dataRunner = new TestRunner(dataWork)
    val dataPool = Executors.newCachedThreadPool
    (1 to 10).foreach(_ => dataPool.execute(dataRunner))
    dataPool.shutdown()
    dataPool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    assert(dataLoadTimes == 1)

    var indexLoadTimes = 0
    val indexFiber = TestIndexFiberId(() => {
      indexLoadTimes += 1
      fiberCacheManager.toDataFiberCache(data)
    }, s"same fiber test")
    def indexWork(): Unit = {
      val fiberCache = fiberCacheManager.get(indexFiber)
      Thread.sleep(100)
      fiberCache.release()
    }
    val indexRunner = new TestRunner(indexWork)
    val indexPool = Executors.newCachedThreadPool
    (1 to 10).foreach(_ => indexPool.execute(indexRunner))
    indexPool.shutdown()
    indexPool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    assert(indexLoadTimes == 1)
  }

  // request data fibers and index fibers exceed max memory at the same time
  test("get different fiber simultaneously") {
    val dataMemorySizeInMB = (dataCacheMemorySize / mbSize).toInt
    val indexMemorySizeInMB = (indexCacheMemorySize / mbSize).toInt
    val pool = Executors.newCachedThreadPool()
    val dataRunners = (1 to 3).map { i =>
      val data = generateData(dataMemorySizeInMB / 5 * mbSize)
      val fiber = TestDataFiberId(() => fiberCacheManager.toDataFiberCache(data),
      s"different test $i")
      def work(): Boolean = {
        val fiberCache = fiberCacheManager.get(fiber)
        val flag = fiberCache.toArray sameElements data
        Thread.sleep(100)
        fiberCache.release()
        flag
      }
      new TestCaller(work)
    }

    val indexRunners = (1 to 3).map { i =>
      val data = generateData(indexMemorySizeInMB / 5 * mbSize)
      val fiber = TestIndexFiberId(() => fiberCacheManager.toDataFiberCache(data),
      s"different test $i")
      def work(): Boolean = {
        val fiberCache = fiberCacheManager.get(fiber)
        val flag = fiberCache.toArray sameElements data
        Thread.sleep(100)
        fiberCache.release()
        flag
      }
      new TestCaller(work)
    }
    val dataResults = dataRunners.map(t => pool.submit(t))
    val indexResults = indexRunners.map(t => pool.submit(t))
    pool.shutdown()
    pool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    Thread.sleep(100)
    dataResults.foreach(r => r.get())
    indexResults.foreach(r => r.get())
    Thread.sleep(2000) // wait for pending cache to free
    assert(fiberCacheManager.pendingCount == 0)
    Thread.sleep(1000)
  }

  // refCount should be correct
  test("release same fiber simultaneously") {
    val pool = Executors.newCachedThreadPool()
    val data = generateData(kbSize)

    val dataFiber = TestDataFiberId(
      () => fiberCacheManager.toDataFiberCache(data), s"data release test")
    val dataFiberCaches = (1 to 5).map(_ => fiberCacheManager.get(dataFiber))
    assert(dataFiberCaches.head.refCount == 5)

    val indexFiber = TestIndexFiberId(
      () => fiberCacheManager.toDataFiberCache(data), s"index release test")
    val indexFiberCaches = (1 to 5).map(_ => fiberCacheManager.get(indexFiber))
    assert(indexFiberCaches.head.refCount == 5)

    dataFiberCaches.foreach { fiberCache =>
      pool.execute(new TestRunner(() => fiberCache.release()))
    }

    indexFiberCaches.foreach { fiberCache =>
      pool.execute(new TestRunner(() => fiberCache.release()))
    }

    pool.shutdown()
    pool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    assert(dataFiberCaches.head.refCount == 0)
    assert(indexFiberCaches.head.refCount == 0)
  }

  // refCount should be correct, and fiber can be disposed after get
  test("get and release fiber simultaneously") {
    val pool = Executors.newCachedThreadPool()
    val data = generateData(kbSize)
    val dataFiber =
      TestDataFiberId(() => fiberCacheManager.toDataFiberCache(data),
      s"data get release test")
    def dataWork(): Boolean = {
      val fiberCache = fiberCacheManager.get(dataFiber)
      val flag = fiberCache.refCount > 0 || !fiberCache.isDisposed
      fiberCache.release()
      flag
    }

    val indexFiber =
      TestIndexFiberId(() => fiberCacheManager.toDataFiberCache(data),
      s"index get release test")
    def indexWork(): Boolean = {
      val fiberCache = fiberCacheManager.get(indexFiber)
      val flag = fiberCache.refCount > 0 || !fiberCache.isDisposed
      fiberCache.release()
      flag
    }
    val dataResults = (1 to 10).map(_ => pool.submit(new TestCaller(dataWork)))
    val indexResults = (1 to 10).map(_ => pool.submit(new TestCaller(indexWork)))
    pool.shutdown()
    pool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    dataResults.foreach(r => assert(r.get()))
    indexResults.foreach(r => assert(r.get()))
  }

  // fiber must not be removed during get
  test("get and remove fiber simultaneously") {
    val pool = Executors.newCachedThreadPool()
    val data = generateData(kbSize)

    val dataFiber = TestDataFiberId(
      () => fiberCacheManager.toDataFiberCache(data),
      s"data fiber get remove test")
    def dataOccupyWork(): Boolean = {
      (1 to 100).foreach { _ =>
        val fiberCache = fiberCacheManager.get(dataFiber)
        if (fiberCache.isDisposed) {
          fiberCache.release()
          return false
        }
        fiberCache.release()
      }
      true
    }
    def dataRemoveWork(): Unit = {
      (1 to 100000).foreach { _ =>
        fiberCacheManager.releaseFiber(dataFiber)
      }
    }

    val indexFiber = TestIndexFiberId(
      () => fiberCacheManager.toDataFiberCache(data),
      s"index fiber get remove test")
    def indexOccupyWork(): Boolean = {
      (1 to 100).foreach { _ =>
        val fiberCache = fiberCacheManager.get(indexFiber)
        if (fiberCache.isDisposed) {
          fiberCache.release()
          return false
        }
        fiberCache.release()
      }
      true
    }
    def indexRemoveWork(): Unit = {
      (1 to 100000).foreach { _ =>
        fiberCacheManager.releaseFiber(indexFiber)
      }
    }

    val dataResult = pool.submit(new TestCaller(dataOccupyWork))
    pool.execute(new TestRunner(dataRemoveWork))
    val indexResult = pool.submit(new TestCaller(indexOccupyWork))
    pool.execute(new TestRunner(indexRemoveWork))
    pool.shutdown()
    pool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    assert(dataResult.get())
    assert(indexResult.get())
  }

  // Simple Cache doesn't support index and data fiber separation
  test("test Simple Cache Strategy") {
    val cache = new SimpleOapCache()
    val data = generateData(10 * kbSize)
    val fiber = TestDataFiberId(
      () => fiberCacheManager.toDataFiberCache(data), "test simple cache fiber")
    val fiberCache = cache.get(fiber)
    assert(fiberCache.toArray sameElements data)
    fiberCache.release()
    Thread.sleep(500)
    assert(fiberCache.isDisposed)
  }

  test("LRU blocks memory free") {
    val dataInUse = generateData(mbSize)
    val dataMemorySizeInMB = (dataCacheMemorySize / mbSize).toInt
    val dataFiberInUse = TestDataFiberId(
      () => fiberCacheManager.toDataFiberCache(dataInUse),
      s"test fiber #${newFiberGroup}.0")

    // Put into cache and make it use
    val dataFiberCacheInUse = fiberCacheManager.get(dataFiberInUse)
    assert(fiberCacheManager.pendingCount == 0)

    // make fiber in use the 1st element in release queue.
    fiberCacheManager.releaseFiber(dataFiberInUse)

    (1 to dataMemorySizeInMB * 2).foreach { i =>
      val data = generateData(mbSize)
      val fiber = TestDataFiberId(
        () => fiberCacheManager.toDataFiberCache(data),
        s"test fiber #$fiberGroupId.$i")
      val fiberCache = fiberCacheManager.get(fiber)
      assert(fiberCache.toArray sameElements data)
      fiberCache.release()
    }

    // Wait for clean.
    Thread.sleep(6000)
    // There should be only one in-use fiber.
    assert(fiberCacheManager.pendingCount == 1)
    dataFiberCacheInUse.release()
    Thread.sleep(6000)
    assert(fiberCacheManager.pendingCount == 0)

    val indexMemorySizeInMB = (indexCacheMemorySize / mbSize).toInt
    val indexFiberInUse = TestIndexFiberId(
      () => fiberCacheManager.toDataFiberCache(dataInUse),
      s"test fiber #${newFiberGroup}.0")

    // Put into cache and make it use
    val indexFiberCacheInUse = fiberCacheManager.get(indexFiberInUse)
    assert(fiberCacheManager.pendingCount == 0)

    // make fiber in use the 1st element in release queue.
    fiberCacheManager.releaseFiber(indexFiberInUse)

    (1 to indexMemorySizeInMB).foreach { i =>
      val data = generateData(mbSize)
      val fiber = TestDataFiberId(
        () => fiberCacheManager.toDataFiberCache(data),
        s"test fiber #$fiberGroupId.$i")
      val fiberCache = fiberCacheManager.get(fiber)
      assert(fiberCache.toArray sameElements data)
      fiberCache.release()
    }

    // Wait for clean.
    Thread.sleep(6000)
    // There should be only one in-use fiber.
    assert(fiberCacheManager.pendingCount == 1)
    indexFiberCacheInUse.release()
    Thread.sleep(6000)
    assert(fiberCacheManager.pendingCount == 0)

    Thread.sleep(1000)
  }
}
