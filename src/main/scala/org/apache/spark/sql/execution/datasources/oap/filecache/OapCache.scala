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

import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import com.google.common.cache._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

trait OapCache {
  val dataFiberSize: AtomicLong = new AtomicLong(0)
  val indexFiberSize: AtomicLong = new AtomicLong(0)
  val dataFiberCount: AtomicLong = new AtomicLong(0)
  val indexFiberCount: AtomicLong = new AtomicLong(0)

  def get(fiber: Fiber, conf: Configuration): FiberCache
  def getIfPresent(fiber: Fiber): FiberCache
  def getFibers: Set[Fiber]
  def invalidate(fiber: Fiber): Unit
  def invalidateAll(fibers: Iterable[Fiber]): Unit
  def cacheSize: Long
  def cacheCount: Long
  def cacheStats: CacheStats
  def pendingFiberCount: Int
  def cleanUp: Unit = {
    invalidateAll(getFibers)
    dataFiberSize.set(0L)
    dataFiberCount.set(0L)
    indexFiberSize.set(0L)
    indexFiberCount.set(0L)
  }

  def incFiberCountAndSize(fiber: Fiber, count: Long, size: Long): Unit = {
    if (fiber.isInstanceOf[DataFiber]) {
      dataFiberCount.addAndGet(count)
      dataFiberSize.addAndGet(size)
    } else if (fiber.isInstanceOf[BTreeFiber] || fiber.isInstanceOf[BitmapFiber]) {
      indexFiberCount.addAndGet(count)
      indexFiberSize.addAndGet(size)
    }
  }

  def decFiberCountAndSize(fiber: Fiber, count: Long, size: Long): Unit =
    incFiberCountAndSize(fiber, -count, -size)

}

class SimpleOapCache extends OapCache with Logging {

  // We don't bother the memory use of Simple Cache
  private val cacheGuardian = new CacheGuardian(Int.MaxValue)
  cacheGuardian.start()

  override def get(fiber: Fiber, conf: Configuration): FiberCache = {
    val fiberCache = fiber.fiber2Data(conf)
    incFiberCountAndSize(fiber, 1, fiberCache.size())
    fiberCache.occupy()
    // We only use fiber for once, and CacheGuardian will dispose it after release.
    cacheGuardian.addRemovalFiber(fiber, fiberCache)
    decFiberCountAndSize(fiber, 1, fiberCache.size())
    fiberCache
  }

  override def getIfPresent(fiber: Fiber): FiberCache = null

  override def getFibers: Set[Fiber] = {
    Set.empty
  }

  override def invalidate(fiber: Fiber): Unit = {}

  override def invalidateAll(fibers: Iterable[Fiber]): Unit = {}

  override def cacheSize: Long = 0

  override def cacheStats: CacheStats = CacheStats()

  override def cacheCount: Long = 0

  override def pendingFiberCount: Int = cacheGuardian.pendingFiberCount
}

class GuavaOapCache(cacheMemory: Long, cacheGuardianMemory: Long) extends OapCache with Logging {

  // TODO: CacheGuardian can also track cache statistics periodically
  private val cacheGuardian = new CacheGuardian(cacheGuardianMemory)
  cacheGuardian.start()

  private val KB: Double = 1024
  private val MAX_WEIGHT = (cacheMemory / KB).toInt
  private val CONCURRENCY_LEVEL = 4

  // Total cached size for debug purpose, not include pending fiber
  private val _cacheSize: AtomicLong = new AtomicLong(0)

  private val removalListener = new RemovalListener[Fiber, FiberCache] {
    override def onRemoval(notification: RemovalNotification[Fiber, FiberCache]): Unit = {
      logDebug(s"Put fiber into removal list. Fiber: ${notification.getKey}")
      cacheGuardian.addRemovalFiber(notification.getKey, notification.getValue)
      _cacheSize.addAndGet(-notification.getValue.size())
      decFiberCountAndSize(notification.getKey, 1, notification.getValue.size())
    }
  }

  private val weigher = new Weigher[Fiber, FiberCache] {
    override def weigh(key: Fiber, value: FiberCache): Int =
      math.ceil(value.size() / KB).toInt
  }

  /**
   * To avoid storing configuration in each Cache, use a loader.
   * After all, configuration is not a part of Fiber.
   */
  private def cacheLoader(fiber: Fiber, configuration: Configuration) =
    new Callable[FiberCache] {
      override def call(): FiberCache = {
        val startLoadingTime = System.currentTimeMillis()
        val fiberCache = fiber.fiber2Data(configuration)
        incFiberCountAndSize(fiber, 1, fiberCache.size())
        logDebug("Load missed fiber took %s. Fiber: %s"
          .format(Utils.getUsedTimeMs(startLoadingTime), fiber))
        _cacheSize.addAndGet(fiberCache.size())
        fiberCache
      }
    }

  private val cache = CacheBuilder.newBuilder()
    .recordStats()
    .removalListener(removalListener)
    .maximumWeight(MAX_WEIGHT)
    .weigher(weigher)
    .concurrencyLevel(CONCURRENCY_LEVEL)
    .build[Fiber, FiberCache]()

  override def get(fiber: Fiber, conf: Configuration): FiberCache = {
    val readLock = FiberLockManager.getFiberLock(fiber).readLock()
    readLock.lock()
    try {
      val fiberCache = cache.get(fiber, cacheLoader(fiber, conf))
      // Avoid loading a fiber larger than MAX_WEIGHT / CONCURRENCY_LEVEL
      assert(fiberCache.size() <= MAX_WEIGHT * KB / CONCURRENCY_LEVEL,
        s"Failed to cache fiber(${Utils.bytesToString(fiberCache.size())}) " +
          s"with cache's MAX_WEIGHT" +
          s"(${Utils.bytesToString(MAX_WEIGHT.toLong * KB.toLong)}) / $CONCURRENCY_LEVEL")
      fiberCache.occupy()
      fiberCache
    } finally {
      readLock.unlock()
    }
  }

  override def getIfPresent(fiber: Fiber): FiberCache = cache.getIfPresent(fiber)

  override def getFibers: Set[Fiber] = {
    cache.asMap().keySet().asScala.toSet
  }

  override def invalidate(fiber: Fiber): Unit = {
    cache.invalidate(fiber)
  }

  override def invalidateAll(fibers: Iterable[Fiber]): Unit = {
    cache.invalidateAll(fibers.asJava)
  }

  override def cacheSize: Long = _cacheSize.get()

  override def cacheStats: CacheStats = {
    val stats = cache.stats()
    CacheStats(
      dataFiberCount.get(), dataFiberSize.get(),
      indexFiberCount.get(), indexFiberSize.get(),
      pendingFiberCount, cacheGuardian.pendingFiberSize,
      stats.hitCount(),
      stats.missCount(),
      stats.loadCount(),
      stats.totalLoadTime(),
      stats.evictionCount()
    )
  }

  override def cacheCount: Long = cache.size()

  override def pendingFiberCount: Int = cacheGuardian.pendingFiberCount

  override def cleanUp: Unit = {
    super.cleanUp
    cache.cleanUp
  }
}
