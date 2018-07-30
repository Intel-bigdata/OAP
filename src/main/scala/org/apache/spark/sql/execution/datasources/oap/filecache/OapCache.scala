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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import com.google.common.cache._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.util.Utils

trait OapCache {
  val dataFiberSize: AtomicLong = new AtomicLong(0)
  val indexFiberSize: AtomicLong = new AtomicLong(0)
  val dataFiberCount: AtomicLong = new AtomicLong(0)
  val indexFiberCount: AtomicLong = new AtomicLong(0)

  def get(fiber: FiberId): FiberCache
  def getIfPresent(fiber: FiberId): FiberCache
  def getFibers: Set[FiberId]
  def invalidate(fiber: FiberId): Unit
  def invalidateAll(fibers: Iterable[FiberId]): Unit
  def cacheSize: Long
  def cacheCount: Long
  def cacheStats: CacheStats
  def pendingFiberCount: Int
  def cleanUp(): Unit = {
    invalidateAll(getFibers)
    dataFiberSize.set(0L)
    dataFiberCount.set(0L)
    indexFiberSize.set(0L)
    indexFiberCount.set(0L)
  }

  def incFiberCountAndSize(fiber: FiberId, count: Long, size: Long): Unit = {
    if (fiber.isInstanceOf[DataFiberId]) {
      dataFiberCount.addAndGet(count)
      dataFiberSize.addAndGet(size)
    } else if (fiber.isInstanceOf[BTreeFiberId] || fiber.isInstanceOf[BitmapFiberId]) {
      indexFiberCount.addAndGet(count)
      indexFiberSize.addAndGet(size)
    }
  }

  def decFiberCountAndSize(fiber: FiberId, count: Long, size: Long): Unit =
    incFiberCountAndSize(fiber, -count, -size)

  protected def cache(fiber: FiberId): FiberCache = {
    val cache = fiber match {
      case DataFiberId(file, columnIndex, rowGroupId) => file.cache(rowGroupId, columnIndex)
      case BTreeFiberId(getFiberData, _, _, _) => getFiberData.apply()
      case BitmapFiberId(getFiberData, _, _, _) => getFiberData.apply()
      case TestFiberId(getFiberData, _) => getFiberData.apply()
      case _ => throw new OapException("Unexpected FiberId type!")
    }
    cache.fiberId = fiber
    cache
  }

}

class SimpleOapCache extends OapCache with Logging {

  // We don't bother the memory use of Simple Cache
  private val cacheGuardian = new CacheGuardian(Int.MaxValue)
  cacheGuardian.start()

  override def get(fiberId: FiberId): FiberCache = {
    val fiberCache = cache(fiberId)
    incFiberCountAndSize(fiberId, 1, fiberCache.size())
    fiberCache.occupy()
    // We only use fiber for once, and CacheGuardian will dispose it after release.
    cacheGuardian.addRemovalFiber(fiberId, fiberCache)
    decFiberCountAndSize(fiberId, 1, fiberCache.size())
    fiberCache
  }

  override def getIfPresent(fiber: FiberId): FiberCache = null

  override def getFibers: Set[FiberId] = {
    Set.empty
  }

  override def invalidate(fiber: FiberId): Unit = {}

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {}

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

  private val removalListener = new RemovalListener[FiberId, FiberCache] {
    override def onRemoval(notification: RemovalNotification[FiberId, FiberCache]): Unit = {
      logDebug(s"Put fiber into removal list. Fiber: ${notification.getKey}")
      cacheGuardian.addRemovalFiber(notification.getKey, notification.getValue)
      _cacheSize.addAndGet(-notification.getValue.size())
      decFiberCountAndSize(notification.getKey, 1, notification.getValue.size())
    }
  }

  private val weigher = new Weigher[FiberId, FiberCache] {
    override def weigh(key: FiberId, value: FiberCache): Int =
      math.ceil(value.size() / KB).toInt
  }

  private val cacheInstance = CacheBuilder.newBuilder()
    .recordStats()
    .removalListener(removalListener)
    .maximumWeight(MAX_WEIGHT)
    .weigher(weigher)
    .concurrencyLevel(CONCURRENCY_LEVEL)
    .build[FiberId, FiberCache](new CacheLoader[FiberId, FiberCache] {
      override def load(key: FiberId): FiberCache = {
        val startLoadingTime = System.currentTimeMillis()
        val fiberCache = cache(key)
        incFiberCountAndSize(key, 1, fiberCache.size())
        logDebug(
          "Load missed fiber took %s. Fiber: %s".format(Utils.getUsedTimeMs(startLoadingTime), key))
        _cacheSize.addAndGet(fiberCache.size())
        fiberCache
      }
    })


  override def get(fiber: FiberId): FiberCache = {
    val readLock = OapRuntime.getOrCreate.fiberLockManager.getFiberLock(fiber).readLock()
    readLock.lock()
    try {
      val fiberCache = cacheInstance.get(fiber)
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

  override def getIfPresent(fiber: FiberId): FiberCache = cacheInstance.getIfPresent(fiber)

  override def getFibers: Set[FiberId] = {
    cacheInstance.asMap().keySet().asScala.toSet
  }

  override def invalidate(fiber: FiberId): Unit = {
    cacheInstance.invalidate(fiber)
  }

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {
    cacheInstance.invalidateAll(fibers.asJava)
  }

  override def cacheSize: Long = _cacheSize.get()

  override def cacheStats: CacheStats = {
    val stats = cacheInstance.stats()
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

  override def cacheCount: Long = cacheInstance.size()

  override def pendingFiberCount: Int = cacheGuardian.pendingFiberCount

  override def cleanUp: Unit = {
    super.cleanUp
    cacheInstance.cleanUp
  }
}
