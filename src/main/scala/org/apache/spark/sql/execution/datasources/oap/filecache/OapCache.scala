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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import com.google.common.cache._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.Utils

trait OapCache {
  val dataFiberSize: AtomicLong = new AtomicLong(0)
  val indexFiberSize: AtomicLong = new AtomicLong(0)
  val dataFiberCount: AtomicLong = new AtomicLong(0)
  val indexFiberCount: AtomicLong = new AtomicLong(0)

  def get(fiberId: FiberId): FiberCache
  def getIfPresent(fiberId: FiberId): FiberCache
  def getFibers: Set[FiberId]
  def invalidate(fiberId: FiberId): Unit
  def invalidateAll(fibers: Iterable[FiberId]): Unit
  def removeFromEvictedQueue(fiberId: FiberId, fiberCache: FiberCache): Boolean
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

  def incFiberCountAndSize(fiberId: FiberId, count: Long, size: Long): Unit = {
    if (fiberId.isInstanceOf[DataFiberId]) {
      dataFiberCount.addAndGet(count)
      dataFiberSize.addAndGet(size)
    } else if (fiberId.isInstanceOf[BTreeFiberId] || fiberId.isInstanceOf[BitmapFiberId]) {
      indexFiberCount.addAndGet(count)
      indexFiberSize.addAndGet(size)
    }
  }

  def decFiberCountAndSize(fiberId: FiberId, count: Long, size: Long): Unit =
    incFiberCountAndSize(fiberId, -count, -size)

  protected def cache(fiberId: FiberId): FiberCache = {
    val memoryBlock: MemoryBlock = fiberId match {
      case DataFiberId(file, columnIndex, rowGroupId) => file.cache(rowGroupId, columnIndex)
      case BTreeFiberId(getFiberData, _, _, _) => getFiberData.apply()
      case BitmapFiberId(getFiberData, _, _, _) => getFiberData.apply()
      case TestFiberId(getFiberData, _) => getFiberData.apply()
      case _ => throw new OapException("Unexpected FiberId type!")
    }
    FiberCache(fiberId, memoryBlock)
  }

}

class SimpleOapCache extends OapCache with Logging {

  // We don't bother the memory use of Simple Cache
  private val evictedCacheGuardian = new CacheGuardian(Int.MaxValue)

  override def get(fiberId: FiberId): FiberCache = {
    val fiberCache = cache(fiberId)
    incFiberCountAndSize(fiberId, 1, fiberCache.size())
    fiberCache.occupy()
    // We only use fiber for once, and CacheGuardian will dispose it after release.
    evictedCacheGuardian.addEvictedFiberToEvictedQueue(fiberId, fiberCache)
    decFiberCountAndSize(fiberId, 1, fiberCache.size())
    fiberCache
  }

  override def removeFromEvictedQueue(fiberId: FiberId, fiberCache: FiberCache): Boolean =
    evictedCacheGuardian.removeFromEvictedQueue(fiberId, fiberCache)

  override def getIfPresent(fiberId: FiberId): FiberCache = null

  override def getFibers: Set[FiberId] = {
    Set.empty
  }

  override def invalidate(fiberId: FiberId): Unit = {}

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {}

  override def cacheSize: Long = 0

  override def cacheStats: CacheStats = CacheStats()

  override def cacheCount: Long = 0

  override def pendingFiberCount: Int = evictedCacheGuardian.pendingFiberCount
}

class GuavaOapCache(cacheMemory: Long, cacheGuardianMemory: Long) extends OapCache with Logging {

  // TODO: CacheGuardian can also track cache statistics periodically
  private val evictedCacheGuardian = new CacheGuardian(cacheGuardianMemory)

  private val KB: Double = 1024
  private val MAX_WEIGHT = (cacheMemory / KB).toInt
  private val CONCURRENCY_LEVEL = 4

  // Total cached size for debug purpose, not include pending fiber
  private val _cacheSize: AtomicLong = new AtomicLong(0)

  private val removalListener = new RemovalListener[FiberId, FiberCache] {
    override def onRemoval(notification: RemovalNotification[FiberId, FiberCache]): Unit = {
      logDebug(s"Put fiber into removal list. Fiber: ${notification.getKey}")
      evictedCacheGuardian.addEvictedFiberToEvictedQueue(notification.getKey, notification.getValue)
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

  override def removeFromEvictedQueue(fiberId: FiberId, fiberCache: FiberCache): Boolean =
    evictedCacheGuardian.removeFromEvictedQueue(fiberId, fiberCache)

  override def get(fiberId: FiberId): FiberCache = {
    val readLock = OapRuntime.getOrCreate.fiberLockManager.getFiberLock(fiberId).readLock()
    readLock.lock()
    try {
      val fiberCache = cacheInstance.get(fiberId)
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

  override def getIfPresent(fiberId: FiberId): FiberCache = cacheInstance.getIfPresent(fiberId)

  override def getFibers: Set[FiberId] = {
    cacheInstance.asMap().keySet().asScala.toSet
  }

  override def invalidate(fiberId: FiberId): Unit = {
    cacheInstance.invalidate(fiberId)
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
      pendingFiberCount, evictedCacheGuardian.pendingFiberSize,
      stats.hitCount(),
      stats.missCount(),
      stats.loadCount(),
      stats.totalLoadTime(),
      stats.evictionCount()
    )
  }

  override def cacheCount: Long = cacheInstance.size()

  override def pendingFiberCount: Int = evictedCacheGuardian.pendingFiberCount

  override def cleanUp: Unit = {
    super.cleanUp
    cacheInstance.cleanUp
  }
}
