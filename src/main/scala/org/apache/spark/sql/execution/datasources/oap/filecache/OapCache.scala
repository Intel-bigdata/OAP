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
import java.util.concurrent.locks.{Condition, ReentrantLock}

import scala.collection.JavaConverters._

import com.google.common.cache._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.util.Utils

private[filecache] class CacheGuardian(maxMemory: Long) extends Thread with Logging {

  // pendingFiberSize and pendingFiberCapacity are different. pendingFiberSize used to
  // show the pending size to user, however pendingFiberCapacity is used to record the
  // actual used memory and log warn when exceed the maxMemory.
  private val _pendingFiberSize: AtomicLong = new AtomicLong(0)
  private val _pendingFiberCapacity: AtomicLong = new AtomicLong(0)

  private val removalPendingQueue = new LinkedBlockingQueue[(FiberId, FiberCache)]()

  private val guardianLock = new ReentrantLock()
  private val guardianLockCond = guardianLock.newCondition()

  private var waitNotifyActive: Boolean = false

  // Tell if guardian thread is trying to remove one Fiber.
  @volatile private var bRemoving: Boolean = false

  def enableWaitNotifyActive(): Unit = {
    waitNotifyActive = true
  }

  def getGuardianLock(): ReentrantLock = {
    guardianLock
  }

  def getGuardianLockCondition(): Condition = {
    guardianLockCond
  }

  def pendingFiberCount: Int = if (bRemoving) {
    removalPendingQueue.size() + 1
  } else {
    removalPendingQueue.size()
  }

  def pendingFiberSize: Long = _pendingFiberSize.get()

  def pendingFiberOccupiedSize: Long = _pendingFiberCapacity.get()

  def addRemovalFiber(fiber: FiberId, fiberCache: FiberCache): Unit = {
    _pendingFiberSize.addAndGet(fiberCache.size())
    // Record the occupied size
    _pendingFiberCapacity.addAndGet(fiberCache.getOccupiedSize())
    removalPendingQueue.offer((fiber, fiberCache))
    if (_pendingFiberCapacity.get() > maxMemory) {
      logWarning("Fibers pending on removal use too much memory, " +
          s"current: ${_pendingFiberCapacity.get()}, max: $maxMemory")
    }
  }

  override def run(): Unit = {
    while (true) {
      val fiberCache = removalPendingQueue.take()._2
      releaseFiberCache(fiberCache)
    }
  }

  private def releaseFiberCache(cache: FiberCache): Unit = {
    bRemoving = true
    val fiberId = cache.fiberId
    logDebug(s"Removing fiber: $fiberId")
    // Block if fiber is in use.
    if (!cache.tryDispose()) {
      logDebug(s"Waiting fiber to be released timeout. Fiber: $fiberId")
      removalPendingQueue.offer((fiberId, cache))
      if (_pendingFiberCapacity.get() > maxMemory) {
        logWarning("Fibers pending on removal use too much memory, " +
            s"current: ${_pendingFiberCapacity.get()}, max: $maxMemory")
      }
    } else {
      _pendingFiberSize.addAndGet(-cache.size())

      // TODO: Make log more readable
      logDebug(s"Fiber removed successfully. Fiber: $fiberId")
      if (waitNotifyActive) {
        this.getGuardianLock().lock()
        _pendingFiberCapacity.addAndGet(-cache.getOccupiedSize())
        if (_pendingFiberCapacity.get() <
          OapRuntime.getOrCreate.fiberCacheManager.dcpmmWaitingThreshold) {
          guardianLockCond.signalAll()
        }
        this.getGuardianLock().unlock()
      } else {
        _pendingFiberCapacity.addAndGet(-cache.getOccupiedSize())
      }
    }
    bRemoving = false
  }
}

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
  def pendingFiberSize: Long
  def pendingFiberOccupiedSize: Long
  def getCacheGuardian: CacheGuardian
  def cleanUp(): Unit = {
    invalidateAll(getFibers)
    dataFiberSize.set(0L)
    dataFiberCount.set(0L)
    indexFiberSize.set(0L)
    indexFiberCount.set(0L)
  }

  def incFiberCountAndSize(fiber: FiberId, count: Long, size: Long): Unit = {
    if (fiber.isInstanceOf[DataFiberId] || fiber.isInstanceOf[TestDataFiberId]) {
      dataFiberCount.addAndGet(count)
      dataFiberSize.addAndGet(size)
    } else if (
      fiber.isInstanceOf[BTreeFiberId] ||
      fiber.isInstanceOf[BitmapFiberId] ||
      fiber.isInstanceOf[TestIndexFiberId]) {
      indexFiberCount.addAndGet(count)
      indexFiberSize.addAndGet(size)
    }
  }

  def decFiberCountAndSize(fiber: FiberId, count: Long, size: Long): Unit =
    incFiberCountAndSize(fiber, -count, -size)

  protected def cache(fiber: FiberId): FiberCache = {
    val cache = fiber match {
      case binary: BinaryDataFiberId => binary.doCache()
      case VectorDataFiberId(file, columnIndex, rowGroupId) => file.cache(rowGroupId, columnIndex)
      case BTreeFiberId(getFiberData, _, _, _) => getFiberData.apply()
      case BitmapFiberId(getFiberData, _, _, _) => getFiberData.apply()
      case TestDataFiberId(getFiberData, _) => getFiberData.apply()
      case TestIndexFiberId(getFiberData, _) => getFiberData.apply()
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

  override def pendingFiberSize: Long = cacheGuardian.pendingFiberSize

  override def pendingFiberOccupiedSize: Long = cacheGuardian.pendingFiberOccupiedSize

  override def getCacheGuardian: CacheGuardian = cacheGuardian
}

class GuavaOapCache(
    dataCacheMemory: Long,
    indexCacheMemory: Long,
    cacheGuardianMemory: Long,
    var separationCache: Boolean)
    extends OapCache with Logging {

  // TODO: CacheGuardian can also track cache statistics periodically
  private val cacheGuardian = new CacheGuardian(cacheGuardianMemory)
  cacheGuardian.start()

  private val KB: Double = 1024
  private val DATA_MAX_WEIGHT = (dataCacheMemory / KB).toInt
  private val INDEX_MAX_WEIGHT = (indexCacheMemory / KB).toInt
  private val TOTAL_MAX_WEIGHT = INDEX_MAX_WEIGHT + DATA_MAX_WEIGHT
  private val CONCURRENCY_LEVEL = 4

  // Total cached size for debug purpose, not include pending fiber
  private val _cacheSize: AtomicLong = new AtomicLong(0)

  private val removalListener = new RemovalListener[FiberId, FiberCache] {
    override def onRemoval(notification: RemovalNotification[FiberId, FiberCache]): Unit = {
      logDebug(s"Put fiber into removal list. Fiber: ${notification.getKey}")

      // if the refCount ==0, directly free and not put in 'removalPendingQueue'
      // to wait the single thread to release. And if release failed,
      // still put it in 'removalPendingQueue'
      if (!notification.getValue.tryDisposeWithoutWait()) {
        cacheGuardian.addRemovalFiber(notification.getKey, notification.getValue)
      }
      _cacheSize.addAndGet(-notification.getValue.size())
      decFiberCountAndSize(notification.getKey, 1, notification.getValue.size())
    }
  }

  private val weigher = new Weigher[FiberId, FiberCache] {
    override def weigh(key: FiberId, value: FiberCache): Int = {
      // We should calculate the weigh with the occupied size of the block.
      math.ceil(value.getOccupiedSize() / KB).toInt
    }
  }

  private var cacheInstance = if (separationCache) {
    initLoadingCache(DATA_MAX_WEIGHT)
  } else {
    initLoadingCache(TOTAL_MAX_WEIGHT)
  }

  // this is only used when enable index and data cache separation
  private var indexCacheInstance = if (separationCache) {
    initLoadingCache(INDEX_MAX_WEIGHT)
  } else {
    null
  }

  private def initLoadingCache(weight: Int) = {
    CacheBuilder.newBuilder()
      .recordStats()
      .removalListener(removalListener)
      .maximumWeight(weight)
      .weigher(weigher)
      .concurrencyLevel(CONCURRENCY_LEVEL)
      .build[FiberId, FiberCache](new CacheLoader[FiberId, FiberCache] {
      override def load(key: FiberId): FiberCache = {
        val startLoadingTime = System.currentTimeMillis()
        val fiberCache = cache(key)
        incFiberCountAndSize(key, 1, fiberCache.size())
        logDebug(
          "Load missed index fiber took %s. Fiber: %s. length: %s".format(
            Utils.getUsedTimeMs(startLoadingTime), key, fiberCache.size()))
        _cacheSize.addAndGet(fiberCache.size())
        fiberCache
      }
    })
  }

  override def get(fiber: FiberId): FiberCache = {
    val readLock = OapRuntime.getOrCreate.fiberCacheManager.getFiberLock(fiber).readLock()
    readLock.lock()
    try {
        if (separationCache) {
          if (fiber.isInstanceOf[DataFiberId] || fiber.isInstanceOf[TestDataFiberId]) {
            val fiberCache = cacheInstance.get(fiber)
            // Avoid loading a fiber larger than DATA_MAX_WEIGHT / CONCURRENCY_LEVEL
            assert(fiberCache.size() <= DATA_MAX_WEIGHT * KB / CONCURRENCY_LEVEL,
              s"Failed to cache fiber(${Utils.bytesToString(fiberCache.size())}) " +
                s"with cache's MAX_WEIGHT" +
                s"(${Utils.bytesToString(DATA_MAX_WEIGHT.toLong * KB.toLong)}) " +
                s"/ $CONCURRENCY_LEVEL")
            fiberCache.occupy()
            fiberCache
          } else if (
            fiber.isInstanceOf[BTreeFiberId] ||
              fiber.isInstanceOf[BitmapFiberId] ||
              fiber.isInstanceOf[TestIndexFiberId]) {
            val fiberCache = indexCacheInstance.get(fiber)
            // Avoid loading a fiber larger than INDEX_MAX_WEIGHT / CONCURRENCY_LEVEL
            assert(fiberCache.size() <= INDEX_MAX_WEIGHT * KB / CONCURRENCY_LEVEL,
              s"Failed to cache fiber(${Utils.bytesToString(fiberCache.size())}) " +
                s"with cache's MAX_WEIGHT" +
                s"(${Utils.bytesToString(INDEX_MAX_WEIGHT.toLong * KB.toLong)}) " +
                s"/ $CONCURRENCY_LEVEL")
            fiberCache.occupy()
            fiberCache
          } else throw new OapException(s"not support fiber type $fiber")
        } else {
          val fiberCache = cacheInstance.get(fiber)
          // Avoid loading a fiber larger than MAX_WEIGHT / CONCURRENCY_LEVEL
          assert(fiberCache.size() <= TOTAL_MAX_WEIGHT * KB / CONCURRENCY_LEVEL,
            s"Failed to cache fiber(${Utils.bytesToString(fiberCache.size())}) " +
              s"with cache's MAX_WEIGHT" +
              s"(${Utils.bytesToString(TOTAL_MAX_WEIGHT.toLong * KB.toLong)}) / $CONCURRENCY_LEVEL")
          fiberCache.occupy()
          fiberCache
        }
    } finally {
      readLock.unlock()
    }
  }

  override def getIfPresent(fiber: FiberId): FiberCache =
    if (separationCache &&
        (fiber.isInstanceOf[BTreeFiberId] ||
         fiber.isInstanceOf[BitmapFiberId] ||
         fiber.isInstanceOf[TestIndexFiberId])) {
        indexCacheInstance.getIfPresent(fiber)
    } else {
      cacheInstance.getIfPresent(fiber)
    }

  override def getFibers: Set[FiberId] =
    if (separationCache) {
      cacheInstance.asMap().keySet().asScala.toSet ++
        indexCacheInstance.asMap().keySet().asScala.toSet
    } else {
      cacheInstance.asMap().keySet().asScala.toSet
    }

  override def invalidate(fiber: FiberId): Unit =
    if (separationCache &&
        (fiber.isInstanceOf[BTreeFiberId] ||
         fiber.isInstanceOf[BitmapFiberId] ||
         fiber.isInstanceOf[TestIndexFiberId])) {
      indexCacheInstance.invalidate(fiber)
    } else {
      cacheInstance.invalidate(fiber)
    }

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {
    fibers.foreach(invalidate)
  }

  override def cacheSize: Long = _cacheSize.get()

  override def cacheStats: CacheStats = {
    if (separationCache) {
      val dataStats = cacheInstance.stats()
      val indexStats = indexCacheInstance.stats()
      CacheStats(
        dataFiberCount.get(), dataFiberSize.get(),
        indexFiberCount.get(), indexFiberSize.get(),
        pendingFiberCount, cacheGuardian.pendingFiberSize,
        dataStats.hitCount(),
        dataStats.missCount(),
        dataStats.loadCount(),
        dataStats.totalLoadTime(),
        dataStats.evictionCount(),
        indexStats.hitCount(),
        indexStats.missCount(),
        indexStats.loadCount(),
        indexStats.totalLoadTime(),
        indexStats.evictionCount()
      )
    } else {
      // when disable index and data cache separation
      // we can't independently retrieve index and data cache
      val cacheStats = cacheInstance.stats()
      CacheStats(
        dataFiberCount.get(), dataFiberSize.get(),
        indexFiberCount.get(), indexFiberSize.get(),
        pendingFiberCount, cacheGuardian.pendingFiberSize,
        cacheStats.hitCount(),
        cacheStats.missCount(),
        cacheStats.loadCount(),
        cacheStats.totalLoadTime(),
        cacheStats.evictionCount(),
        0L,
        0L,
        0L,
        0L,
        0L
      )
    }
  }

  override def cacheCount: Long =
    if (separationCache) {
      cacheInstance.size() + indexCacheInstance.size()
    } else {
      cacheInstance.size()
    }

  override def pendingFiberCount: Int = cacheGuardian.pendingFiberCount

  override def pendingFiberSize: Long = cacheGuardian.pendingFiberSize

  override def pendingFiberOccupiedSize: Long = cacheGuardian.pendingFiberOccupiedSize

  override def getCacheGuardian: CacheGuardian = cacheGuardian

  override def cleanUp(): Unit = {
    super.cleanUp()
    cacheInstance.cleanUp()
    if (separationCache) {
      indexCacheInstance.cleanUp()
    }
  }

  // This is only for test purpose
  private[filecache] def enableCacheSeparation(): Unit = {
    this.separationCache = true
    cacheInstance = initLoadingCache(DATA_MAX_WEIGHT)
    indexCacheInstance = initLoadingCache(INDEX_MAX_WEIGHT)
  }
}
