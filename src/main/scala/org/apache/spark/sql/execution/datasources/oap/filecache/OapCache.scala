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

trait OapCache {
  def get(fiber: Fiber, conf: Configuration): FiberCache
  def getIfPresent(fiber: Fiber): FiberCache
  def getFibers: Set[Fiber]
  def invalidate(fiber: Fiber): Unit
  def invalidateAll(fibers: Iterable[Fiber]): Unit
  def cacheSize: Long
  def cacheCount: Long
  // TODO: To be compatible with some test cases. But we shouldn't rely on Guava in trait.
  def cacheStats: CacheStats
  def pendingSize: Int
}

class SimpleOapCache extends OapCache with Logging {

  // We don't bother the memory use of Simple Cache
  private val cacheGuardian = new CacheGuardian(Int.MaxValue)
  cacheGuardian.start()

  override def get(fiber: Fiber, conf: Configuration): FiberCache = {
    val fiberCache = fiber.fiber2Data(conf)
    fiberCache.occupy()
    // We only use fiber for once, and CacheGuardian will dispose it after release.
    cacheGuardian.addRemovalFiber(fiberCache)
    fiberCache
  }

  override def getIfPresent(fiber: Fiber): FiberCache = null

  override def getFibers: Set[Fiber] = {
    Set.empty
  }

  override def invalidate(fiber: Fiber): Unit = {}

  override def invalidateAll(fibers: Iterable[Fiber]): Unit = {}

  override def cacheSize: Long = 0

  override def cacheStats: CacheStats = {
    new CacheStats(0, 0, 0, 0, 0, 0)
  }

  override def cacheCount: Long = 0

  override def pendingSize: Int = cacheGuardian.pendingSize
}

class GuavaOapCache(cacheMemory: Long, cacheGuardianMemory: Long) extends OapCache with Logging {

  // TODO: CacheGuardian can also track cache statistics periodically
  private val cacheGuardian = new CacheGuardian(cacheGuardianMemory)
  cacheGuardian.start()

  private val MB: Double = 1024 * 1024
  private val MAX_WEIGHT = (cacheMemory / MB).toInt

  // Total cached size for debug purpose
  private val _cacheSize: AtomicLong = new AtomicLong(0)

  private val removalListener = new RemovalListener[Fiber, FiberCache] {
    override def onRemoval(notification: RemovalNotification[Fiber, FiberCache]): Unit = {
      logDebug(s"Add Cache into removal list: ${notification.getKey}")
      cacheGuardian.addRemovalFiber(notification.getValue)
      _cacheSize.addAndGet(-notification.getValue.size())
    }
  }

  private val weigher = new Weigher[Fiber, FiberCache] {
    override def weigh(key: Fiber, value: FiberCache): Int =
      math.ceil(value.size() / MB).toInt
  }

  /**
   * To avoid storing configuration in each Cache, use a loader.
   * After all, configuration is not a part of Fiber.
   */
  private def cacheLoader(fiber: Fiber, configuration: Configuration) =
    new Callable[FiberCache] {
      override def call(): FiberCache = {
        logDebug(s"Loading Cache: $fiber")
        val fiberCache = fiber.fiber2Data(configuration)
        _cacheSize.addAndGet(fiberCache.size())
        fiberCache
      }
    }

  private val cache = CacheBuilder.newBuilder()
    .recordStats()
    .removalListener(removalListener)
    .maximumWeight(MAX_WEIGHT)
    .weigher(weigher)
    .build[Fiber, FiberCache]()

  override def get(fiber: Fiber, conf: Configuration): FiberCache = {
    val fiberCache = cache.get(fiber, cacheLoader(fiber, conf))
    // Avoid loading a fiber larger than MAX_WEIGHT / 4, 4 is concurrency number
    assert(fiberCache.size() <= MAX_WEIGHT * MB / 4, "Can't cache fiber larger than MAX_WEIGHT / 4")
    fiberCache.occupy()
    fiberCache
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

  override def cacheStats: CacheStats = cache.stats()

  override def cacheCount: Long = cache.size()

  override def pendingSize: Int = cacheGuardian.pendingSize
}
