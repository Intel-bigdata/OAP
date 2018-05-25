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

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.google.common.cache._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

private[filecache] class CacheGuardian(maxMemory: Long) extends Thread with Logging {

  private val _pendingFiberSize: AtomicLong = new AtomicLong(0)

  private val removalPendingQueue = new LinkedBlockingQueue[(Fiber, FiberCache)]()

  // Tell if guardian thread is trying to remove one Fiber.
  @volatile private var bRemoving: Boolean = false

  def pendingFiberCount: Int = if (bRemoving) {
    removalPendingQueue.size() + 1
  } else {
    removalPendingQueue.size()
  }

  def pendingFiberSize: Long = _pendingFiberSize.get()

  def addRemovalFiber(fiber: Fiber, fiberCache: FiberCache): Unit = {
    _pendingFiberSize.addAndGet(fiberCache.size())
    removalPendingQueue.offer((fiber, fiberCache))
    if (_pendingFiberSize.get() > maxMemory) {
      logWarning("Fibers pending on removal use too much memory, " +
          s"current: ${_pendingFiberSize.get()}, max: $maxMemory")
    }
  }

  override def run(): Unit = {
    // Loop forever, TODO: provide a release function
    while (true) {
      val (fiber, fiberCache) = removalPendingQueue.take()
      bRemoving = true
      logDebug(s"Removing fiber: $fiber")
      // Block if fiber is in use.
      if (!fiberCache.tryDispose(fiber, 3000)) {
        // Check memory usage every 3s while we are waiting fiber release.
        logDebug(s"Waiting fiber to be released timeout. Fiber: $fiber")
        removalPendingQueue.offer((fiber, fiberCache))
        if (_pendingFiberSize.get() > maxMemory) {
          logWarning("Fibers pending on removal use too much memory, " +
            s"current: ${_pendingFiberSize.get()}, max: $maxMemory")
        }
      } else {
        _pendingFiberSize.addAndGet(-fiberCache.size())
        // TODO: Make log more readable
        logDebug(s"Fiber removed successfully. Fiber: $fiber")
      }
      bRemoving = false
    }
  }
}

private[sql] class FiberCacheManager(
    sparkEnv: SparkEnv, memoryManager: MemoryManager) extends Logging {

  private val GUAVA_CACHE = "guava"
  private val SIMPLE_CACHE = "simple"
  private val DEFAULT_CACHE_STRATEGY = GUAVA_CACHE

  private val cacheBackend: OapCache = {
    val cacheName = sparkEnv.conf.get("spark.oap.cache.strategy", DEFAULT_CACHE_STRATEGY)
    if (cacheName.equals(GUAVA_CACHE)) {
      new GuavaOapCache(memoryManager.cacheMemory, memoryManager.cacheGuardianMemory)
    } else if (cacheName.equals(SIMPLE_CACHE)) {
      new SimpleOapCache()
    } else {
      throw new OapException(s"Unsupported cache strategy $cacheName")
    }
  }

  def stop(): Unit = {
    cacheBackend.cleanUp()
  }

  // NOTE: all members' init should be placed before this line.
  logDebug(s"Initialized FiberCacheManager")

  def get(fiber: Fiber, conf: Configuration): FiberCache = {
    logDebug(s"Getting Fiber: $fiber")
    cacheBackend.get(fiber, conf)
  }

  def removeIndexCache(indexName: String): Unit = {
    logDebug(s"Going to remove all index cache of $indexName")
    val fiberToBeRemoved = cacheBackend.getFibers.filter {
      case BTreeFiber(_, file, _, _) => file.contains(indexName)
      case BitmapFiber(_, file, _, _) => file.contains(indexName)
      case _ => false
    }
    cacheBackend.invalidateAll(fiberToBeRemoved)
    logDebug(s"Removed ${fiberToBeRemoved.size} fibers.")
  }

  // Used by test suite
  private[filecache] def removeFiber(fiber: TestFiber): Unit = {
    if (cacheBackend.getIfPresent(fiber) != null) {
      cacheBackend.invalidate(fiber)
    }
  }

  // Used by test suite
  private[oap] def clearAllFibers(): Unit = cacheBackend.cleanUp

  // TODO: test case, consider data eviction, try not use DataFileMeta which my be costly
  private[sql] def status(): String = {
    logDebug(s"Reporting ${cacheBackend.cacheCount} fibers to the master")
    val dataFibers = cacheBackend.getFibers.collect {
      case fiber: DataFiber => fiber
    }

    // Use a bit set to represent current cache status of one file.
    // Say, there is a file has 3 row groups and 3 columns. Then bit set size is 3 * 3 = 9
    // Say, cache status is below:
    //            field#0    field#1     field#2
    // group#0       -        cached        -          // BitSet(1 + 0 * 3) = 1
    // group#1       -        cached        -          // BitSet(1 + 1 * 3) = 1
    // group#2       -          -         cached       // BitSet(2 + 2 * 3) = 1
    // The final bit set is: 010010001
    val statusRawData = dataFibers.groupBy(_.file).map {
      case (dataFile, fiberSet) =>
        val fileMeta: DataFileMeta = DataFileMetaCacheManager(dataFile)
        val fiberBitSet = new BitSet(fileMeta.getGroupCount * fileMeta.getFieldCount)
        fiberSet.foreach(fiber =>
          fiberBitSet.set(fiber.columnIndex + fileMeta.getFieldCount * fiber.rowGroupId))
        FiberCacheStatus(dataFile.path, fiberBitSet, fileMeta.getGroupCount, fileMeta.getFieldCount)
    }.toSeq

    CacheStatusSerDe.serialize(statusRawData)
  }

  def cacheStats: CacheStats = cacheBackend.cacheStats

  def cacheSize: Long = cacheBackend.cacheSize

  def cacheCount: Long = cacheBackend.cacheCount

  // Used by test suite
  private[filecache] def pendingCount: Int = cacheBackend.pendingFiberCount

  // A description of this FiberCacheManager for debugging.
  def toDebugString: String = {
    s"FiberCacheManager Statistics: { cacheCount=${cacheBackend.cacheCount}, " +
        s"usedMemory=${Utils.bytesToString(cacheSize)}, ${cacheStats.toDebugString} }"
  }
}

private[oap] object DataFileMetaCacheManager extends Logging {
  type ENTRY = DataFile

  private val _cacheSize: AtomicLong = new AtomicLong(0)

  def cacheSize: Long = _cacheSize.get()

  private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .expireAfterAccess(1000, TimeUnit.SECONDS) // auto expire after 1000 seconds.
      .removalListener(new RemovalListener[ENTRY, DataFileMeta]() {
        override def onRemoval(n: RemovalNotification[ENTRY, DataFileMeta])
        : Unit = {
          logDebug(s"Evicting Data File Meta ${n.getKey.path}")
          _cacheSize.addAndGet(-n.getValue.len)
          n.getValue.close
        }
      })
      .build[ENTRY, DataFileMeta](new CacheLoader[ENTRY, DataFileMeta]() {
        override def load(entry: ENTRY)
        : DataFileMeta = {
          logDebug(s"Loading Data File Meta ${entry.path}")
          val meta = entry.getDataFileMeta()
          _cacheSize.addAndGet(meta.len)
          meta
        }
      })

  def apply(fiberCache: DataFile): DataFileMeta = {
    cache.get(fiberCache)
  }
}

object FiberLockManager {
  private val lockMap = new ConcurrentHashMap[Fiber, ReentrantReadWriteLock]()
  def getFiberLock(fiber: Fiber): ReentrantReadWriteLock = {
    var lock = lockMap.get(fiber)
    if (lock == null) {
      val newLock = new ReentrantReadWriteLock()
      val prevLock = lockMap.putIfAbsent(fiber, newLock)
      lock = if (prevLock == null) newLock else prevLock
    }
    lock
  }

  def removeFiberLock(fiber: Fiber): Unit = {
    lockMap.remove(fiber)
  }
}
