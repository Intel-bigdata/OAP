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

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.executor.custom.CustomManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

// TODO need to register within the SparkContext
class OapFiberCacheHeartBeatMessager extends CustomManager with Logging {
  override def status(conf: SparkConf): String = {
    FiberCacheManager.status
  }
}

private[filecache] class CacheGuardian(maxMemory: Long) extends Thread with Logging {

  private val _pendingFiberSize: AtomicLong = new AtomicLong(0)

  private val removalPendingQueue = new LinkedBlockingQueue[(Fiber, FiberCache)]()

  // Tell if guardian thread is trying to remove one Fiber.
  @volatile private var bRemoving: Boolean = false

  def pendingSize: Int = if (bRemoving) {
    removalPendingQueue.size() + 1
  } else {
    removalPendingQueue.size()
  }

  def addRemovalFiber(fiber: Fiber, fiberCache: FiberCache): Unit = {
    _pendingFiberSize.addAndGet(fiberCache.size())
    removalPendingQueue.offer(fiber, fiberCache)
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
        // TODO: Make log more readable
        _pendingFiberSize.addAndGet(-fiberCache.size())
        logDebug(s"Fiber removed successfully. Fiber: $fiber")
      }
      bRemoving = false
    }
  }
}

/**
 * Fiber Cache Manager
 *
 * TODO: change object to class for better initialization
 */
object FiberCacheManager extends Logging {

  private val GUAVA_CACHE = "guava"
  private val SIMPLE_CACHE = "simple"
  private val DEFAULT_CACHE_STRATEGY = GUAVA_CACHE

  private val cacheBackend: OapCache = {
    val sparkEnv = SparkEnv.get
    assert(sparkEnv != null, "Oap can't run without SparkContext")
    val cacheName = sparkEnv.conf.get("spark.oap.cache.strategy", DEFAULT_CACHE_STRATEGY)
    if (cacheName.equals(GUAVA_CACHE)) {
      new GuavaOapCache(MemoryManager.cacheMemory, MemoryManager.cacheGuardianMemory)
    } else if (cacheName.equals(SIMPLE_CACHE)) {
      new SimpleOapCache()
    } else {
      throw new OapException("Unsupported cache strategy")
    }
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
    if (cacheBackend.getIfPresent(fiber) != null) cacheBackend.invalidate(fiber)
  }

  // TODO: test case, consider data eviction, try not use DataFileHandle which my be costly
  private[filecache] def status: String = {
    logDebug(s"Reporting ${cacheBackend.cacheCount} fibers to the master")
    val dataFibers = cacheBackend.getFibers.collect {
      case fiber: DataFiber => fiber
    }

    val statusRawData = dataFibers.groupBy(_.file).map {
      case (dataFile, fiberSet) =>
        val fileMeta = DataFileHandleCacheManager(dataFile).asInstanceOf[OapDataFileHandle]
        val fiberBitSet = new BitSet(fileMeta.groupCount * fileMeta.fieldCount)
        fiberSet.foreach(fiber =>
          fiberBitSet.set(fiber.columnIndex + fileMeta.fieldCount * fiber.rowGroupId))
        FiberCacheStatus(dataFile.path, fiberBitSet, fileMeta)
    }.toSeq

    CacheStatusSerDe.serialize(statusRawData)
  }

  def cacheStats: CacheStats = cacheBackend.cacheStats

  def cacheSize: Long = cacheBackend.cacheSize

  // Used by test suite
  private[filecache] def pendingSize: Int = cacheBackend.pendingSize

  // A description of this FiberCacheManager for debugging.
  def toDebugString: String = {
    s"FiberCacheManager Statistics: { cacheCount=${cacheBackend.cacheCount}, " +
        s"usedMemory=${Utils.bytesToString(cacheSize)}, ${cacheStats.toDebugString} }"
  }
}

private[oap] object DataFileHandleCacheManager extends Logging {
  type ENTRY = DataFile

  private val _cacheSize: AtomicLong = new AtomicLong(0)

  def cacheSize: Long = _cacheSize.get()

  private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .expireAfterAccess(1000, TimeUnit.SECONDS) // auto expire after 1000 seconds.
      .removalListener(new RemovalListener[ENTRY, DataFileHandle]() {
        override def onRemoval(n: RemovalNotification[ENTRY, DataFileHandle])
        : Unit = {
          logDebug(s"Evicting Data File Handle ${n.getKey.path}")
          _cacheSize.addAndGet(-n.getValue.len)
          n.getValue.close
        }
      })
      .build[ENTRY, DataFileHandle](new CacheLoader[ENTRY, DataFileHandle]() {
        override def load(entry: ENTRY)
        : DataFileHandle = {
          logDebug(s"Loading Data File Handle ${entry.path}")
          val handle = entry.createDataFileHandle()
          _cacheSize.addAndGet(handle.len)
          handle
        }
      })

  def apply[T <: DataFileHandle](fiberCache: DataFile): T = {
    cache.get(fiberCache).asInstanceOf[T]
  }
}

private[oap] trait Fiber {
  def fiber2Data(conf: Configuration): FiberCache
}

private[oap]
case class DataFiber(file: DataFile, columnIndex: Int, rowGroupId: Int) extends Fiber {
  override def fiber2Data(conf: Configuration): FiberCache =
    file.getFiberData(rowGroupId, columnIndex, conf)

  override def hashCode(): Int = (file.path + columnIndex + rowGroupId).hashCode

  override def equals(obj: Any): Boolean = obj match {
    case another: DataFiber =>
      another.columnIndex == columnIndex &&
        another.rowGroupId == rowGroupId &&
        another.file.path.equals(file.path)
    case _ => false
  }

  override def toString: String = {
    s"type: DataFiber rowGroup: $rowGroupId column: $columnIndex\n\tfile: ${file.path}"
  }
}

private[oap]
case class BTreeFiber(
    getFiberData: () => FiberCache,
    file: String,
    section: Int,
    idx: Int) extends Fiber {
  override def fiber2Data(conf: Configuration): FiberCache = getFiberData()

  override def hashCode(): Int = (file + section + idx).hashCode

  override def equals(obj: Any): Boolean = obj match {
    case another: BTreeFiber =>
      another.section == section &&
        another.idx == idx &&
        another.file.equals(file)
    case _ => false
  }

  override def toString: String = {
    s"type: BTreeFiber section: $section idx: $idx\n\tfile: $file"
  }
}

private[oap]
case class BitmapFiber(
    getFiberData: () => FiberCache,
    file: String,
    // "0" means no split sections within file.
    sectionIdxOfFile: Int,
    // "0" means no smaller loading units.
    loadUnitIdxOfSection: Int) extends Fiber {
  override def fiber2Data(conf: Configuration): FiberCache = getFiberData()

  override def hashCode(): Int = (file + sectionIdxOfFile + loadUnitIdxOfSection).hashCode

  override def equals(obj: Any): Boolean = obj match {
    case another: BitmapFiber =>
      another.sectionIdxOfFile == sectionIdxOfFile &&
        another.loadUnitIdxOfSection == loadUnitIdxOfSection &&
        another.file.equals(file)
    case _ => false
  }

  override def toString: String = {
    s"type: BitmapFiber section: $sectionIdxOfFile idx: $loadUnitIdxOfSection\n\tfile: $file"
  }
}

private[oap] case class TestFiber(getData: () => FiberCache, name: String) extends Fiber {
  override def fiber2Data(conf: Configuration): FiberCache = getData()

  override def hashCode(): Int = name.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case another: TestFiber => name.equals(another.name)
    case _ => false
  }

  override def toString: String = {
    s"type: TestFiber name: $name"
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
