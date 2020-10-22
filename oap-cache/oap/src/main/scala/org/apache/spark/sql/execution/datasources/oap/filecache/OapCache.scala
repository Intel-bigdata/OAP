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

import java.io.File
import java.nio.{ByteBuffer, DirectByteBuffer}
import java.util.concurrent.{ConcurrentHashMap, Executors, LinkedBlockingQueue}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.{Condition, ReentrantLock}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Success

import com.google.common.cache._
import com.google.common.hash._
import com.intel.oap.common.unsafe.VMEMCacheJNI
import org.apache.arrow.plasma
import org.apache.arrow.plasma.exceptions.{DuplicateObjectException, PlasmaClientException}
import sun.nio.ch.DirectBuffer

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.execution.datasources.{CacheMetaInfoValue, ExternalDBClient, ExternalDBClientFactory, OapException, StoreCacheMetaInfo}
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberType.FiberType
import org.apache.spark.sql.execution.datasources.oap.utils.PersistentMemoryConfigUtils
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

private[filecache] class MultiThreadCacheGuardian(maxMemory: Long) extends CacheGuardian(maxMemory)
  with Logging {

  // pendingFiberSize and pendingFiberCapacity are different. pendingFiberSize used to
  // show the pending size to user, however pendingFiberCapacity is used to record the
  // actual used memory and log warn when exceed the maxMemory.
  private val freeThreadNum = if (SparkEnv.get != null) {
    SparkEnv.get.conf.get(OapConf.OAP_CACHE_GUARDIAN_FREE_THREAD_NUM)
  } else {
    OapConf.OAP_CACHE_GUARDIAN_FREE_THREAD_NUM.defaultValue.get
  }
  private val removalPendingQueues =
    new Array[LinkedBlockingQueue[(FiberId, FiberCache)]](freeThreadNum)
  for ( i <- 0 until freeThreadNum)
    removalPendingQueues(i) = new LinkedBlockingQueue[(FiberId, FiberCache)]()
  val roundRobin = new AtomicInteger(0)

  override def pendingFiberCount: Int = {
    var sum = 0
    removalPendingQueues.foreach(sum += _.size())
    sum
  }

  override def addRemovalFiber(fiber: FiberId, fiberCache: FiberCache): Unit = {
    _pendingFiberSize.addAndGet(fiberCache.size())
    // Record the occupied size
    _pendingFiberCapacity.addAndGet(fiberCache.getOccupiedSize())
    removalPendingQueues(roundRobin.addAndGet(1) % freeThreadNum)
      .offer((fiber, fiberCache))
    if (_pendingFiberCapacity.get() > maxMemory) {
      // TODO:change back logWarning
      logDebug("Fibers pending on removal use too much memory, " +
        s"current: ${_pendingFiberCapacity.get()}, max: $maxMemory")
    }
  }

  lazy val freeThreadPool = Executors.newFixedThreadPool(freeThreadNum)


  override def start(): Unit = {
    for (i <- 0 until freeThreadNum)
      freeThreadPool.execute(new freeThread(i))

    class freeThread(id: Int) extends Runnable {
      override def run(): Unit = {
        val queue = removalPendingQueues(id)
        while (true) {
          val fiberCache = queue.take()._2
          releaseFiberCache(fiberCache)
        }
      }
    }
  }

  private def releaseFiberCache(cache: FiberCache): Unit = {
    val fiberId = cache.fiberId
    logDebug(s"Removing fiber: $fiberId")
    // Block if fiber is in use.
    if (!cache.tryDispose()) {
      logDebug(s"Waiting fiber to be released timeout. Fiber: $fiberId")
      removalPendingQueues(roundRobin.addAndGet(1) % freeThreadNum)
        .offer((fiberId, cache))
      if (_pendingFiberCapacity.get() > maxMemory) {
        // TODO:change back
        logDebug("Fibers pending on removal use too much memory, " +
          s"current: ${_pendingFiberCapacity.get()}, max: $maxMemory")
      }
    } else {
      _pendingFiberSize.addAndGet(-cache.size())
      _pendingFiberCapacity.addAndGet(-cache.getOccupiedSize())
      // TODO: Make log more readable
      logDebug(s"Fiber removed successfully. Fiber: $fiberId")
    }
  }
}

private[filecache] class CacheGuardian(maxMemory: Long) extends Thread with Logging {

  // pendingFiberSize and pendingFiberCapacity are different. pendingFiberSize used to
  // show the pending size to user, however pendingFiberCapacity is used to record the
  // actual used memory and log warn when exceed the maxMemory.
  protected val _pendingFiberSize: AtomicLong = new AtomicLong(0)
  protected val _pendingFiberCapacity: AtomicLong = new AtomicLong(0)

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

private[filecache] object OapCache extends Logging {
  def plasmaServerDetect(): Boolean = {
    val command = "ps -ef" #| "grep plasma"
    val plasmaServerStatus = command.!!
    if (plasmaServerStatus.indexOf("plasma-store-server") == -1) {
      logWarning("External cache strategy requires plasma-store-server launched, " +
        "failed to detect plasma-store-server, will fallback to simpleCache.")
      return false
    }
    true
  }
  def cacheFallBackDetect(sparkEnv: SparkEnv,
                          fallBackEnabled: Boolean = true,
                          fallBackRes: Boolean = true): Boolean = {
    if (fallBackEnabled == false) return fallBackRes
    val conf = sparkEnv.conf
    var numaId = conf.getInt("spark.executor.numa.id", -1)
    val executorIdStr = sparkEnv.executorId
    scala.util.Try(executorIdStr.toInt) match {
      case Success(_) => logDebug("valid executor id for numa binding.") ;
      case _ =>
        logWarning("invalid executor id for numa binding.")
        return false
    }
    val executorId = executorIdStr.toInt
    var map: mutable.HashMap[Int, String] = mutable.HashMap.empty
    try {
      map = PersistentMemoryConfigUtils.parseConfig(conf)
    } catch {
      case e: OapException =>
        logWarning("cacheFallBackDetect: execption when parse config" + e.getMessage)
        return false
    }
    if (numaId == -1) {
      logWarning(s"Executor ${executorId} is not bind with NUMA. It would be better to bind " +
        s"executor with NUMA when cache data to Intel Optane DC persistent memory.")
      numaId = executorId % PersistentMemoryConfigUtils.totalNumaNode(conf)
    }
    val initialPath = map.get(numaId).get
    val initialSizeStr =
      if (conf.getOption(OapConf.OAP_FIBERCACHE_PERSISTENT_MEMORY_INITIAL_SIZE.key).isDefined) {
        conf.get(OapConf.OAP_FIBERCACHE_PERSISTENT_MEMORY_INITIAL_SIZE).trim
      } else {
        conf.get(OapConf.OAP_FIBERCACHE_PERSISTENT_MEMORY_INITIAL_SIZE_BK).trim
      }
    val initialSize = Utils.byteStringAsBytes(initialSizeStr)

    val file: File = new File(initialPath)
    if(!file.exists()) {
       return false
    }
    if(initialSize > file.getUsableSpace) {
      logWarning(s"Required initialSize larger than usable space, will use largest usable space")
      return false
    }
    true
  }

  def apply(sparkEnv: SparkEnv, cacheMemory: Long,
            cacheGuardianMemory: Long, fiberType: FiberType): OapCache = {
    apply(sparkEnv, OapConf.OAP_FIBERCACHE_STRATEGY, cacheMemory, cacheGuardianMemory, fiberType)
  }

  def apply(sparkEnv: SparkEnv, configEntry: ConfigEntry[String],
            cacheMemory: Long, cacheGuardianMemory: Long, fiberType: FiberType): OapCache = {
    val conf = sparkEnv.conf
    val oapCacheOpt = conf.get(
      configEntry.key,
      configEntry.defaultValue.get).toLowerCase
    val memoryManagerOpt =
      if (sparkEnv.conf.getOption(OapConf.OAP_FIBERCACHE_MEMORY_MANAGER.key).isDefined) {
        sparkEnv.conf.get(OapConf.OAP_FIBERCACHE_MEMORY_MANAGER.key, "offheap").toLowerCase
      } else {
        sparkEnv.conf.get(OapConf.OAP_FIBERCACHE_MEMORY_MANAGER_BK.key, "offheap").toLowerCase
      }
    logInfo(s"${memoryManagerOpt}")
    val fallBackEnabled = conf.get(OapConf.OAP_CACHE_BACKEND_FALLBACK_ENABLED.key,
      "true").toLowerCase
    val fallBackRes = conf.get(OapConf.OAP_TEST_CACHE_BACKEND_FALLBACK_RES.key,
      "true").toLowerCase

    oapCacheOpt match {
      case "external" =>
        if (plasmaServerDetect()) new ExternalCache(fiberType)
        else new SimpleOapCache()
      case "guava" =>
        if (cacheFallBackDetect(sparkEnv, fallBackEnabled.toBoolean, fallBackRes.toBoolean)) {
          new GuavaOapCache(cacheMemory, cacheGuardianMemory, fiberType)
        }
        else {
          if (oapCacheOpt.equals("guava") && memoryManagerOpt.equals("offheap")) {
            new GuavaOapCache(cacheMemory, cacheGuardianMemory, fiberType)
          }
          else new SimpleOapCache()
        }
      case "noevict" =>
        if (cacheFallBackDetect(sparkEnv, fallBackEnabled.toBoolean, fallBackRes.toBoolean)) {
          new NoEvictPMCache(cacheMemory, cacheGuardianMemory, fiberType)
        }
        else new SimpleOapCache()
      case "vmem" =>
        if (cacheFallBackDetect(sparkEnv, fallBackEnabled.toBoolean, fallBackRes.toBoolean)) {
          new VMemCache(fiberType)
        }
        else new SimpleOapCache()
      case _ =>
        throw new UnsupportedOperationException(
          s"The cache backend: ${oapCacheOpt} is not supported now")
    }
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
  def dataCacheCount: Long
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
      case orcChunk: OrcBinaryFiberId => orcChunk.doCache()
      case VectorDataFiberId(file, columnIndex, rowGroupId) =>
        file.cache(rowGroupId, columnIndex, fiber)
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

class NoEvictPMCache(pmSize: Long,
                      cacheGuardianMemory: Long,
                      fiberType: FiberType) extends OapCache with Logging {
  // We don't bother the memory use of Simple Cache
  private val cacheGuardian = new MultiThreadCacheGuardian(Int.MaxValue)
  private val _cacheSize: AtomicLong = new AtomicLong(0)
  private val _cacheCount: AtomicLong = new AtomicLong(0)
  private val cacheHitCount: AtomicLong = new AtomicLong(0)
  private val cacheMissCount: AtomicLong = new AtomicLong(0)

  val cacheMap : ConcurrentHashMap[FiberId, FiberCache] = new ConcurrentHashMap[FiberId, FiberCache]
  cacheGuardian.start()

  override def get(fiber: FiberId): FiberCache = {
    if (cacheMap.containsKey(fiber)) {
      cacheHitCount.getAndAdd(1)
      val fiberCache = cacheMap.get(fiber)
      fiberCache.occupy()
      fiberCache
    } else {
      cacheMissCount.getAndAdd(1)
      val fiberCache = cache(fiber)
      fiberCache.occupy()
      if (fiberCache.fiberData.source.equals(SourceEnum.DRAM)) {
        cacheGuardian.addRemovalFiber(fiber, fiberCache)
      } else {
        _cacheSize.addAndGet(fiberCache.size())
        _cacheCount.addAndGet(1)
        cacheMap.put(fiber, fiberCache)
      }
      fiberCache
    }
  }

  override def getIfPresent(fiber: FiberId): FiberCache = {
    if (cacheMap.contains(fiber)) {
      cacheMap.get(fiber)
    } else {
      null
    }
  }

  override def getFibers: Set[FiberId] = {
    cacheMap.keySet().asScala.toSet
  }

  override def invalidate(fiber: FiberId): Unit = {
    if (cacheMap.contains(fiber)) {
      cacheMap.remove(fiber)
    }
  }

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {
    fibers.foreach(invalidate)
  }

  override def cacheSize: Long = {_cacheSize.get()}

  override def cacheCount: Long = {_cacheCount.get()}

  override def cacheStats: CacheStats = {
    CacheStats(_cacheCount.get(), _cacheSize.get(), 0, 0, cacheGuardian.pendingFiberCount,
      cacheGuardian.pendingFiberSize, cacheHitCount.get(), cacheMissCount.get(),
      0, 0, 0,
      0, 0, 0, 0, 0)
  }

  override def dataCacheCount: Long = 0

  override def pendingFiberCount: Int = cacheGuardian.pendingFiberCount

  override def pendingFiberSize: Long = cacheGuardian.pendingFiberSize

  override def pendingFiberOccupiedSize: Long = cacheGuardian.pendingFiberOccupiedSize

  override def getCacheGuardian: CacheGuardian = cacheGuardian
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

  override def dataCacheCount: Long = 0

  override def pendingFiberCount: Int = cacheGuardian.pendingFiberCount

  override def pendingFiberSize: Long = cacheGuardian.pendingFiberSize

  override def pendingFiberOccupiedSize: Long = cacheGuardian.pendingFiberOccupiedSize

  override def getCacheGuardian: CacheGuardian = cacheGuardian
}

class VMemCache(fiberType: FiberType) extends OapCache with Logging {
  private def emptyDataFiber(fiberLength: Long): FiberCache =
    OapRuntime.getOrCreate.fiberCacheManager.getEmptyDataFiberCache(fiberLength)

  private val cacheHitCount: AtomicLong = new AtomicLong(0)
  private val cacheMissCount: AtomicLong = new AtomicLong(0)
  private val cacheTotalGetTime: AtomicLong = new AtomicLong(0)
  private var cacheTotalCount: Long = 0
  private var cacheEvictCount: Long = 0
  private var cacheTotalSize: Long = 0
  // We don't bother the memory use of Simple Cache
  private val cacheGuardian = new MultiThreadCacheGuardian(Int.MaxValue)
  cacheGuardian.start()

  @volatile private var initialized = false
  private val lock = new Object
  private val conf = SparkEnv.get.conf
  private val initialSizeStr =
    if (conf.getOption(OapConf.OAP_FIBERCACHE_PERSISTENT_MEMORY_INITIAL_SIZE.key).isDefined) {
      conf.get(OapConf.OAP_FIBERCACHE_PERSISTENT_MEMORY_INITIAL_SIZE).trim
    } else {
      conf.get(OapConf.OAP_FIBERCACHE_PERSISTENT_MEMORY_INITIAL_SIZE_BK).trim
    }
  private val vmInitialSize = Utils.byteStringAsBytes(initialSizeStr)
  require(!(conf.getBoolean(OapConf.OAP_ENABLE_DATA_FIBER_CACHE_COMPRESSION.key,
    OapConf.OAP_ENABLE_DATA_FIBER_CACHE_COMPRESSION.defaultValue.get) ||
    conf.getBoolean(OapConf.OAP_ENABLE_DATA_FIBER_CACHE_COMPRESSION_BK.key,
      OapConf.OAP_ENABLE_DATA_FIBER_CACHE_COMPRESSION_BK.defaultValue.get)),
    "Vmemcache strategy doesn't support fiber cache compression currently, " +
    "please try other strategy.")
  require(vmInitialSize > 0, "AEP initial size must be greater than zero")
  def initializeVMEMCache(): Unit = {
    if (!initialized) {
      lock.synchronized {
        if (!initialized) {
          val sparkEnv = SparkEnv.get
          val conf = sparkEnv.conf
          // The NUMA id should be set when the executor process start up. However, Spark don't
          // support NUMA binding currently.
          var numaId = conf.getInt("spark.executor.numa.id", -1)
          val executorId = sparkEnv.executorId.toInt
          val map = PersistentMemoryConfigUtils.parseConfig(conf)
          if (numaId == -1) {
            logWarning(s"Executor ${executorId} is not bind with NUMA. It would be better" +
              s" to bind executor with NUMA when cache data to Intel Optane DC persistent memory.")
            // Just round the executorId to the total NUMA number.
            // TODO: improve here
            numaId = executorId % PersistentMemoryConfigUtils.totalNumaNode(conf)
          }
          val initialPath = map.get(numaId).get
          val fullPath = Utils.createTempDir(initialPath + File.separator + executorId)

          require(fullPath.isDirectory(), "VMEMCache initialize path must be a directory")
          val success = VMEMCacheJNI.initialize(fullPath.getCanonicalPath, vmInitialSize);
          if (success != 0) {
            throw new SparkException("Failed to call VMEMCacheJNI.initialize")
          }
          logInfo(s"Executors ${executorId}: VMEMCache initialize path:" +
            s" ${fullPath.getCanonicalPath}, size: ${1.0 * vmInitialSize / 1024 / 1024 / 1024}GB")
          initialized = true
        }
      }
    }
  }

  initializeVMEMCache()

  var fiberSet = scala.collection.mutable.Set[FiberId]()
  override def get(fiber: FiberId): FiberCache = {
    val fiberKey = fiber.toFiberKey()
    val startTime = System.currentTimeMillis()
    val res = VMEMCacheJNI.exist(fiberKey.getBytes(), null, 0, fiberKey.getBytes().length)
    logDebug(s"vmemcache.exist return $res ," +
      s" takes ${System.currentTimeMillis() - startTime} ms")
    if (res <= 0) {
      cacheMissCount.addAndGet(1)
      val fiberCache = cache(fiber)
      fiberSet.add(fiber)
      incFiberCountAndSize(fiber, 1, fiberCache.size())
      fiberCache.occupy()
      decFiberCountAndSize(fiber, 1, fiberCache.size())
      cacheGuardian.addRemovalFiber(fiber, fiberCache)
      fiberCache
    } else { // cache hit
      cacheHitCount.addAndGet(1)
      val length = res
      val fiberCache = emptyDataFiber(length)
      val startTime = System.currentTimeMillis()
      val get = VMEMCacheJNI.getNative(fiberKey.getBytes(), null,
        0, fiberKey.length, fiberCache.getBaseOffset, 0, fiberCache.size().toInt)
      logDebug(s"second getNative require ${length} bytes. " +
        s"returns $get bytes, takes ${System.currentTimeMillis() - startTime} ms")
      fiberCache.fiberId = fiber
      fiberCache.occupy()
      cacheGuardian.addRemovalFiber(fiber, fiberCache)
      fiberCache
    }
  }

  override def getIfPresent(fiber: FiberId): FiberCache = null

  override def getFibers: Set[FiberId] = {
    val tmpFiberSet = fiberSet
    // todo: we can implement a VmemcacheJNI.exist(keys:byte[][])
    for(fibId <- tmpFiberSet) {
      val fiberKey = fibId.toFiberKey()
      val get = VMEMCacheJNI.exist(fiberKey.getBytes(), null, 0, fiberKey.getBytes().length)
      if(get <=0 ) {
        fiberSet.remove(fibId)
        logDebug(s"$fiberKey is removed.")
      } else {
        logDebug(s"$fiberKey is still stored.")
      }
    }
    fiberSet.toSet
  }

  override def invalidate(fiber: FiberId): Unit = {}

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {}

  override def cacheSize: Long = 0

  override def cache(fiberId: FiberId): FiberCache = {
    val fiber = super.cache(fiberId)
    VMEMCacheJNI.putNative(fiberId.toFiberKey().getBytes(), null, 0,
      fiberId.toFiberKey().length, fiber.getBaseOffset,
      0, fiber.getOccupiedSize().toInt)
    fiber
  }

  override def cacheStats: CacheStats = {
    val status = new Array[Long](3)
    VMEMCacheJNI.status(status)
    cacheEvictCount = status(0)
    cacheTotalCount = status(1)
    cacheTotalSize = status(2)
    logDebug(s"Current status is evict:$cacheEvictCount," +
      s" count:$cacheTotalCount, size:$cacheTotalSize")

    if (fiberType == FiberType.INDEX) {
      CacheStats(
        0, 0,
        cacheTotalCount, cacheTotalSize,
        cacheGuardian.pendingFiberCount, // pendingFiberCount
        cacheGuardian.pendingFiberSize, // pendingFiberSize
        0, 0, 0, 0, 0, // For index cache, the data fiber metrics should always be zero
        cacheHitCount.get(), // indexFiberHitCount
        cacheMissCount.get(), // indexFiberMissCount
        cacheHitCount.get(), // indexFiberLoadCount
        cacheTotalGetTime.get(), // indexTotalLoadTime
        cacheEvictCount // indexEvictionCount
      )
    } else {
      CacheStats(
        cacheTotalCount, cacheTotalSize,
        0, 0,
        cacheGuardian.pendingFiberCount, // pendingFiberCount
        cacheGuardian.pendingFiberSize, // pendingFiberSize
        cacheHitCount.get(), // dataFiberHitCount
        cacheMissCount.get(), // dataFiberMissCount
        cacheHitCount.get(), // dataFiberLoadCount
        cacheTotalGetTime.get(), // dataTotalLoadTime
        cacheEvictCount, // dataEvictionCount
        0, 0, 0, 0, 0) // For data cache, the index fiber metrics should always be zero
    }
  }

  override def cacheCount: Long = 0

  override def pendingFiberCount: Int = 0

  override def cleanUp: Unit = {
    super.cleanUp
  }

  override def dataCacheCount: Long = 0

  override def pendingFiberSize: Long = cacheGuardian.pendingFiberSize

  override def pendingFiberOccupiedSize: Long = cacheGuardian.pendingFiberOccupiedSize

  override def getCacheGuardian: CacheGuardian = cacheGuardian
}

class GuavaOapCache(
    cacheMemory: Long,
    cacheGuardianMemory: Long,
    fiberType: FiberType)
    extends OapCache with Logging {

  // TODO: CacheGuardian can also track cache statistics periodically
  private val cacheGuardian = new CacheGuardian(cacheGuardianMemory)
  cacheGuardian.start()

  private val MAX_WEIGHT = cacheMemory
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
      value.getOccupiedSize().toInt
    }
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
          "Load missed fiber took %s. Fiber: %s. length: %s".format(
            Utils.getUsedTimeNs(startLoadingTime), key, fiberCache.size()))
        _cacheSize.addAndGet(fiberCache.size())
        fiberCache
      }
    })


  override def get(fiber: FiberId): FiberCache = {
    val readLock = OapRuntime.getOrCreate.fiberCacheManager.getFiberLock(fiber).readLock()
    readLock.lock()
    try {
      val fiberCache = cacheInstance.get(fiber)
      // Avoid loading a fiber larger than MAX_WEIGHT / CONCURRENCY_LEVEL
      assert(fiberCache.size() <= MAX_WEIGHT / CONCURRENCY_LEVEL,
        s"Failed to cache fiber(${Utils.bytesToString(fiberCache.size())}) " +
          s"with cache's MAX_WEIGHT" +
          s"(${Utils.bytesToString(MAX_WEIGHT)}) / $CONCURRENCY_LEVEL")
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
    fibers.foreach(invalidate)
  }

  override def cacheSize: Long = _cacheSize.get()

  override def cacheStats: CacheStats = {
    val stats = cacheInstance.stats()
    if (fiberType == FiberType.INDEX) {
      CacheStats(
        dataFiberCount.get(), dataFiberSize.get(),
        indexFiberCount.get(), indexFiberSize.get(),
        pendingFiberCount, cacheGuardian.pendingFiberSize,
        0, 0, 0, 0, 0, // For index cache, the data fiber metrics should always be zero
        stats.hitCount(),
        stats.missCount(),
        stats.loadCount(),
        stats.totalLoadTime(),
        stats.evictionCount()
      )
    } else {
      CacheStats(
        dataFiberCount.get(), dataFiberSize.get(),
        indexFiberCount.get(), indexFiberSize.get(),
        pendingFiberCount, cacheGuardian.pendingFiberSize,
        stats.hitCount(),
        stats.missCount(),
        stats.loadCount(),
        stats.totalLoadTime(),
        stats.evictionCount(),
        0, 0, 0, 0, 0 // For data cache, the index fiber metrics should always be zero
      )
    }
  }

  override def cacheCount: Long = cacheInstance.size()

  override def pendingFiberCount: Int = cacheGuardian.pendingFiberCount

  override def pendingFiberSize: Long = cacheGuardian.pendingFiberSize

  override def pendingFiberOccupiedSize: Long = cacheGuardian.pendingFiberOccupiedSize

  override def getCacheGuardian: CacheGuardian = cacheGuardian

  override def cleanUp(): Unit = {
    super.cleanUp()
    cacheInstance.cleanUp()
  }

  override def dataCacheCount: Long = {
    cacheInstance.asMap().keySet().asScala
      .count(fiber => fiber.isInstanceOf[DataFiberId] || fiber.isInstanceOf[TestDataFiberId])
  }
}

class MixCache(dataCacheMemory: Long,
               indexCacheMemory: Long,
               dataCacheGuardianMemory: Long,
               indexCacheGuardianMemory: Long,
               separation: Boolean,
               sparkEnv: SparkEnv)
  extends OapCache with Logging {

  private val (dataCacheBackend, indexCacheBackend) = init()

  private def init(): (OapCache, OapCache) = {
    if (!separation) {
      val dataCacheBackend = OapCache(sparkEnv, OapConf.OAP_MIX_DATA_CACHE_BACKEND,
        dataCacheMemory, dataCacheGuardianMemory, FiberType.DATA);
      val indexCacheBackend = OapCache(sparkEnv, OapConf.OAP_MIX_INDEX_CACHE_BACKEND,
        indexCacheMemory, indexCacheGuardianMemory, FiberType.INDEX);
      (dataCacheBackend, indexCacheBackend)
    } else {
      val dataCacheBackend = new GuavaOapCache(dataCacheMemory, dataCacheGuardianMemory,
        FiberType.DATA)
      val indexCacheBackend = new GuavaOapCache(indexCacheMemory, indexCacheGuardianMemory,
        FiberType.INDEX)
      (dataCacheBackend, indexCacheBackend)
    }
  }

  override def get(fiberId: FiberId): FiberCache = {
    if (fiberId.isInstanceOf[DataFiberId] ||
      fiberId.isInstanceOf[TestDataFiberId]) {
      val fiberCache = dataCacheBackend.get(fiberId)
      fiberCache
    } else if (fiberId.isInstanceOf[BTreeFiberId] ||
      fiberId.isInstanceOf[BitmapFiberId] ||
      fiberId.isInstanceOf[TestIndexFiberId]) {
      val fiberCache = indexCacheBackend.get(fiberId)
      fiberCache
    } else {
      throw new OapException(s"not support fiber type $fiberId")
    }
  }

  override def getIfPresent(fiber: FiberId): FiberCache =
    if (fiber.isInstanceOf[BTreeFiberId] ||
      fiber.isInstanceOf[BitmapFiberId] ||
      fiber.isInstanceOf[TestIndexFiberId]) {
      indexCacheBackend.getIfPresent(fiber)
    } else {
      dataCacheBackend.getIfPresent(fiber)
    }

  override def getFibers: Set[FiberId] = {
    dataCacheBackend.getFibers ++ indexCacheBackend.getFibers
  }

  override def invalidate(fiber: FiberId): Unit =
    if (fiber.isInstanceOf[BTreeFiberId] ||
      fiber.isInstanceOf[BitmapFiberId] ||
      fiber.isInstanceOf[TestIndexFiberId]) {
      indexCacheBackend.invalidate(fiber)
    } else {
      dataCacheBackend.invalidate(fiber)
    }

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {
    fibers.foreach(invalidate)
  }

  override def cacheSize: Long = {
    dataCacheBackend.cacheSize + indexCacheBackend.cacheSize
  }

  override def cacheStats: CacheStats = {
    dataCacheBackend.cacheStats + indexCacheBackend.cacheStats
  }

  override def cacheCount: Long = {
    dataCacheBackend.cacheCount + indexCacheBackend.cacheCount
  }

  override def dataCacheCount: Long = {
    dataCacheBackend.dataCacheCount + indexCacheBackend.dataCacheCount
  }

  override def pendingFiberCount: Int = {
    dataCacheBackend.getCacheGuardian.pendingFiberCount +
      indexCacheBackend.getCacheGuardian.pendingFiberCount
  }

  override def pendingFiberSize: Long = {
    dataCacheBackend.getCacheGuardian.pendingFiberSize +
      indexCacheBackend.getCacheGuardian.pendingFiberSize
  }

  override def pendingFiberOccupiedSize: Long = {
    dataCacheBackend.getCacheGuardian.pendingFiberOccupiedSize +
      indexCacheBackend.getCacheGuardian.pendingFiberOccupiedSize
  }

  override def getCacheGuardian: CacheGuardian = {
    // this method is only used in VectorizedCacheReader
    // So we get from dataCacheBackend
    dataCacheBackend.getCacheGuardian
  }

  override def cleanUp: Unit = {
    dataCacheBackend.cleanUp()
    indexCacheBackend.cleanUp()
  }
}

class ExternalCache(fiberType: FiberType) extends OapCache with Logging {
  private val conf = SparkEnv.get.conf
  private val externalStoreCacheSocket: String = "/tmp/plasmaStore"
  private var cacheInit: Boolean = false
  private var externalDBClient: ExternalDBClient = null

  if (SparkEnv.get.conf.get(OapConf.OAP_EXTERNAL_CACHE_METADB_ENABLED) == true) {
    externalDBClient = ExternalDBClientFactory.getDBClientInstance(SparkEnv.get)
  }

  def init(): Unit = {
    if (!cacheInit) {
      try {
        System.loadLibrary("plasma_java")
        cacheInit = true
      } catch {
        case e: Exception => logError(s"load plasma jni lib failed " + e.getMessage)
      }
    }
  }

  init()

  private val cacheHitCount: AtomicLong = new AtomicLong(0)
  private val cacheMissCount: AtomicLong = new AtomicLong(0)
  private val cacheTotalGetTime: AtomicLong = new AtomicLong(0)
  private var cacheTotalCount: AtomicLong = new AtomicLong(0)
  private var cacheEvictCount: AtomicLong = new AtomicLong(0)
  private var cacheTotalSize: AtomicLong = new AtomicLong(0)

  private def emptyDataFiber(fiberLength: Long): FiberCache =
    OapRuntime.getOrCreate.fiberCacheManager.getEmptyDataFiberCache(fiberLength)

  var fiberSet = scala.collection.mutable.Set[FiberId]()
  val cacheReadOnlyEnbale = conf.get(OapConf.OAP_EXTERNAL_CACHE_READ_ONLY_ENABLE) &&
    conf.get(OapConf.OAP_EXTERNAL_CACHE_READ_ONLY_ENABLED)
  val clientPoolSize =
    if (conf.getOption(OapConf.OAP_EXTERNAL_CACHE_CLIENT_POOL_SIZE.key).isDefined) {
      conf.get(OapConf.OAP_EXTERNAL_CACHE_CLIENT_POOL_SIZE)
    } else {
      conf.get(OapConf.OAP_EXTERNAL_CACHE_CLIENT_POOL_SIZE_BK)
    }
  val clientRoundRobin = new AtomicInteger(0)
  val plasmaClientPool = new Array[ plasma.PlasmaClient](clientPoolSize)
  for ( i <- 0 until clientPoolSize) {
    try {
      plasmaClientPool(i) = new plasma.PlasmaClient(externalStoreCacheSocket, "", 0)
    } catch {
      case e: Exception =>
        logError(s"Error occurred when connecting to plasma server" + e.getMessage)
    }
  }

  val cacheGuardian = new MultiThreadCacheGuardian(Int.MaxValue)
  cacheGuardian.start()

  val hf: HashFunction = Hashing.murmur3_128()

  def hash(key: Array[Byte]): Array[Byte] = {
    val ret = new Array[Byte](20)
    hf.newHasher().putBytes(key).hash().writeBytesTo(ret, 0, 20)
    ret
  }

  def hash(key: String): Array[Byte] = {
    hash(key.getBytes())
  }

  def delete(fiberId: FiberId): Unit = {
    val objectId = hash(fiberId.toString)
    plasmaClientPool(clientRoundRobin.getAndAdd(1) % clientPoolSize).delete(objectId)
  }

  def contains(fiberId: FiberId): Boolean = {
    val objectId = hash(fiberId.toString)
    if (plasmaClientPool(clientRoundRobin.getAndAdd(1) % clientPoolSize).contains(objectId)) true
    else false
  }

  override def get(fiberId: FiberId): FiberCache = {
    logDebug(s"external cache get FiberId is ${fiberId}")
    val objectId = hash(fiberId.toString)
    if(contains(fiberId)) {
      var fiberCache : FiberCache = null
      try{
        logDebug(s"Cache hit, get from external cache.")
        val plasmaClient = plasmaClientPool(clientRoundRobin.getAndAdd(1) % clientPoolSize)
        val buf: ByteBuffer = plasmaClient.getObjAsByteBuffer(objectId, -1, false)
        cacheHitCount.addAndGet(1)
        fiberCache = emptyDataFiber(buf.capacity())
        fiberCache.fiberId = fiberId
        Platform.copyMemory(null, buf.asInstanceOf[DirectBuffer].address(),
          null, fiberCache.fiberData.baseOffset, buf.capacity())
        plasmaClient.release(objectId)
      }
      catch {
        case getException : plasma.exceptions.PlasmaGetException =>
          logWarning("Get exception: " + getException.getMessage)
          fiberCache = cache(fiberId)
          cacheMissCount.addAndGet(1)
      }
      fiberCache.occupy()
      cacheGuardian.addRemovalFiber(fiberId, fiberCache)
      fiberCache
    } else {
      if (cacheReadOnlyEnbale) {
        val fiberCache = cache(fiberId)
        cacheMissCount.addAndGet(1)
        fiberSet.add(fiberId)
        fiberCache.occupy()
        cacheGuardian.addRemovalFiber(fiberId, fiberCache)
        fiberCache
      } else {
        val fiberCache = super.cache(fiberId)
        fiberCache
      }
    }
  }

  def reportCacheMeta(fiberId: FiberId): Unit = {
    fiberId match {
      case binary: BinaryDataFiberId => binary
        .doReport(SparkEnv.get.blockManager.blockManagerId.host, externalDBClient)
      case vectorData: VectorDataFiberId => vectorData
        .doReport(SparkEnv.get.blockManager.blockManagerId.host, externalDBClient)
      case bitMapData: BitmapFiberId =>
        logWarning("Index cache is not support to report cache to external DB.")
      case bTreeData: BTreeFiberId =>
        logWarning("Index cache is not support to report cache to external DB.")
    }
  }

  override def cache(fiberId: FiberId): FiberCache = {
    val fiber = super.cache(fiberId)

    val objectId = hash(fiberId.toString)
    if( !contains(fiberId)) {
      val plasmaClient = plasmaClientPool(clientRoundRobin.getAndAdd(1) % clientPoolSize)
      try {
        val buf = plasmaClient.create(objectId, fiber.size().toInt)
        Platform.copyMemory(null, fiber.fiberData.baseOffset,
          null, buf.asInstanceOf[DirectBuffer].address(), fiber.size())
        plasmaClient.seal(objectId)
        plasmaClient.release(objectId)
      } catch {
        case e: DuplicateObjectException => logWarning(e.getMessage)
      }
    }
    if (SparkEnv.get.conf.get(OapConf.OAP_EXTERNAL_CACHE_METADB_ENABLED) == true) {
      reportCacheMeta(fiberId)
    }
    fiber
  }

  private val _cacheSize: AtomicLong = new AtomicLong(0)

  override def getIfPresent(fiber: FiberId): FiberCache = null

  override def getFibers: Set[FiberId] = {
    val set : Set[Array[Byte]] =
      plasmaClientPool(clientRoundRobin.getAndAdd(1) % clientPoolSize).list().asScala.toSet
    cacheTotalCount = new AtomicLong(set.size)
    logDebug("cache total size is " + cacheTotalCount)
    fiberSet.foreach( fiber =>
      if ( !set.contains(hash(fiber.toFiberKey()))) fiberSet.remove(fiber) )
    fiberSet.toSet
  }

  override def invalidate(fiber: FiberId): Unit = { }

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = { }

  override def cacheSize: Long = _cacheSize.get()

  override def cacheCount: Long = 0

  override def cacheStats: CacheStats = {
    val array = new Array[Long](4)
    // TODO:total size will be incorrect due to it's an external cache
    plasmaClientPool(clientRoundRobin.getAndAdd(1) % clientPoolSize).metrics(array)
    cacheTotalSize = new AtomicLong(array(3) + array(1))
    // Memory store and external store used size

    if (fiberType == FiberType.INDEX) {
      CacheStats(
        0, 0,
        cacheTotalCount.get(),
        cacheTotalSize.get(),
        cacheGuardian.pendingFiberCount, // pendingFiberCount
        cacheGuardian.pendingFiberSize, // pendingFiberSize
        0, 0, 0, 0, 0, // For index cache, the data fiber metrics should always be zero
        cacheHitCount.get(), // indexFiberHitCount
        cacheMissCount.get(), // indexFiberMissCount
        cacheHitCount.get(), // indexFiberLoadCount
        cacheTotalGetTime.get(), // indexTotalLoadTime
        cacheEvictCount.get() // indexEvictionCount
      )
    } else {
      CacheStats(
        cacheTotalCount.get(),
        cacheTotalSize.get(),
        0, 0,
        cacheGuardian.pendingFiberCount, // pendingFiberCount
        cacheGuardian.pendingFiberSize, // pendingFiberSize
        cacheHitCount.get(), // dataFiberHitCount
        cacheMissCount.get(), // dataFiberMissCount
        cacheHitCount.get(), // dataFiberLoadCount
        cacheTotalGetTime.get(), // dataTotalLoadTime
        cacheEvictCount.get(), // dataEvictionCount
        0, 0, 0, 0, 0) // For data cache, the index fiber metrics should always be zero
    }
  }

  override def pendingFiberCount: Int = {
    cacheGuardian.pendingFiberCount
  }

  override def dataCacheCount: Long = 0

  override def pendingFiberSize: Long = cacheGuardian.pendingFiberSize

  override def pendingFiberOccupiedSize: Long = cacheGuardian.pendingFiberOccupiedSize

  override def getCacheGuardian: CacheGuardian = cacheGuardian

  override def cleanUp(): Unit = {
    invalidateAll(getFibers)
    dataFiberSize.set(0L)
    dataFiberCount.set(0L)
    indexFiberSize.set(0L)
    indexFiberCount.set(0L)
  }
}
