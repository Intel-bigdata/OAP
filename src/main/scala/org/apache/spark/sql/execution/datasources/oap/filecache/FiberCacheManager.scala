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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import com.google.common.cache._
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.format.CompressionCodec

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.executor.custom.CustomManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock
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

  def pendingSize: Int = removalPendingQueue.size()

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
      logDebug(s"Removing fiber: $fiber")
      // Block if fiber is in use.
      while (!fiberCache.tryDispose(3000)) {
        // Check memory usage every 3s while we are waiting fiber release.
        logDebug(s"Waiting fiber to be released timeout. Fiber: $fiber")
        if (_pendingFiberSize.get() > maxMemory) {
          logWarning("Fibers pending on removal use too much memory, " +
              s"current: ${_pendingFiberSize.get()}, max: $maxMemory")
        }
      }
      // TODO: Make log more readable
      _pendingFiberSize.addAndGet(-fiberCache.size())
      logDebug(s"Fiber removed successfully. Fiber: $fiber")
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

  private var (_cacheStrategy: String,
    _indexCacheCompressEnable: Boolean,
    _dataCacheCompressEnable: Boolean,
    _indexCacheCompressionCodec: String,
    _dataCacheCompressionCodec: String) = {
    val sparkEnv = SparkEnv.get
    // Different options enable users to separately configure cache compression as needed.
    assert(sparkEnv != null, "Oap can't run without SparkContext")
    (sparkEnv.conf.get("spark.oap.cache.strategy", DEFAULT_CACHE_STRATEGY),
      sparkEnv.conf.get(SQLConf.OAP_ENABLE_INDEX_FIBER_CACHE_COMPRESSION),
      sparkEnv.conf.get(SQLConf.OAP_ENABLE_DATA_FIBER_CACHE_COMPRESSION),
      sparkEnv.conf.get(SQLConf.OAP_INDEX_FIBER_CACHE_COMPRESSION_Codec),
      sparkEnv.conf.get(SQLConf.OAP_DATA_FIBER_CACHE_COMPRESSION_Codec))
  }

  private lazy val codecFactory = new CodecFactory(new Configuration())
  def getCodecFactory: CodecFactory = {
    codecFactory
  }

  private val cacheBackend: OapCache = {
    if (_cacheStrategy.equals(GUAVA_CACHE)) {
      new GuavaOapCache(MemoryManager.cacheMemory, MemoryManager.cacheGuardianMemory)
    } else if (_cacheStrategy.equals(SIMPLE_CACHE)) {
      _indexCacheCompressEnable = false
      _dataCacheCompressEnable = false
      new SimpleOapCache()
    } else {
      throw new OapException("Unsupported cache strategy")
    }
  }

  def indexCacheCompressEnable: Boolean = _indexCacheCompressEnable
  def dataCacheCompressEnable: Boolean = _dataCacheCompressEnable
  def indexCacheCompressionCodec: String = _indexCacheCompressionCodec
  def dataCacheCompressionCodec: String = _dataCacheCompressionCodec

  // NOTE: all members' init should be placed before this line.
  logInfo(s"Initialized FiberCacheManager: " +
    s"indexCacheCompressEnable = ${_indexCacheCompressEnable} " +
    s"dataCacheCompressEnable = ${_dataCacheCompressEnable} " +
    s"indexCacheCompressionCodec = ${_indexCacheCompressionCodec} " +
    s"dataCacheCompressionCodec = ${_dataCacheCompressionCodec}")

  def get(fiber: Fiber, conf: Configuration): FiberCache = synchronized {
    logDebug(s"Getting Fiber: $fiber")
    val fiberCache = cacheBackend.get(fiber, conf)

    val (enbaleCompress, decompressor) = if (_indexCacheCompressEnable &&
      (fiber.isInstanceOf[BitmapFiber] || fiber.isInstanceOf[BTreeFiber])) {
      (fiber.isCompress(), codecFactory.getDecompressor(
        CompressionCodec.valueOf(_indexCacheCompressionCodec)))
    } else if (_dataCacheCompressEnable && fiber.isInstanceOf[DataFiber]) {
      (fiber.isCompress(), codecFactory.getDecompressor(
        CompressionCodec.valueOf(_dataCacheCompressionCodec)))
      // For Unit Test
    } else if (fiber.isInstanceOf[TestFiber]) {
      (fiber.isCompress(), codecFactory.getDecompressor(
        CompressionCodec.valueOf(_dataCacheCompressionCodec)))
    } else {
      (false, null)
    }

    if (enbaleCompress && decompressor != null) {
      // First, copy compressed data from off-heap to on-heap, then decompress the data.
      val bytes = new Array[Byte](fiberCache.size().toInt)
      fiberCache.copyMemoryToBytes(0, bytes)
      val decompressBytes = decompressor.decompress(bytes, fiberCache.decompressLength)
      val memoryBlock = new MemoryBlock(decompressBytes, Platform.BYTE_ARRAY_OFFSET,
        decompressBytes.length)
      DecompressFiberCache(memoryBlock, decompressBytes.length, fiberCache)
    } else {
      fiberCache
    }
  }

  def removeIndexCache(indexName: String): Unit = synchronized {
    logDebug(s"Going to remove all index cache of $indexName")
    val fiberToBeRemoved = cacheBackend.getFibers.filter {
      case BTreeFiber(_, file, _, _, _) => file.contains(indexName)
      case BitmapFiber(_, file, _, _, _) => file.contains(indexName)
      case _ => false
    }
    cacheBackend.invalidateAll(fiberToBeRemoved)
    logDebug(s"Removed ${fiberToBeRemoved.size} fibers.")
  }

  // Used by test suite
  private[filecache] def removeFiber(fiber: TestFiber): Unit = synchronized {
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

  // Unit Test
  def setCompressionConf(indexEnable: Boolean = false, dataEnable: Boolean = false,
                    indexCodec: String = "GZIP", dataCodec: String = "GZIP"): Unit = {
    _indexCacheCompressEnable = indexEnable
    _dataCacheCompressEnable = dataEnable
    _indexCacheCompressionCodec = indexCodec
    _dataCacheCompressionCodec = dataCodec
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
  // To identify whether corresponding cache will be compressed.
  def isCompress(): Boolean
}

private[oap]
case class DataFiber(file: DataFile, columnIndex: Int, rowGroupId: Int,
                     enableCompress: Boolean) extends Fiber {
  override def fiber2Data(conf: Configuration): FiberCache =
    file.getFiberData(rowGroupId, columnIndex, conf, enableCompress)

  override def hashCode(): Int = (file.path + columnIndex + rowGroupId).hashCode

  override def equals(obj: Any): Boolean = obj match {
    case another: DataFiber =>
      another.columnIndex == columnIndex &&
        another.rowGroupId == rowGroupId &&
        another.file.path.equals(file.path)
    case _ => false
  }

  override def toString: String = {
    s"type: DataFiber rowGroup: $rowGroupId column: $columnIndex " +
      s"enableCompress: $enableCompress\n\tfile: ${file.path}"
  }

  override def isCompress(): Boolean = {
    enableCompress
  }
}

private[oap]
case class BTreeFiber(
    getFiberData: (Boolean) => FiberCache,
    file: String,
    section: Int,
    idx: Int,
    enableCompress: Boolean) extends Fiber {
  override def fiber2Data(conf: Configuration): FiberCache = getFiberData(enableCompress)

  override def hashCode(): Int = (file + section + idx).hashCode

  override def equals(obj: Any): Boolean = obj match {
    case another: BTreeFiber =>
      another.section == section &&
        another.idx == idx &&
        another.file.equals(file)
    case _ => false
  }

  override def toString: String = {
    s"type: BTreeFiber section: $section idx: $idx enableCompress: $enableCompress \n\tfile: $file"
  }

  override def isCompress(): Boolean = {
    enableCompress
  }
}

private[oap]
case class BitmapFiber(
    getFiberData: (Boolean) => FiberCache,
    file: String,
    // "0" means no split sections within file.
    sectionIdxOfFile: Int,
    // "0" means no smaller loading units.
    loadUnitIdxOfSection: Int,
    enableCompress: Boolean) extends Fiber {
  override def fiber2Data(conf: Configuration): FiberCache = getFiberData(enableCompress)

  override def hashCode(): Int = (file + sectionIdxOfFile + loadUnitIdxOfSection).hashCode

  override def equals(obj: Any): Boolean = obj match {
    case another: BitmapFiber =>
      another.sectionIdxOfFile == sectionIdxOfFile &&
        another.loadUnitIdxOfSection == loadUnitIdxOfSection &&
        another.file.equals(file)
    case _ => false
  }

  override def toString: String = {
    s"type: BitmapFiber section: $sectionIdxOfFile idx: $loadUnitIdxOfSection" +
      s" enableCompress: $enableCompress \n\tfile: $file"
  }

  override def isCompress(): Boolean = {
    enableCompress
  }
}

private[oap] case class TestFiber(getData: (Boolean) => FiberCache, name: String,
                                  enableCompress: Boolean) extends Fiber {
  override def fiber2Data(conf: Configuration): FiberCache = getData(enableCompress)

  override def hashCode(): Int = name.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case another: TestFiber => name.equals(another.name)
    case _ => false
  }

  override def toString: String = {
    s"type: TestFiber name: $name enableCompress: $enableCompress"
  }

  override def isCompress(): Boolean = {
    enableCompress
  }
}
