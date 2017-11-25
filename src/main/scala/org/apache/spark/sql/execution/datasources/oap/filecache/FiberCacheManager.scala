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

import java.util.concurrent.{Callable, TimeUnit}

import scala.collection.JavaConverters._
import com.google.common.cache._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.executor.custom.CustomManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.MemoryManager.allocate
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.collection.BitSet

// TODO need to register within the SparkContext
class OapFiberCacheHeartBeatMessager extends CustomManager with Logging {
  override def status(conf: SparkConf): String = {
    FiberCacheManager.status
  }
}

/**
 * CacheManager
 *
 * TODO: change object to class for better initialization
 */
object FiberCacheManager extends Logging {

  private val removalListener = new RemovalListener[Fiber, FiberCache] {
    override def onRemoval(notification: RemovalNotification[Fiber, FiberCache]): Unit = {
      logDebug(s"Removing Cache ${notification.getKey}")
      notification.getValue.dispose()
    }
  }
  private val weigher = new Weigher[Fiber, FiberCache] {
    override def weigh(key: Fiber, value: FiberCache): Int = (value.size() / MB).toInt
  }

  private val MB = 1024 * 1024
  private val maxWeight = MemoryManager.maxMemory / MB

  /**
   * To avoid storing configuration in each Cache, use a loader.
   * After all, configuration is not a part of Fiber.
   */
  private def cacheLoader(fiber: Fiber, configuration: Configuration) =
    new Callable[FiberCache] {
      override def call(): FiberCache = {
        logDebug(s"Loading Cache $fiber")
        fiber2Data(fiber, configuration)
      }
    }
  /*
  private def anotherCacheLoader(fiber: Fiber, configuration: Configuration) = {
    new Callable[FiberCache] {
      override def call(): FiberCache = {
        def calculateFiberSize(fiber: Fiber): Int = {
          // Some code to get the size of fiber, to request an ENTRY in cache.
          throw new NotImplementedError()
        }
        val memoryBlock = new MemoryBlock(null, -1, calculateFiberSize(fiber))
        DataFiberCache(memoryBlock)
      }
    }
  }
  */

  private val cache = CacheBuilder.newBuilder()
      .recordStats()
      .concurrencyLevel(4)
      .removalListener(removalListener)
      .maximumWeight(maxWeight)
      .weigher(weigher)
      .build[Fiber, FiberCache]()

  def get(fiber: Fiber, conf: Configuration): FiberCache = {
    // Doesn't handle no enough memory problem here. A flag in FiberCache seem a better solution
    //
    // The problem in no enough memory is: The ENTRY.value (FiberCache) will still be returned.
    // but ENTRY will be removed from the cache at the same time (i.e. FiberCache's memory is freed)
    //
    // But, with eviction is enabled, ENTRY can be removed at anytime. Every time we want to access
    // a FiberCache, it's always possible to access a freed memory.
    // So, a flag in FiberCache is needed.
    cache.get(fiber, cacheLoader(fiber, conf))
    /*
    // Draft code to handle no enough memory problem
    val v = cache.get(fiber, anotherCacheLoader(fiber, conf))
    if (!v.isDisposed) {
      val fiberData = MemoryManager.allocate(v.size().toInt)
      val data = fiber2Data(fiber, conf)
      Platform.copyMemory(
        data,
        Platform.BYTE_ARRAY_OFFSET,
        fiberData.getBaseObject,
        fiberData.getBaseOffset,
        fiberData.size())
      v.updateFiberData(fiberData)
    }
    */
  }

  // TODO: test case
  private[filecache] def status: String = {
    val dataFibers = cache.asMap().keySet().asScala.collect {
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

  // TODO: need more discuss about how to put data from file to cache.
  // Restore back the original implementation first.
  private def fiber2Data(fiber: Fiber, conf: Configuration): FiberCache = {
    val data = fiber match {
      case DataFiber(file, columnIndex, rowGroupId) =>
        file.getFiberData(rowGroupId, columnIndex, conf)
      case IndexFiber(file) => file.getIndexFiberData(conf)
      case BTreeFiber(getFiberData, _, _, _) => getFiberData()
      case BitmapFiber(getFiberData, _, _, _) => getFiberData()
      case other => throw new OapException(s"Cannot identify what's $other")
    }
    putToFiberCache(data)
  }

  def putToFiberCache(data: Array[Byte]): FiberCache = {
    val memoryBlock = allocate(data.length)
    Platform.copyMemory(
      data,
      Platform.BYTE_ARRAY_OFFSET,
      memoryBlock.getBaseObject,
      memoryBlock.getBaseOffset,
      data.length)
    DataFiberCache(memoryBlock)
  }

  def getStats: CacheStats = cache.stats()
}

private[oap] object DataFileHandleCacheManager extends Logging {
  type ENTRY = DataFile
  private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .expireAfterAccess(1000, TimeUnit.SECONDS) // auto expire after 1000 seconds.
      .removalListener(new RemovalListener[ENTRY, DataFileHandle]() {
        override def onRemoval(n: RemovalNotification[ENTRY, DataFileHandle])
        : Unit = {
          logDebug(s"Evicting Data File Handle ${n.getKey.path}")
          n.getValue.close
        }
      })
      .build[ENTRY, DataFileHandle](new CacheLoader[ENTRY, DataFileHandle]() {
        override def load(entry: ENTRY)
        : DataFileHandle = {
          logDebug(s"Loading Data File Handle ${entry.path}")
          entry.createDataFileHandle()
        }
      })

  def apply[T <: DataFileHandle](fiberCache: DataFile): T = {
    cache.get(fiberCache).asInstanceOf[T]
  }
}

private[oap] trait Fiber

private[oap]
case class DataFiber(file: DataFile, columnIndex: Int, rowGroupId: Int) extends Fiber

private[oap]
case class IndexFiber(file: IndexFile) extends Fiber

private[oap]
case class BTreeFiber(
    getFiberData: () => Array[Byte],
    file: String,
    section: Int,
    idx: Int) extends Fiber

private[oap]
case class BitmapFiber(
    getFiberData: () => Array[Byte],
    file: String,
    // "0" means no split sections within file.
    sectionIdxOfFile: Int,
    // "0" means no smaller loading units.
    loadUnitIdxOfSection: Int) extends Fiber

private[oap] case class TestFiber(getData: () => Array[Byte], name: String) extends Fiber
