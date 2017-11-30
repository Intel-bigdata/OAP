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

import java.io.InputStream
import java.util.concurrent.{Callable, TimeUnit}

import scala.collection.JavaConverters._
import com.google.common.cache._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.executor.custom.CustomManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.oap.OapEnv
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.collection.BitSet

// TODO need to register within the SparkContext
class OapFiberCacheHeartBeatMessager extends CustomManager with Logging {
  override def status(conf: SparkConf): String = {
    OapEnv.get.fiberCacheManager.status
  }
}

case class FiberInputStream(is: InputStream, offset: Long, length: Int)

/**
 * Fiber Cache Manager
 *
 * TODO: change object to class for better initialization
 */
class FiberCacheManager(memorySize: Long, concurrency: Int) extends Logging {

  private val removalListener = new RemovalListener[Fiber, FiberCache] {
    override def onRemoval(notification: RemovalNotification[Fiber, FiberCache]): Unit = {
      // TODO: Change the log more readable
      logDebug(s"Removing Cache ${notification.getKey}")
      // TODO: Investigate lock mechanism to secure in-used FiberCache
      val fiberCache = notification.getValue
      if (fiberCache.lock.writeLock().tryLock()) {
        notification.getValue.dispose()
        fiberCache.lock.writeLock().unlock()
      } else {
        cache.put(notification.getKey, notification.getValue)
      }
    }
  }
  private val weigher = new Weigher[Fiber, FiberCache] {
    override def weigh(key: Fiber, value: FiberCache): Int =
      math.ceil(value.size() / MB).toInt
  }

  private val MB: Double = 1024 * 1024
  private val MAX_WEIGHT = (memorySize / MB).toInt

  /**
   * To avoid storing configuration in each Cache, use a loader.
   * After all, configuration is not a part of Fiber.
   */
  private def cacheLoader(fiber: Fiber, length: Int) =
    new Callable[FiberCache] {
      override def call(): FiberCache = {
        logDebug(s"Loading Cache $fiber")
        // Create a fiberCache with dummy MemoryBlock. This is used to acquire memory
        // If there is no enough memory, this fiberCache will be removed right after loading.
        fiber.createFiberCache(length)
      }
    }

  private val cache: Cache[Fiber, FiberCache] = CacheBuilder.newBuilder()
      .recordStats()
      .concurrencyLevel(concurrency)
      .removalListener(removalListener)
      .maximumWeight(MAX_WEIGHT)
      .weigher(weigher)
      .build[Fiber, FiberCache]()

  def get(fiber: Fiber, is: => FiberInputStream): FiberCache = {
    val fiberCache = cache.get(fiber, cacheLoader(fiber, is.length))
    fiberCache.lock.readLock().lock()
    if (fiberCache.isDisposed) {
      fiberCache.lock.readLock().unlock()
      null
    } else {
      // we have inserted FiberCache into Cache Manager, so we've acquire memory. load data
      fiberCache.loadData(is)
      fiberCache
    }
  }

  // TODO: test case, consider data eviction, try not use DataFileHandle which my be costly
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

  def stop(): Unit = {
    cache.cleanUp()
  }

  def getStats: CacheStats = cache.stats()
}

object FiberCacheManager {
  val OAP_FIBER_CACHE_CONCURRENCY = "spark.oap.fiberCache.concurrency"
  val OAP_FIBER_CACHE_CONCURRENCY_DEFAULT = 4
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

private[oap] trait Fiber {
  def createFiberCache(length: Int): FiberCache
}

private[oap]
case class DataFiber(file: DataFile, columnIndex: Int, rowGroupId: Int) extends Fiber {
  override def createFiberCache(length: Int): FiberCache =
    DataFiberCache(new MemoryBlock(null, -1, length))
}

private[oap]
case class IndexFiber(file: IndexFile) extends Fiber {
  override def createFiberCache(length: Int): FiberCache =
    IndexFiberCache(new MemoryBlock(null, -1, length))
}

private[oap]
case class BTreeFiber(
    file: String,
    section: Int,
    idx: Int) extends Fiber {
  override def createFiberCache(length: Int): FiberCache =
    IndexFiberCache(new MemoryBlock(null, -1, length))
}

private[oap]
case class BitmapFiber(
    file: String,
    // "0" means no split sections within file.
    sectionIdxOfFile: Int,
    // "0" means no smaller loading units.
    loadUnitIdxOfSection: Int) extends Fiber {
  override def createFiberCache(length: Int): FiberCache =
    IndexFiberCache(new MemoryBlock(null, -1, length))
}

private[oap] case class TestFiber(
    name: String) extends Fiber {
  override def createFiberCache(length: Int): FiberCache =
    DataFiberCache(new MemoryBlock(null, -1, length))
}
