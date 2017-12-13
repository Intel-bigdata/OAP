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
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import com.google.common.cache._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.executor.custom.CustomManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.util.collection.BitSet

// TODO need to register within the SparkContext
class OapFiberCacheHeartBeatMessager extends CustomManager with Logging {
  override def status(conf: SparkConf): String = {
    FiberCacheManager.status
  }
}

/**
 * Fiber Cache Manager
 *
 * TODO: change object to class for better initialization
 */
object FiberCacheManager extends Logging {

  private val removalListener = new RemovalListener[Fiber, FiberCache] {
    override def onRemoval(notification: RemovalNotification[Fiber, FiberCache]): Unit = {
      // TODO: Change the log more readable
      logDebug(s"Removing Cache ${notification.getKey}")
      // TODO: Investigate lock mechanism to secure in-used FiberCache
      notification.getValue.dispose()
      _cacheSize.addAndGet(-notification.getValue.size())
    }
  }
  private val weigher = new Weigher[Fiber, FiberCache] {
    override def weigh(key: Fiber, value: FiberCache): Int =
      math.ceil(value.size() / MB).toInt
  }

  private val MB: Double = 1024 * 1024
  private val MAX_WEIGHT = (MemoryManager.maxMemory / MB).toInt

  // Total cached size for debug purpose
  private val _cacheSize: AtomicLong = new AtomicLong(0)

  /**
   * To avoid storing configuration in each Cache, use a loader.
   * After all, configuration is not a part of Fiber.
   */
  private def cacheLoader(fiber: Fiber, configuration: Configuration) =
    new Callable[FiberCache] {
      override def call(): FiberCache = {
        logDebug(s"Loading Cache $fiber")
        val fiberCache = fiber.fiber2Data(configuration)
        _cacheSize.addAndGet(fiberCache.size())
        fiberCache
      }
    }

  private val cache = CacheBuilder.newBuilder()
      .recordStats()
      .concurrencyLevel(4)
      .removalListener(removalListener)
      .maximumWeight(MAX_WEIGHT)
      .weigher(weigher)
      .build[Fiber, FiberCache]()

  def get(fiber: Fiber, conf: Configuration): FiberCache = {
    // Used a flag called disposed in FiberCache to indicate if this FiberCache is removed
    cache.get(fiber, cacheLoader(fiber, conf))
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

  def cacheStats: CacheStats = cache.stats()

  def cacheSize : Long = _cacheSize.get()
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
}

private[oap] case class TestFiber(getData: () => FiberCache, name: String) extends Fiber {
  override def fiber2Data(conf: Configuration): FiberCache = getData()

  override def hashCode(): Int = name.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case another: TestFiber => name.equals(another.name)
    case _ => false
  }
}
