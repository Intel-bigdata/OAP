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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.cache._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.executor.custom.CustomManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.collection.BitSet


// TODO need to register within the SparkContext
class OapHeartBeatMessager extends CustomManager with Logging {
  override def status(conf: SparkConf): String = {
    FiberCacheManager.status
  }
}

private[oap] sealed case class ConfigurationCache[T](key: T, conf: Configuration) {
  override def hashCode: Int = key.hashCode()
  override def equals(other: Any): Boolean = other match {
    case cc: ConfigurationCache[_] => cc.key == key
    case _ => false
  }
}

/**
 * The abstract class is for unit testing purpose.
 */
private[oap] trait AbstractFiberCacheManger extends Logging {
  type ENTRY = ConfigurationCache[Fiber]

  protected def fiber2Data(fiber: Fiber, conf: Configuration): FiberCache
  protected def freeFiberData(fiberCache: FiberCache): Unit

  @transient protected var cache: LoadingCache[ENTRY, FiberCache] = _

  protected var maximumFiberSizeInBytes: Long = _

  def getMaximumFiberSizeInBytes(conf: Configuration): Long = {
    if (cache == null) {
      cache = buildCache(conf)
      maximumFiberSizeInBytes
    }
    else maximumFiberSizeInBytes
  }

  private def buildCache(conf: Configuration): LoadingCache[ENTRY, FiberCache] = {

    val weightConfig = conf.getLong(SQLConf.OAP_FIBERCACHE_SIZE.key,
                              SQLConf.OAP_FIBERCACHE_SIZE.defaultValue.get)

    val weight =
      if (weightConfig > 4L * Int.MaxValue) {

        // In Guava 15.0, totalWeight is Int, so maximumWeight / concurrencyLevel should be
        // less than Int.MaxValue. What's more, if one fiber cache is very large, it may cause
        // totalWeight < Int.MaxValue < totalWeight + fiberSize. So the maximumWeight should
        // also be *FAR* less than Int.MaxValue. If totalWeight > Int.MaxValue, overflow, then
        // totalWeight will treated as a very small value, and never greater than maximumWeight.
        logWarning(s"${SQLConf.OAP_FIBERCACHE_SIZE.key}): $weightConfig is too large." +
          s"Please reduce to 8TB or less")

        4L * Int.MaxValue // The Unit here is KB. Int.MaxValue = 2G.

      } else weightConfig

    maximumFiberSizeInBytes = weight * 1024L / 4

    val builder = CacheBuilder.newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .weigher(new Weigher[ENTRY, FiberCache] {
        override def weigh(key: ENTRY, value: FiberCache): Int = {
          // Use KB as Unit due to large weight will cause Guava overflow
          val weight = math.ceil(value.fiberData.size() / 1024.0)
          if (weight > Int.MaxValue / 2) { // make sure totalWeight + fiberSize less than Int.Max
            throw new OapException(s"Fiber with more than 1TB size is not allowed.")
          }
          weight.toInt
        }
      })
      .maximumWeight(weight)
      .removalListener(new RemovalListener[ENTRY, FiberCache] {
        override def onRemoval(n: RemovalNotification[ENTRY, FiberCache]): Unit = {
          logDebug("Removing Fiber Cache" + (n.getKey.key match {
            case DataFiber(file, group, fiber) => s"(Data: ${file.path}, $group, $fiber)"
            case IndexFiber(file) => s"(Index: ${file.file.toString})"
            case _ => s"Unknown Fiber"
          }))
          // TODO cause exception while we removal the using data. we need an lock machanism
          // to lock the allocate, using and the free
          freeFiberData(n.getValue)
        }
      })

    if (conf.getBoolean(SQLConf.OAP_FIBERCACHE_STATS.key, false)) builder.recordStats()

    builder.build[ENTRY, FiberCache](new CacheLoader[ENTRY, FiberCache]() {
      override def load(entry: ENTRY): FiberCache = {
        logDebug("Loading Fiber Cache" + (entry.key match {
          case DataFiber(file, group, fiber) => s"(Data: ${file.path}, $group, $fiber)"
          case IndexFiber(file) => s"(Index: ${file.file.toString})"
          case _ => s"Unknown Fiber"
        }))
        fiber2Data(entry.key, entry.conf)
      }
    })
  }

  def apply[T <: FiberCache](fiber: Fiber, conf: Configuration): T = {
    if (cache == null) cache = buildCache(conf)
    cache.get(ConfigurationCache(fiber, conf)).asInstanceOf[T]
  }
}

/**
 * Fiber Cache Manager
 */
object FiberCacheManager extends AbstractFiberCacheManger {
  /**
   * evict Fiber -> FiberCache(MemoryBlock off-heap) data manually
   * @param fiber:the fiber whose corresponding memory block(off -heap) that needs to be evicted
   */
  def evictFiberCacheData(fiber: Fiber): Unit = fiber match {
    case idxFiber: IndexFiber =>
      val entry = ConfigurationCache[Fiber](idxFiber, new Configuration())
      if(cache != null && cache.asMap().asScala.contains(entry)) cache.invalidate(entry)
    case _ => // todo: consider whether we indeed need to evict DataFiberCachedData manually
  }

  override def fiber2Data(fiber: Fiber, conf: Configuration): FiberCache = fiber match {
    case DataFiber(file, columnIndex, rowGroupId) =>
      file.getFiberData(rowGroupId, columnIndex, conf)
    case IndexFiber(file) => file.getIndexFiberData(conf)
    case other => throw new OapException(s"Cannot identify what's $other")
  }

  override def freeFiberData(fiberCache: FiberCache): Unit = MemoryManager.free(fiberCache)

  def status: String = {
    val dataFiberConfPairs =
      if (cache == null) Set.empty[(DataFiber, Configuration)]
      else {
        this.cache.asMap().keySet().asScala.collect {
          case entry @ ConfigurationCache(key: DataFiber, conf) => (key, conf)
        }
      }

    val fiberFileToFiberMap = new mutable.HashMap[String, mutable.Buffer[DataFiber]]()
    dataFiberConfPairs.foreach { case (dataFiber, _) =>
      fiberFileToFiberMap.getOrElseUpdate(
        dataFiber.file.path, new mutable.ArrayBuffer[DataFiber]) += dataFiber
    }

    val filePathSet = new mutable.HashSet[String]()
    val statusRawData = dataFiberConfPairs.collect {
      case (dataFiber @ DataFiber(dataFile : OapDataFile, _, _), conf)
        if !filePathSet.contains(dataFile.path) =>
        val fileMeta =
          DataFileHandleCacheManager(dataFile, conf).asInstanceOf[OapDataFileHandle]
        val fiberBitSet = new BitSet(fileMeta.groupCount * fileMeta.fieldCount)
        val fiberCachedList: Seq[DataFiber] =
          fiberFileToFiberMap.getOrElse(dataFile.path, Seq.empty)
        fiberCachedList.foreach { fiber =>
          fiberBitSet.set(fiber.columnIndex + fileMeta.fieldCount * fiber.rowGroupId)
        }
        filePathSet.add(dataFile.path)
        FiberCacheStatus(dataFile.path, fiberBitSet, fileMeta)
    }.toSeq

    val retStatus = CacheStatusSerDe.serialize(statusRawData)
    retStatus
  }
}

private[oap] object DataFileHandleCacheManager extends Logging {
  type ENTRY = ConfigurationCache[DataFile]
  private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .expireAfterAccess(1000, TimeUnit.SECONDS) // auto expire after 1000 seconds.
      .removalListener(new RemovalListener[ENTRY, DataFileHandle]() {
        override def onRemoval(n: RemovalNotification[ENTRY, DataFileHandle])
        : Unit = {
          logDebug(s"Evicting Data File Handle ${n.getKey.key.path}")
          n.getValue.fin.close()
        }
      })
      .build[ENTRY, DataFileHandle](new CacheLoader[ENTRY, DataFileHandle]() {
        override def load(entry: ENTRY)
        : DataFileHandle = {
          logDebug(s"Loading Data File Handle ${entry.key.path}")
          entry.key.createDataFileHandle(entry.conf)
        }
      })

  def apply[T <: DataFileHandle](fiberCache: DataFile, conf: Configuration): T = {
    cache.get(ConfigurationCache(fiberCache, conf)).asInstanceOf[T]
  }
}

private[oap] trait Fiber

private[oap]
case class DataFiber(file: DataFile, columnIndex: Int, rowGroupId: Int) extends Fiber

private[oap]
case class IndexFiber(file: IndexFile) extends Fiber
