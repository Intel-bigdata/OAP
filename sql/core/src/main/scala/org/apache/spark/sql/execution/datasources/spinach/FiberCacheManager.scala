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

package org.apache.spark.sql.execution.datasources.spinach

import java.util.concurrent.TimeUnit

import com.google.common.cache._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.util.StringUtils

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.io.SnappyCompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

private sealed case class ConfigurationCache[T](key: T, conf: Configuration) {
  override def hashCode: Int = key.hashCode()
  override def equals(other: Any): Boolean = other match {
    case cc: ConfigurationCache[_] => cc.key == key
    case _ => false
  }
}

/**
 * The abstract class is for unit testing purpose.
 */
private[spinach] sealed trait AbstractFiberCacheManger extends Logging {
  type ENTRY = ConfigurationCache[Fiber]

  protected def fiber2Data(key: Fiber, conf: Configuration): FiberCacheData

  @transient protected val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .weigher(new Weigher[ENTRY, FiberCacheData] {
        override def weigh(key: ENTRY, value: FiberCacheData): Int = value.fiberData.size().toInt
      })
      .maximumWeight(MemoryManager.getCapacity())
      .removalListener(new RemovalListener[ENTRY, FiberCacheData] {
        override def onRemoval(n: RemovalNotification[ENTRY, FiberCacheData]): Unit = {
          // TODO cause exception while we removal the using data. we need an lock machanism
          // to lock the allocate, using and the free
          MemoryManager.free(n.getValue)
        }
      })
      .build(new CacheLoader[ENTRY, FiberCacheData]() {
        override def load(key: ENTRY): FiberCacheData = {
          fiber2Data(key.key, key.conf)
        }
      })

  def apply(fiberCache: Fiber, conf: Configuration): FiberCacheData = {
    cache.get(ConfigurationCache(fiberCache, conf))
  }
}

/**
 * Fiber Cache Manager
 */
object FiberCacheManager extends AbstractFiberCacheManger {
  override def fiber2Data(key: Fiber, conf: Configuration): FiberCacheData = {
    key.file.getFiberData(key.rowGroupId, key.columnIndex, conf)
  }
}

/**
 * Index Cache Manager TODO: merge this with AbstractFiberCacheManager
 */
private[spinach] object IndexCacheManager extends Logging {
  type ENTRY = ConfigurationCache[IndexFile]
  @transient protected val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .weigher(new Weigher[ENTRY, IndexFiberCacheData] {
        override def weigh(key: ENTRY, value: IndexFiberCacheData): Int =
         value.fiberData.size().toInt
      }).maximumWeight(MemoryManager.getCapacity())
      .removalListener(new RemovalListener[ENTRY, IndexFiberCacheData] {
        override def onRemoval(n: RemovalNotification[ENTRY, IndexFiberCacheData]): Unit = {
          logDebug(s"Evicting Index Block ${n.getKey.key.path}")
          MemoryManager.free(FiberCacheData(n.getValue.fiberData))
        }
      }).build(new CacheLoader[ENTRY, IndexFiberCacheData] {
        override def load(key: ENTRY): IndexFiberCacheData = {
          logDebug(s"Loading Index Block ${key.key.path}")
          key.key.getIndexFiberData(key.conf)
        }
      })

  def apply(fileScanner: IndexFile, conf: Configuration): IndexFiberCacheData = {
    cache.get(ConfigurationCache(fileScanner, conf))
  }
}

/**
 * The data file handle, will be cached for performance purpose, as we don't want to open the
 * specified file again and again to get its data meta, the data file extension can have its own
 * implementation.
 */
abstract class DataFileHandle {
  def fin: FSDataInputStream
  def len: Long
}

private[spinach] object DataFileHandleCacheManager extends Logging {
  type ENTRY = ConfigurationCache[DataFile]
  private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .maximumSize(MemoryManager.getCapacity())
      .expireAfterAccess(1000, TimeUnit.SECONDS) // auto expire after 1000 seconds.
      .removalListener(new RemovalListener[ENTRY, DataFileHandle]() {
        override def onRemoval(n: RemovalNotification[ENTRY, DataFileHandle])
        : Unit = {
          logDebug(s"Evicting Data File Handle ${n.getKey.key.path}")
          n.getValue.fin.close()
        }
      })
      .build(new CacheLoader[ENTRY, DataFileHandle]() {
        override def load(entry: ENTRY)
        : DataFileHandle = {
          logDebug(s"Loading Data File Handle ${entry.key.path}")
          val path = new Path(StringUtils.unEscapeString(entry.key.path))
          val fs = FileSystem.get(entry.conf)

          new DataFileMeta().read(fs.open(path), fs.getFileStatus(path).getLen)
        }
      })

  def apply[T <: DataFileHandle](fiberCache: DataFile, conf: Configuration): T = {
    cache.get(ConfigurationCache(fiberCache, conf)).asInstanceOf[T]
  }
}

abstract class DataFile {
  def path: String
  def schema: StructType

  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): FiberCacheData
  def iterator(conf: Configuration, requiredIds: Array[Int]): Iterator[InternalRow]
  def iterator(conf: Configuration, requiredIds: Array[Int], rowIds: Array[Int])
  : Iterator[InternalRow]
}

private[spinach] case class Fiber(file: DataFile, columnIndex: Int, rowGroupId: Int)
