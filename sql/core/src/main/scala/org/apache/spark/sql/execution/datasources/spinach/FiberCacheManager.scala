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

import scala.util.{Failure, Success, Try}

import com.google.common.cache._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.util.StringUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.SpinachException
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


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

  protected def fiber2Data(key: Fiber, conf: Configuration): FiberCache

  @transient protected val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .weigher(new Weigher[ENTRY, FiberCache] {
        override def weigh(key: ENTRY, value: FiberCache): Int = value.fiberData.size().toInt
      })
      .maximumWeight(MemoryManager.getDataCacheCapacity())
      .removalListener(new RemovalListener[ENTRY, FiberCache] {
        override def onRemoval(n: RemovalNotification[ENTRY, FiberCache]): Unit = {
          // TODO cause exception while we removal the using data. we need an lock machanism
          // to lock the allocate, using and the free
          MemoryManager.free(n.getValue)
        }
      })
      .build(new CacheLoader[ENTRY, FiberCache]() {
        override def load(key: ENTRY): FiberCache = {
          fiber2Data(key.key, key.conf)
        }
      })

  def apply[T <: FiberCache](fiberCache: Fiber, conf: Configuration): T = {
    cache.get(ConfigurationCache(fiberCache, conf)).asInstanceOf[T]
  }
}

/**
 * Fiber Cache Manager
 */
object FiberCacheManager extends AbstractFiberCacheManger {
  override def fiber2Data(key: Fiber, conf: Configuration): FiberCache = key match {
    case DataFiber(file, columnIndex, rowGroupId) =>
      file.getFiberData(rowGroupId, columnIndex, conf)
    case IndexFiber(file) => file.getIndexFiberData(conf)
    case other => throw new SpinachException(s"Cannot identify what's $other")
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
          entry.key.createDataFileHandle(entry.conf)
        }
      })

  def apply[T <: DataFileHandle](fiberCache: DataFile, conf: Configuration): T = {
    cache.get(ConfigurationCache(fiberCache, conf)).asInstanceOf[T]
  }
}

abstract class DataFile {
  def path: String
  def schema: StructType

  def createDataFileHandle(conf: Configuration): DataFileHandle
  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): DataFiberCache
  def iterator(conf: Configuration, requiredIds: Array[Int]): Iterator[InternalRow]
  def iterator(conf: Configuration, requiredIds: Array[Int], rowIds: Array[Long])
  : Iterator[InternalRow]
}

private[spinach] object DataFile {
  def apply(path: String, schema: StructType, dataFileClassName: String): DataFile = {
    Try(Utils.classForName(dataFileClassName).getDeclaredConstructor(
      classOf[String], classOf[StructType])).toOption match {
      case Some(ctor) =>
        Try (ctor.newInstance(path, schema).asInstanceOf[DataFile]) match {
          case Success(e) => e
          case Failure(e) =>
            throw new SpinachException(s"Cannot instantiate class $dataFileClassName", e)
        }
      case None => throw new SpinachException(
        s"Cannot find constructor of signature like:" +
          s" (String, StructType) for class $dataFileClassName")
    }
  }
}

private[spinach] trait Fiber

private[spinach]
case class DataFiber(file: DataFile, columnIndex: Int, rowGroupId: Int) extends Fiber

private[spinach]
case class IndexFiber(file: IndexFile) extends Fiber
