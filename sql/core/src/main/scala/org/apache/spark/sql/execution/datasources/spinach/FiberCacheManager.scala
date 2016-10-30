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

private[spinach] trait AbstractFiberCacheManger extends Logging {
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
  type ENTRY = ConfigurationCache[IndexFileScanner]
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
          MemoryManager.free(FiberCacheData(n.getValue.fiberData))
        }
      }).build(new CacheLoader[ENTRY, IndexFiberCacheData] {
        override def load(key: ENTRY): IndexFiberCacheData = {
          key.key.getIndexFiberData(key.conf)
        }
      })

  def apply(fileScanner: IndexFileScanner, conf: Configuration): IndexFiberCacheData = {
    cache.get(ConfigurationCache(fileScanner, conf))
  }
}

private[spinach] case class InputDataFileDescriptor(fin: FSDataInputStream, len: Long)

private[spinach] object DataMetaCacheManager extends Logging {
  type ENTRY = ConfigurationCache[DataFileScanner]
  // Using java options to config.
  val spinachDataMetaCacheSize = System.getProperty("spinach.datametacache.size",
    "262144").toLong  // default size is 256k

  @transient private val cache =
    CacheBuilder
      .newBuilder()
      .maximumSize(spinachDataMetaCacheSize)
      .build(new CacheLoader[ENTRY, DataFileMeta] {
      override def load(entry: ENTRY): DataFileMeta = {
        val fd = FiberDataFileHandler(entry.key, entry.conf)
        new DataFileMeta().read(fd.fin, fd.len)
      }
    })

  def apply(fiberCache: DataFileScanner, conf: Configuration): DataFileMeta = {
    cache.get(ConfigurationCache(fiberCache, conf))
  }
}

private[spinach] object FiberDataFileHandler extends Logging {
  type ENTRY = ConfigurationCache[DataFileScanner]
  private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .maximumSize(MemoryManager.getCapacity())
      .expireAfterAccess(1000, TimeUnit.SECONDS) // auto expire after 1000 seconds.
      .removalListener(new RemovalListener[ENTRY, InputDataFileDescriptor]() {
        override def onRemoval(n: RemovalNotification[ENTRY, InputDataFileDescriptor])
        : Unit = {
          n.getValue.fin.close()
        }
      })
      .build(new CacheLoader[ENTRY, InputDataFileDescriptor]() {
        override def load(entry: ENTRY)
        : InputDataFileDescriptor = {
          val path = new Path(StringUtils.unEscapeString(entry.key.path))
          val fs = FileSystem.get(entry.conf)

          InputDataFileDescriptor(fs.open(path), fs.getFileStatus(path).getLen)
        }
      })

  def apply(fiberCache: DataFileScanner, conf: Configuration): InputDataFileDescriptor = {
    cache.get(ConfigurationCache(fiberCache, conf))
  }
}

private[spinach] case class Fiber(file: DataFileScanner, columnIndex: Int, rowGroupId: Int)

private[spinach] case class DataFileScanner(path: String, schema: StructType) {
  override def hashCode(): Int = path.hashCode
  override def equals(that: Any): Boolean = that match {
    case DataFileScanner(thatPath, _) => path == thatPath
    case _ => false
  }

  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): FiberCacheData = {
    val is = FiberDataFileHandler(this, conf).fin
    val meta: DataFileMeta = DataMetaCacheManager(this, conf)
    val groupMeta = meta.rowGroupsMeta(groupId)
    // get the fiber data start position
    // TODO: update the meta to store the fiber start pos
    var i = 0
    var fiberStart = groupMeta.start
    while (i < fiberId) {
      fiberStart += groupMeta.fiberLens(i)
      i += 1
    }
    val len = groupMeta.fiberLens(fiberId)
    val bytes = new Array[Byte](len)

    is.synchronized {
      is.seek(fiberStart)
      is.readFully(bytes)
      putToFiberCache(bytes)
    }

  }

  def putToFiberCache(buf: Array[Byte]): FiberCacheData = {
    // TODO: make it configurable
    // TODO: disable compress first since there's some issue to solve with conpression
    val fiberCacheData = MemoryManager.allocate(buf.length)
    Platform.copyMemory(buf, Platform.BYTE_ARRAY_OFFSET, fiberCacheData.fiberData.getBaseObject,
      fiberCacheData.fiberData.getBaseOffset, buf.length)
    fiberCacheData
  }

  // full file scan
  def iterator(requiredIds: Array[Int], conf: Configuration): Iterator[InternalRow] = {
    val meta: DataFileMeta = DataMetaCacheManager(this, conf)
    val row = new BatchColumn()
    val columns: Array[ColumnValues] = new Array[ColumnValues](requiredIds.length)
    (0 until meta.groupCount).iterator.flatMap { groupId =>
      var i = 0
      while (i < columns.length) {
        columns(i) = new ColumnValues(
          meta.rowCountInEachGroup,
          schema(requiredIds(i)).dataType,
          FiberCacheManager(Fiber(this, requiredIds(i), groupId), conf))
        i += 1
      }

      if (groupId < meta.groupCount - 1) {
        // not the last row group
        row.reset(meta.rowCountInEachGroup, columns).toIterator
      } else {
        row.reset(meta.rowCountInLastGroup, columns).toIterator
      }
    }
  }

  // scan by given row ids, and we assume the rowIds are sorted
  def iterator(requiredIds: Array[Int], rowIds: Array[Int], conf: Configuration)
  : Iterator[InternalRow] = {
    val meta: DataFileMeta = DataMetaCacheManager(this, conf)
    val row = new BatchColumn()
    val columns: Array[ColumnValues] = new Array[ColumnValues](requiredIds.length)
    var lastGroupId = -1
    (0 until rowIds.length).iterator.map { idx =>
      val rowId = rowIds(idx)
      val groupId = (rowId + 1) / meta.rowCountInEachGroup
      val rowIdxInGroup = rowId % meta.rowCountInEachGroup

      if (lastGroupId != groupId) {
        // if we move to another row group, or the first row group
        var i = 0
        while (i < columns.length) {
          columns(i) = new ColumnValues(
            meta.rowCountInEachGroup,
            schema(requiredIds(i)).dataType,
            FiberCacheManager(Fiber(this, requiredIds(i), groupId), conf))
          i += 1
        }
        if (groupId < meta.groupCount - 1) {
          // not the last row group
          row.reset(meta.rowCountInEachGroup, columns)
        } else {
          row.reset(meta.rowCountInLastGroup, columns)
        }

        lastGroupId = groupId
      }

      row.moveToRow(rowIdxInGroup)
    }
  }
}

private[spinach] case class IndexFileScanner(path: String) {
  override def hashCode(): Int = path.hashCode
  override def equals(that: Any): Boolean = that match {
    case DataFileScanner(thatPath, _) => path == thatPath
    case _ => false
  }

  def putToFiberCache(buf: Array[Byte]): FiberCacheData = {
    // TODO: make it configurable
    val fiberCacheData = MemoryManager.allocate(buf.length)
    Platform.copyMemory(
      buf, Platform.BYTE_ARRAY_OFFSET, fiberCacheData.fiberData.getBaseObject,
      fiberCacheData.fiberData.getBaseOffset, buf.length)
    fiberCacheData
  }

  def getIndexFiberData(conf: Configuration): IndexFiberCacheData = {
    val file = new Path(path)
    val fs = file.getFileSystem(conf)
    val fin = fs.open(file)
    // wind to end of file to get tree root
    // TODO check if enough to fit in Int
    val fileLength = fs.getContentSummary(file).getLength
    var bytes = new Array[Byte](fileLength.toInt)
    fin.read(bytes, 0, fileLength.toInt)
    val offHeapMem = putToFiberCache(bytes)
    bytes = null

    val baseObj = offHeapMem.fiberData.getBaseObject
    val baseOff = offHeapMem.fiberData.getBaseOffset
    val dataEnd = Platform.getInt(baseObj, baseOff + fileLength - 8)
    val rootOffset = Platform.getInt(baseObj, baseOff + fileLength - 4)

    // TODO partial cached index fiber
    IndexFiberCacheData(offHeapMem.fiberData, dataEnd, rootOffset)
  }
}
