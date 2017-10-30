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

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import com.google.common.cache._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.executor.custom.CustomManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.storage.{BlockId, FiberBlockId, StorageLevel}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.TimeStampedHashMap
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}


// TODO need to register within the SparkContext
class OapFiberCacheHeartBeatMessager extends CustomManager with Logging {
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

private[oap] class CacheResult (
  val cached: Boolean,
  val buffer: ChunkedByteBuffer)

/**
 * Fiber Cache Manager
 */
object FiberCacheManager extends Logging {

  private val dataFileIdMap = new TimeStampedHashMap[String, DataFile](updateTimeStampOnGet = true)

  private def toByteBuffer(buf: Array[Byte]): ChunkedByteBuffer = {
    new ChunkedByteBuffer(ByteBuffer.wrap(buf))
  }

  def fiber2Block(fiber: Fiber): BlockId = {
    fiber match {
      case DataFiber(file, columnIndex, rowGroupId) =>
        dataFileIdMap.getOrElseUpdate(file.path, file)
        FiberBlockId("data_" + file.path + "_" + columnIndex + "_" + rowGroupId)
      case IndexFiber(file) =>
        // TODO: Need to improve this if we have multiple fibers in one index file
        FiberBlockId("index_" + file.file)
      case BTreeFiber(_, file, section, idx) =>
        FiberBlockId("btree_" + file + "_" + section + "_" + idx)
    }
  }

  def block2Fiber(blockId: BlockId): Fiber = {

    val FiberDataBlock = "fiber_data_(.*)_([0-9]+)_([0-9]+)".r
    blockId.name match {
      case FiberDataBlock(fileId, columnIndex, rowGroupId) =>
        val dataFile = dataFileIdMap(fileId)
        DataFiber(dataFile, columnIndex.toInt, rowGroupId.toInt)
      case _ => throw new OapException("unknown blockId: " + blockId.name)
    }
  }

  def releaseLock(fiber: Fiber): Unit = {
    val blockId = fiber2Block(fiber)
    logDebug("Release lock for: " + blockId.name)
    val blockManager = SparkEnv.get.blockManager
    blockManager.releaseLock(blockId)
  }

  def getOrElseUpdate(fiber: Fiber, conf: Configuration): CacheResult = {
    // Make sure no exception if no SparkContext is created.
    if (SparkEnv.get == null) return new CacheResult(false, fiber2Data(fiber, conf))
    val blockManager = SparkEnv.get.blockManager
    val blockId = fiber2Block(fiber)
    logDebug("Fiber name: " + blockId.name)
    val storageLevel = StorageLevel(useDisk = false, useMemory = true,
      useOffHeap = true, deserialized = false, 1)

    val allocator = Platform.allocateDirectBuffer _
    blockManager.getLocalBytes(blockId) match {
      case Some(buffer) =>
        logDebug("Got fiber from cache.")
        new CacheResult(true, buffer)
      case None =>
        logDebug("No fiber found. Build it")
        val bytes = fiber2Data(fiber, conf)
        // For the sake of simplicity, only support one ByteBuffer in ChunkedBytesBuffer currently.
        assert(bytes.chunks.length == 1, "Fiber data can have only one ByteBuffer")
        val offHeapBytes = bytes.copy(allocator)
        // If put bytes into BlockManager failed, means there is no enough off-heap memory.
        // So, use on-heap memory after failure.
        if (blockManager.putBytes(blockId, offHeapBytes, storageLevel)) {
          logDebug("Put fiber to cache success")
          new CacheResult(true, blockManager.getLocalBytes(blockId).get)
        } else {
          logDebug("Put fiber to cache fail")
          offHeapBytes.dispose()
          new CacheResult(false, bytes)
        }
    }
  }

  def fiber2Data(fiber: Fiber, conf: Configuration): ChunkedByteBuffer = fiber match {
    case DataFiber(file, columnIndex, rowGroupId) =>
      file.getFiberData(rowGroupId, columnIndex, conf)
    case IndexFiber(file) => file.getIndexFiberData(conf)
    case BTreeFiber(getFiberData, _, _, _) => toByteBuffer(getFiberData())
    case other => throw new OapException(s"Cannot identify what's $other")
  }

  def status: String = {
    val sparkEnv = SparkEnv.get
    val threshTime = System.currentTimeMillis()

    val fibers =
      if (sparkEnv == null) Seq.empty
      else {
        val fiberBlockIds = sparkEnv.blockManager.getMatchingBlockIds(blockId =>
          blockId.name.startsWith("fiber_data_"))
        fiberBlockIds.map(blockId => block2Fiber(blockId))
      }

    logDebug("current cached blocks: \n" +
      fibers.map {
        case dataFiber: DataFiber => dataFiber.file.path +
          " column:" + dataFiber.columnIndex +
          " groupId:" + dataFiber.rowGroupId }.mkString("\n"))

    // We have went over all fiber blocks in BlockManager. Remove out-dated item in dataFileIdMap
    dataFileIdMap.clearOldValues(threshTime)

    val fiberFileToFiberMap = new mutable.HashMap[String, mutable.Buffer[DataFiber]]()
    fibers.foreach { case dataFiber: DataFiber =>
      fiberFileToFiberMap.getOrElseUpdate(
        dataFiber.file.path, new mutable.ArrayBuffer[DataFiber]) += dataFiber
    }

    val filePathSet = new mutable.HashSet[String]()
    val statusRawData = fibers.collect {
      case _ @ DataFiber(dataFile : OapDataFile, _, _) if !filePathSet.contains(dataFile.path) =>
        val fileMeta =
          DataFileHandleCacheManager(dataFile).asInstanceOf[OapDataFileHandle]
        val fiberBitSet = new BitSet(fileMeta.groupCount * fileMeta.fieldCount)
        val fiberCachedList: Seq[DataFiber] =
          fiberFileToFiberMap.getOrElse(dataFile.path, Seq.empty)
        fiberCachedList.foreach { fiber =>
          fiberBitSet.set(fiber.columnIndex + fileMeta.fieldCount * fiber.rowGroupId)
        }
        filePathSet.add(dataFile.path)
        FiberCacheStatus(dataFile.path, fiberBitSet, fileMeta)
    }

    val retStatus = CacheStatusSerDe.serialize(statusRawData)
    retStatus
  }
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
          n.getValue.fin.close()
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
