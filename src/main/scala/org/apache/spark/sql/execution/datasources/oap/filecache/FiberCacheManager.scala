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

import scala.collection.mutable

import com.google.common.cache._
import org.apache.hadoop.conf.Configuration
import sun.nio.ch.DirectBuffer

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.executor.custom.CustomManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.storage.{BlockId, FiberBlockId, StorageLevel}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.TimeStampedHashMap
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.io.ChunkedByteBuffer

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

/**
 * TODO: Handle bug that release can be called twice.
 */
private[oap] case class FiberData(
    private val memory: MemoryBlock,
    private val releaseFunc: () => Unit = () => {}) {

  /**
   * toArray may cause copy memory from off-heap to on-heap. Should be avoid.
   */
  def toArray: Array[Byte] = {
    assert(memory.size() <= Int.MaxValue, "FiberData exceeds maximum size (Int.MaxValue).")
    val bytes = new Array[Byte](length.toInt)
    Platform.copyMemory(baseObj, baseOffset, bytes, Platform.BYTE_ARRAY_OFFSET, length)
    bytes
  }
  def baseObj: AnyRef = memory.getBaseObject
  def baseOffset: Long = memory.getBaseOffset
  def length: Long = memory.size()

  private var released = false
  def release(): Unit = {
    if (!released) releaseFunc()
    else released = true
  }
}

private[oap] object FiberData {
  def apply(data: Array[Byte]): FiberData = {
    val memoryBlock = new MemoryBlock(data, Platform.BYTE_ARRAY_OFFSET, data.length)
    FiberData(memoryBlock)
  }

  def apply(data: ChunkedByteBuffer, release: () => Unit): FiberData = {
    assert(data.chunks.length == 1, "FiberData in Spark BlockManager can have only one chunk")
    data.chunks.head match {
      case db: DirectBuffer =>
        val memoryBlock = new MemoryBlock(null, db.address(), data.size)
        FiberData(memoryBlock, release)
      case _ => throw new OapException("FiberData in Spark BlockManager can only be off-heap")
    }
  }
}

/**
 * Fiber Cache Manager
 */
object FiberCacheManager extends Logging {

  private val dataFileIdMap = new TimeStampedHashMap[String, DataFile](updateTimeStampOnGet = true)

  private def toChunkedByteBuffer(buf: Array[Byte]): ChunkedByteBuffer = {
    val allocator = MemoryManager.allocateDirectBuffer _
    val byteBuffer = allocator(buf.length)
    byteBuffer.put(buf)
    byteBuffer.flip()
    new ChunkedByteBuffer(byteBuffer)
  }

  def fiber2Block(fiber: Fiber): BlockId = {
    fiber match {
      case DataFiber(file, columnIndex, rowGroupId) =>
        dataFileIdMap.getOrElseUpdate(file.path, file)
        FiberBlockId("data_" + file.path + "_" + columnIndex + "_" + rowGroupId)
      case IndexFiber(file) =>
        // TODO: this should be removed
        FiberBlockId("index_" + file.file)
      case BTreeFiber(_, file, section, idx) =>
        FiberBlockId("btree_" + file + "_" + section + "_" + idx)
      case BitmapFiber(_, file, sectionIdxOfFile, loadUnitIdxOfSection) =>
        FiberBlockId("bitmapIndex_" + file + "_" + sectionIdxOfFile + "_" + loadUnitIdxOfSection)
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

  def getOrElseUpdate(fiber: Fiber, conf: Configuration): FiberData = {
    // Make sure no exception if no SparkContext is created.
    if (SparkEnv.get == null) return FiberData(fiber2Data(fiber, conf))
    val blockManager = SparkEnv.get.blockManager
    val blockId = fiber2Block(fiber)
    logDebug("Fiber name: " + blockId.name)
    val storageLevel = StorageLevel(useDisk = false, useMemory = true,
      useOffHeap = true, deserialized = false, 1)

    blockManager.getLocalBytes(blockId) match {
      case Some(buffer) =>
        logDebug("Got fiber from cache.")
        FiberData(buffer, () => blockManager.releaseLock(blockId))
      case None =>
        logDebug("No fiber found. Build it")
        val bytes = fiber2Data(fiber, conf)
        val offHeapBytes = toChunkedByteBuffer(bytes)
        // If put bytes into BlockManager failed, means there is no enough off-heap memory.
        // So, use on-heap memory after failure.
        if (blockManager.putBytes(blockId, offHeapBytes, storageLevel)) {
          logDebug("Put fiber to cache success")
          FiberData(
            blockManager.getLocalBytes(blockId).get, () => blockManager.releaseLock(blockId))
        } else {
          logDebug("Put fiber to cache fail")
          offHeapBytes.dispose()
          FiberData(bytes)
        }
    }
  }

  def fiber2Data(fiber: Fiber, conf: Configuration): Array[Byte] = fiber match {
    case DataFiber(file, columnIndex, rowGroupId) =>
      file.getFiberData(rowGroupId, columnIndex, conf)
    case IndexFiber(file) => file.getIndexFiberData(conf)
    case BTreeFiber(getFiberData, _, _, _) => getFiberData()
    case BitmapFiber(getFiberData, _, _, _) => getFiberData()
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
