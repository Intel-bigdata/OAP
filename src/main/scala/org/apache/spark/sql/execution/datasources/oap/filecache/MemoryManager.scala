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

import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.fs.FSDataInputStream

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.storage.{BlockManager, TestBlockId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.{MemoryAllocator, MemoryBlock}

private[sql] abstract class MemoryManager {
  /**
   * Return the total memory used until now.
   */
  def memoryUsed: Long

  /**
   * The memory size used for cache.
   */
  def getCacheMemorySize: Long

  /**
   * The memory size used for cache guardian.
   */
  def getCacheGuardianMemorySize: Long

  /**
   * Allocate a block of memory with given size. The actual occupied size of memory maybe different
   * with the requested size, that's depends on the underlying implementation.
   * @param size requested size of memory block
   */
  private[filecache] def allocate(size: Long): MemoryBlock
  private[filecache] def free(block: MemoryBlock): Unit

  /**
   * Get the actual occupied size of requested memory. The meaning should be equal with
   * 'malloc.malloc_usable_size'. This method is valuable, because we may allocate memory based
   * on different implementation (eg: jemalloc).
   * @param address the address of the block.
   * @param requestedSize the requested size of the block.
   * @return the actual occupied size.
   */
  protected def getUsableSize(address: Long, requestedSize: Long): Long

  /**
   * Used by IndexFile
   */
  def toIndexFiberCache(in: FSDataInputStream, position: Long, length: Int): FiberCache

  /**
   * Used by IndexFile. For decompressed data
   */
  def toIndexFiberCache(bytes: Array[Byte]): FiberCache

  /**
   * Used by OapDataFile since we need to parse the raw data in on-heap memory before put it into
   * off-heap memory
   */
  def toDataFiberCache(bytes: Array[Byte]): FiberCache
  def getEmptyDataFiberCache(length: Long): FiberCache
  def stop(): Unit
}

private[sql] object MemoryManager {
  /**
   * Dummy block id to acquire memory from [[org.apache.spark.memory.MemoryManager]]
   *
   * NOTE: We do acquire some memory from Spark without adding a Block into[[BlockManager]]
   * It may cause consistent problem.
   * (i.e. total size of blocks in BlockManager is not equal to Spark used storage memory)
   *
   * TODO should avoid using [[TestBlockId]]
   */
  private[filecache] val DUMMY_BLOCK_ID = TestBlockId("oap_memory_request_block")

  def apply(sparkEnv: SparkEnv): MemoryManager = {
    val conf = sparkEnv.conf
    val memoryManagerOpt =
      conf.get(OapConf.OAP_FIBERCACHE_MEMORY_MANAGER.key, "offheap").toLowerCase
    memoryManagerOpt match {
      case "offheap" => new OffHeapMemoryManager(sparkEnv)
      case _ => throw new UnsupportedOperationException(
        s"The memory manager: ${memoryManagerOpt} is not supported now")
    }
  }
}

/**
 * An memory manager which support allocate OFF_HEAP memory. It will acquire fixed amount of
 * memory from spark during initialization.
 */
private[filecache] class OffHeapMemoryManager(sparkEnv: SparkEnv)
  extends MemoryManager with Logging {

  private lazy val memoryManager = sparkEnv.memoryManager

  private lazy val oapMemory = {
    assert(memoryManager.maxOffHeapStorageMemory > 0, "Oap can't run without offHeap memory")
    val useOffHeapRatio = sparkEnv.conf.getDouble(
      OapConf.OAP_FIBERCACHE_USE_OFFHEAP_RATIO.key,
      OapConf.OAP_FIBERCACHE_USE_OFFHEAP_RATIO.defaultValue.get)
    logInfo(s"Oap use ${useOffHeapRatio * 100}% of 'spark.memory.offHeap.size' for fiber cache.")
    assert(useOffHeapRatio > 0 && useOffHeapRatio <1,
      "OapConf 'spark.sql.oap.fiberCache.use.offheap.ratio' must more than 0 and less than 1.")
    (memoryManager.maxOffHeapStorageMemory * useOffHeapRatio).toLong
  }

  // TODO: a config to control max memory size
  private val (_cacheMemory, _cacheGuardianMemory) = {
    if (memoryManager.acquireStorageMemory(
      MemoryManager.DUMMY_BLOCK_ID, oapMemory, MemoryMode.OFF_HEAP)) {
      // TODO: make 0.9, 0.1 configurable
      ((oapMemory * 0.9).toLong, (oapMemory * 0.1).toLong)
    } else {
      throw new OapException("Can't acquire memory from spark Memory Manager")
    }
  }

  // TODO: Atomic is really needed?
  private val _memoryUsed = new AtomicLong(0)

  override def memoryUsed: Long = _memoryUsed.get()

  override def getCacheMemorySize: Long = _cacheMemory

  override def getCacheGuardianMemorySize: Long = _cacheGuardianMemory

  override private[filecache] def allocate(size: Long): MemoryBlock = {
    val block = MemoryAllocator.UNSAFE.allocate(size)
    val occupied = getUsableSize(block.getBaseOffset, block.size())
    _memoryUsed.getAndAdd(occupied)
    logDebug(s"request allocate $size memory, actual occupied size: " +
      s"${occupied}, used: $memoryUsed")
    block
  }

  override private[filecache] def free(block: MemoryBlock): Unit = {
    // TODO: get the usable size from the parameter? However, it's a bit ugly.
    val occupiedSize = getUsableSize(block.getBaseOffset, block.size())
    MemoryAllocator.UNSAFE.free(block)
    _memoryUsed.getAndAdd(-occupiedSize)
    logDebug(s"freed ${block.size()} memory, used: $memoryUsed")
  }

  override protected def getUsableSize(address: Long, requestedSize: Long): Long = {
    requestedSize
  }

  override def toIndexFiberCache(
      in: FSDataInputStream,
      position: Long,
      length: Int): FiberCache = {
    val bytes = new Array[Byte](length)
    in.readFully(position, bytes)
    toFiberCache(bytes)
  }

  override def toIndexFiberCache(bytes: Array[Byte]): FiberCache = {
    toFiberCache(bytes)
  }

  override def toDataFiberCache(bytes: Array[Byte]): FiberCache = {
    toFiberCache(bytes)
  }
  override def getEmptyDataFiberCache(length: Long): FiberCache = {
    val memoryBlock = allocate(length)
    val usableSize = getUsableSize(memoryBlock.getBaseOffset, memoryBlock.size())
    FiberCache(memoryBlock, usableSize)
  }

  @inline private def toFiberCache(bytes: Array[Byte]): FiberCache = {
    val memoryBlock = allocate(bytes.length)
    Platform.copyMemory(
      bytes,
      Platform.BYTE_ARRAY_OFFSET,
      memoryBlock.getBaseObject,
      memoryBlock.getBaseOffset,
      bytes.length)
    val usableSize = getUsableSize(memoryBlock.getBaseOffset, bytes.length)
    FiberCache(memoryBlock, usableSize)
  }

  override def stop(): Unit = {
    memoryManager.releaseStorageMemory(oapMemory, MemoryMode.OFF_HEAP)
  }
}
