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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.storage.{BlockManager, TestBlockId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.{MemoryAllocator, MemoryBlock}

// TODO: make it an alias of MemoryBlock
trait FiberCache {
  // In our design, fiberData should be a internal member.
  protected def fiberData: MemoryBlock

  // If MemoryManager has no off-heap memory, FiberCache can maintain a Array[Byte] as MemoryBlock
  def isOffHeap: Boolean = fiberData.getBaseObject == null

  // TODO: need a flag to avoid accessing disposed FiberCache
  def dispose(): Unit = if (isOffHeap) MemoryManager.free(fiberData)

  /** For debug purpose */
  def toArray: Array[Byte] = {
    // TODO: Handle overflow
    val bytes = new Array[Byte](fiberData.size().toInt)
    Platform.copyMemory(
      fiberData.getBaseObject,
      fiberData.getBaseOffset,
      bytes,
      Platform.BYTE_ARRAY_OFFSET,
      bytes.length)
    bytes
  }

  def size(): Long = fiberData.size()
}

// Data fiber caching, the in-memory representation can be found at [[DataFiberBuilder]]
case class DataFiberCache(fiberData: MemoryBlock) extends FiberCache

// TODO: This class seems not needed.
// Because After index partial loading enabled, each MemoryBlock is only for its specific part.
// Index fiber caching, only used internally by Oap
private[oap] case class IndexFiberCacheData(
    fiberData: MemoryBlock, dataEnd: Long, rootOffset: Long) extends FiberCache

/** Memory Manager
 *
 *  Each component wants to use [[MemoryManager.allocate]], must give a function to free space.
 *
 *  Acquire fixed amount memory from spark during initialization.
 *  Otherwise, it's possible that OAP acquired all spark's memory and never release.
 *
 *  TODO: Should change object to class for better initialization.
 *  For example, we can't test two MemoryManger in one test suite.
 */
private[oap] object MemoryManager extends Logging {

  /** Dummy block id to acquire memory from [[org.apache.spark.memory.MemoryManager]]
   *
   *  NOTE: We do acquire some memory from Spark without adding a Block into[[BlockManager]]
   *  It may cause consistent problem.
   *  (i.e. total size of blocks in BlockManager is not equal to Spark used storage memory)
   */
  private val DUMMY_BLOCK_ID = TestBlockId("oap_memory_request_block")

  // TODO: a config to control max memory size
  private val maxMemory = {
    val sparkMemoryManager = if (SparkEnv.get == null) None else Some(SparkEnv.get.memoryManager)
    sparkMemoryManager.map { memoryManager =>
      val oapMaxMemory = (memoryManager.maxOffHeapStorageMemory * 0.7).toLong
      if (memoryManager.acquireStorageMemory(DUMMY_BLOCK_ID, oapMaxMemory, MemoryMode.OFF_HEAP)) {
        oapMaxMemory
      } else {
        0L
      }
    }.getOrElse {
      logWarning("No Spark MemoryManager was found. Can't allocate any Off-heap Memory!")
      0L
    }
  }

  // TODO: Atomic is really needed?
  private val _memoryUsed = new AtomicLong(0)
  private def memoryFree = maxOffHeapMemory - memoryUsed
  def memoryUsed: Long = _memoryUsed.get()
  def maxOffHeapMemory: Long = maxMemory

  /** Invoke freeSpace function in [[FiberCacheManager]] to evict unused caches.
   *
   *  Leave it empty for now
   *  TODO: finish this after Cache Manager is ready.
   */
  private def freeSpace(space: Long): Long = CacheManager.evictToFreeSpace(space)

  /** Set to private, since putToFiberCache is enough for Cache Manager to request memory */
  private[filecache] def allocate(numOfBytes: Int): Option[MemoryBlock] = {
    // First free some memory from cache if needed
    if (memoryFree < numOfBytes) {
      freeSpace(numOfBytes - memoryFree)
    }
    // Check again if we have enough memory
    if (memoryFree >= numOfBytes) {
      _memoryUsed.getAndAdd(numOfBytes)
      logDebug(s"allocate $numOfBytes memory, remaining: $memoryFree")
      Some(MemoryAllocator.UNSAFE.allocate(numOfBytes))
    } else {
      logWarning(s"no enough memory, remaining: $memoryFree")
      None
    }
  }

  /** Set to private, Since only [[FiberCache.dispose()]] should invoke this */
  private[filecache] def free(memoryBlock: MemoryBlock): Unit = {
    MemoryAllocator.UNSAFE.free(memoryBlock)
    _memoryUsed.getAndAdd(-memoryBlock.size())
    logDebug(s"freed ${memoryBlock.size()} memory, remaining: $memoryFree")
  }

  /** Put on-heap data into off-heap memory.
   *
   * allocated [[MemoryBlock]] is wrapped by [[FiberCache]] for generic data access.
   * if there is no enough off-heap memory, [[MemoryBlock]] is only a wrapper of Array[Byte]
   */
  def putToFiberCache(data: Array[Byte]): FiberCache = {
    val memoryBlock: Option[MemoryBlock] = allocate(data.length)
    memoryBlock.map { memoryBlock =>
      Platform.copyMemory(
        data,
        Platform.BYTE_ARRAY_OFFSET,
        memoryBlock.getBaseObject,
        memoryBlock.getBaseOffset,
        data.length)
      DataFiberCache(memoryBlock)
    }.getOrElse(DataFiberCache(new MemoryBlock(data, Platform.BYTE_ARRAY_OFFSET, data.length)))
  }
}
