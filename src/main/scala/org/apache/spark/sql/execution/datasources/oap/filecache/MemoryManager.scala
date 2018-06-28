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

/**
 * Memory Manager
 *
 * Acquire fixed amount of memory from spark during initialization.
 */
private[sql] class MemoryManager(sparkEnv: SparkEnv) extends Logging {

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
  def memoryUsed: Long = _memoryUsed.get()

  def cacheMemory: Long = _cacheMemory
  def cacheGuardianMemory: Long = _cacheGuardianMemory

  private[filecache] def allocate(numOfBytes: Long): MemoryBlock = {
    _memoryUsed.getAndAdd(numOfBytes)
    logDebug(s"allocate $numOfBytes memory, used: $memoryUsed")
    MemoryAllocator.UNSAFE.allocate(numOfBytes)
  }

  private[filecache] def free(memoryBlock: MemoryBlock): Unit = {
    MemoryAllocator.UNSAFE.free(memoryBlock)
    _memoryUsed.getAndAdd(-memoryBlock.size())
    logDebug(s"freed ${memoryBlock.size()} memory, used: $memoryUsed")
  }

  // Used by IndexFile
  def toIndexFiberCache(in: FSDataInputStream, position: Long, length: Int): FiberCache = {
    val bytes = new Array[Byte](length)
    in.readFully(position, bytes)
    toFiberCache(bytes)
  }

  // Used by IndexFile. For decompressed data
  def toIndexFiberCache(bytes: Array[Byte]): FiberCache = {
    toFiberCache(bytes)
  }

  // Used by OapDataFile since we need to parse the raw data in on-heap memory before put it into
  // off-heap memory
  def toDataFiberCache(bytes: Array[Byte]): FiberCache = {
    toFiberCache(bytes)
  }

  private def toFiberCache(bytes: Array[Byte]): FiberCache = {
    val memoryBlock = allocate(bytes.length)
    Platform.copyMemory(
      bytes,
      Platform.BYTE_ARRAY_OFFSET,
      memoryBlock.getBaseObject,
      memoryBlock.getBaseOffset,
      bytes.length)
    FiberCache(memoryBlock)
  }

  def getEmptyDataFiberCache(length: Long): FiberCache = {
    val memoryBlock = allocate(length)
    FiberCache(memoryBlock)
  }

  def stop(): Unit = {
    memoryManager.releaseStorageMemory(oapMemory, MemoryMode.OFF_HEAP)
  }
}

private[filecache] object MemoryManager {
  /**
   * Dummy block id to acquire memory from [[org.apache.spark.memory.MemoryManager]]
   *
   * NOTE: We do acquire some memory from Spark without adding a Block into[[BlockManager]]
   * It may cause consistent problem.
   * (i.e. total size of blocks in BlockManager is not equal to Spark used storage memory)
   *
   * TODO should avoid using [[TestBlockId]]
   */
  private val DUMMY_BLOCK_ID = TestBlockId("oap_memory_request_block")
}
