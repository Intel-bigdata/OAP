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

import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.ColumnValues
import org.apache.spark.storage.{BlockManager, TestBlockId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.{MemoryAllocator, MemoryBlock}
import org.apache.spark.unsafe.types.UTF8String

// TODO: make it an alias of MemoryBlock
trait FiberCache {
  /*
  def updateFiberData(fiberData: MemoryBlock): Unit = {
    this.fiberData.setObjAndOffset(fiberData.getBaseObject, fiberData.getBaseOffset)
  }
  */

  // In our design, fiberData should be a internal member.
  protected def fiberData: MemoryBlock

  // TODO: need a flag to avoid accessing disposed FiberCache
  private var disposed = false
  def dispose(): Unit = {
    if (!disposed) MemoryManager.free(fiberData)
    disposed = true
  }

  def isDisposed: Boolean = disposed
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

  private def getBaseObj: AnyRef = {
    // NOTE: A trick here. Since every function need to get memory data has to get here first.
    // So, here check the if the memory has been freed.
    if (disposed) throw new OapException("Try to access a freed memory")
    fiberData.getBaseObject
  }
  private def getBaseOffset: Long = fiberData.getBaseOffset

  def getBoolean(offset: Long): Boolean = Platform.getBoolean(getBaseObj, getBaseOffset + offset)
  def getByte(offset: Long): Byte = Platform.getByte(getBaseObj, getBaseOffset + offset)
  def getInt(offset: Long): Int = Platform.getInt(getBaseObj, getBaseOffset + offset)
  def getDouble(offset: Long): Double = Platform.getDouble(getBaseObj, getBaseOffset + offset)
  def getLong(offset: Long): Long = Platform.getLong(getBaseObj, getBaseOffset + offset)
  def getShort(offset: Long): Short = Platform.getShort(getBaseObj, getBaseOffset + offset)
  def getFloat(offset: Long): Float = Platform.getFloat(getBaseObj, getBaseOffset + offset)
  def getUTF8String(offset: Long, length: Int): UTF8String =
    UTF8String.fromAddress(getBaseObj, getBaseOffset + offset, length)
  /** TODO: may cause copy memory from off-heap to on-heap, used by [[ColumnValues]] */
  def copyMemory(offset: Long, dst: AnyRef, dstOffset: Long, length: Long): Unit =
    Platform.copyMemory(getBaseObj, getBaseOffset + offset, dst, dstOffset, length)

  def size(): Long = fiberData.size()
}

object FiberCache {
  // Give test suite a way to convert Array[Byte] to FiberCache. For test purpose.
  private[oap] def apply(data: Array[Byte]): FiberCache = {
    val memoryBlock = new MemoryBlock(data, Platform.BYTE_ARRAY_OFFSET, data.length)
    DataFiberCache(memoryBlock)
  }
}

// Data fiber caching, the in-memory representation can be found at [[DataFiberBuilder]]
case class DataFiberCache(fiberData: MemoryBlock) extends FiberCache

// TODO: This class seems not needed.
// Because After index partial loading enabled, each MemoryBlock is only for its specific part.
// Index fiber caching, only used internally by Oap
private[oap] case class IndexFiberCacheData(
    fiberData: MemoryBlock, dataEnd: Long, rootOffset: Long) extends FiberCache

/**
 * Memory Manager
 *
 * Acquire fixed amount memory from spark during initialization.
 *
 * TODO: Should change object to class for better initialization.
 * For example, we can't test two MemoryManger in one test suite.
 */
private[oap] object MemoryManager extends Logging {

  /**
   * Dummy block id to acquire memory from [[org.apache.spark.memory.MemoryManager]]
   *
   * NOTE: We do acquire some memory from Spark without adding a Block into[[BlockManager]]
   * It may cause consistent problem.
   * (i.e. total size of blocks in BlockManager is not equal to Spark used storage memory)
   */
  private val DUMMY_BLOCK_ID = TestBlockId("oap_memory_request_block")

  // TODO: a config to control max memory size
  private val _maxMemory = {
    val sparkMemoryManager = if (SparkEnv.get == null) None else Some(SparkEnv.get.memoryManager)
    sparkMemoryManager.flatMap { memoryManager =>
      val oapMaxMemory = (memoryManager.maxOffHeapStorageMemory * 0.7).toLong
      if (memoryManager.acquireStorageMemory(DUMMY_BLOCK_ID, oapMaxMemory, MemoryMode.OFF_HEAP)) {
        Some(oapMaxMemory)
      } else {
        None
      }
    }.getOrElse {
      throw new OapException("Can't acquire memory from spark Memory Manager")
    }
  }

  // TODO: Atomic is really needed?
  private val _memoryUsed = new AtomicLong(0)
  def memoryUsed: Long = _memoryUsed.get()
  def maxMemory: Long = _maxMemory

  private[filecache] def allocate(numOfBytes: Int): MemoryBlock = {
    _memoryUsed.getAndAdd(numOfBytes)
    logDebug(s"allocate $numOfBytes memory, used: $memoryUsed")
    MemoryAllocator.UNSAFE.allocate(numOfBytes)
  }

  private[filecache] def free(memoryBlock: MemoryBlock): Unit = {
    MemoryAllocator.UNSAFE.free(memoryBlock)
    _memoryUsed.getAndAdd(-memoryBlock.size())
    logDebug(s"freed ${memoryBlock.size()} memory, used: $memoryUsed")
  }
}
