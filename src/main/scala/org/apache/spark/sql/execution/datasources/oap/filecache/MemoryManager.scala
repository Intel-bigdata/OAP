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

import java.io.ByteArrayInputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.ColumnValues
import org.apache.spark.sql.execution.datasources.oap.OapEnv
import org.apache.spark.storage.{BlockManager, TestBlockId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.{MemoryAllocator, MemoryBlock}
import org.apache.spark.unsafe.types.UTF8String

// TODO: make it an alias of MemoryBlock
trait FiberCache {
  def loadData(is: FiberInputStream): Unit = {
    val bytes = new Array[Byte](is.length)
    is.is match {
      case fsInputStream: FSDataInputStream => fsInputStream.readFully(is.offset, bytes)
      case bytesInputStream: ByteArrayInputStream =>
        bytesInputStream.skip(is.offset)
        bytesInputStream.read(bytes)
    }
    val memoryBlock = OapEnv.get.memoryManager.allocate(bytes.length)
    Platform.copyMemory(
      bytes,
      Platform.BYTE_ARRAY_OFFSET,
      memoryBlock.getBaseObject,
      memoryBlock.getBaseOffset,
      bytes.length)
    fiberData.setObjAndOffset(memoryBlock.getBaseObject, memoryBlock.getBaseOffset)
  }

  // In our design, fiberData should be a internal member.
  protected def fiberData: MemoryBlock

  // TODO: need a flag to avoid accessing disposed FiberCache
  private var disposed = false
  def isDisposed: Boolean = disposed
  def dispose(): Unit = {
    if (!disposed && fiberData.getBaseOffset != -1) OapEnv.get.memoryManager.free(fiberData)
    disposed = true
  }
  val lock = new ReentrantReadWriteLock()

  /** For debug purpose */
  def toArray: Array[Byte] = {
    // TODO: Handle overflow
    val bytes = new Array[Byte](fiberData.size().toInt)
    copyMemoryToBytes(0, bytes)
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

  def getBytes(offset: Long, length: Int): Array[Byte] = {
    val bytes = new Array[Byte](length)
    copyMemoryToBytes(offset, bytes)
    bytes
  }

  /** TODO: may cause copy memory from off-heap to on-heap, used by [[ColumnValues]] */
  private def copyMemory(offset: Long, dst: AnyRef, dstOffset: Long, length: Long): Unit =
    Platform.copyMemory(getBaseObj, getBaseOffset + offset, dst, dstOffset, length)

  def copyMemoryToLongs(offset: Long, dst: Array[Long]): Unit =
    copyMemory(offset, dst, Platform.LONG_ARRAY_OFFSET, dst.length * 8)

  def copyMemoryToInts(offset: Long, dst: Array[Int]): Unit =
    copyMemory(offset, dst, Platform.INT_ARRAY_OFFSET, dst.length * 4)

  def copyMemoryToBytes(offset: Long, dst: Array[Byte]): Unit =
    copyMemory(offset, dst, Platform.BYTE_ARRAY_OFFSET, dst.length)

  def size(): Long = fiberData.size()

  def release(): Unit = {
    lock.readLock().unlock()
  }
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

// Index fiber caching, only used internally by Oap
private[oap] case class IndexFiberCache(fiberData: MemoryBlock) extends FiberCache

/**
 * Memory Manager
 *
 * Acquire fixed amount of memory from spark during initialization.
 *
 * TODO: Should change object to class for better initialization.
 * For example, we can't test two MemoryManger in one test suite.
 */
private[oap] class MemoryManager(
    memoryManager: org.apache.spark.memory.MemoryManager,
    fraction: Double) extends Logging {

  require(memoryManager.maxOffHeapStorageMemory > 0)
  /**
   * Dummy block id to acquire memory from [[org.apache.spark.memory.MemoryManager]]
   *
   * NOTE: We do acquire some memory from Spark without adding a Block into[[BlockManager]]
   * It may cause consistent problem.
   * (i.e. total size of blocks in BlockManager is not equal to Spark used storage memory)
   */
  private val DUMMY_BLOCK_ID = TestBlockId("oap_memory_request_block")

  private val _maxMemory = {

    val oapMaxMemory = (memoryManager.maxOffHeapStorageMemory * fraction).toLong
    if (memoryManager.acquireStorageMemory(DUMMY_BLOCK_ID, oapMaxMemory, MemoryMode.OFF_HEAP)) {
      oapMaxMemory
    } else {
      throw new OapException("Can't acquire memory from spark Memory Manager")
    }
  }

  // TODO: Atomic is really needed?
  private val _memoryUsed = new AtomicLong(0)
  def memoryUsed: Long = _memoryUsed.get()
  def maxMemory: Long = _maxMemory

  def stop(): Unit = {
    memoryManager.releaseStorageMemory(maxMemory, MemoryMode.OFF_HEAP)
  }

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

  // Used by IndexFile
  // TODO: putToFiberCache(in: Stream, position: Long, length: Int, type: FiberType)
  def putToIndexFiberCache(in: FSDataInputStream, position: Long, length: Int): IndexFiberCache = {
    val bytes = new Array[Byte](length)
    in.readFully(position, bytes)

    val memoryBlock = allocate(bytes.length)
    Platform.copyMemory(
      bytes,
      Platform.BYTE_ARRAY_OFFSET,
      memoryBlock.getBaseObject,
      memoryBlock.getBaseOffset,
      bytes.length)
    IndexFiberCache(memoryBlock)
  }

  // Used by OapDataFile since we need to parse the raw data in on-heap memory before put it into
  // off-heap memory
  def putToDataFiberCache(bytes: Array[Byte]): DataFiberCache = {
    val memoryBlock = allocate(bytes.length)
    Platform.copyMemory(
      bytes,
      Platform.BYTE_ARRAY_OFFSET,
      memoryBlock.getBaseObject,
      memoryBlock.getBaseOffset,
      bytes.length)
    DataFiberCache(memoryBlock)
  }
}

object MemoryManager {
  // TODO: all configure parameters should be put in one place
  val OAP_OFF_HEAP_MEMORY_FRACTION = "spark.oap.memory.offHeap.fraction"
  val OAP_OFF_HEAP_MEMORY_FRACTION_DEFAULT = 0.7
}
