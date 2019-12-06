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
import java.util.concurrent.atomic.AtomicLong

import com.google.common.primitives.Ints
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

case class FiberCache(fiberData: MemoryBlockHolder) extends Logging {

  // This is and only is set in `cache() of OapCache`
  // TODO: make it immutable
  var fiberId: FiberId = _

  val DISPOSE_TIMEOUT = 3000

  // record every batch startAddress, endAddress and the boolean of whether compressed
  // and the child column vector length in CompressedBatchFiberInfo
  var fiberBatchedInfo: mutable.HashMap[Int, CompressedBatchedFiberInfo] = _
  // record whether the fiber is compressed
  var fiberCompressed: Boolean = false

  // This suppose to be used when data cache allocation failed
  var column: OnHeapColumnVector = null

  // This suppose to be used when index cache allocation failed
  var originByteArray: Array[Byte] = null

  // We use readLock to lock occupy. _refCount need be atomic to make sure thread-safe
  protected val _refCount = new AtomicLong(0)
  def refCount: Long = _refCount.get()

  def occupy(): Unit = {
    _refCount.incrementAndGet()
  }

  def isFailedMemoryBlock(): Boolean = {
    fiberData.cacheType == CacheEnum.FAIL
  }

  def setOriginByteArray(bytes: Array[Byte]): Unit = {
    if (isFailedMemoryBlock()) {
      this.originByteArray = bytes
    } else {
      throw new UnsupportedOperationException(
        "fiber cache original byte array can only be set when cache allocation failed")
    }
  }

  def getOriginByteArray(): Array[Byte] = {
    originByteArray
  }

  def setColumn(column: OnHeapColumnVector): Unit = {
    if (isFailedMemoryBlock()) {
      this.column = column;
    } else {
      throw new UnsupportedOperationException(
        "fiber cache column can only be set when cache allocation failed")
    }
  }

  def getColumn(): OnHeapColumnVector = {
    column
  }

  // TODO: seems we are safe even on lock for release.
  // 1. if we release fiber during another occupy. atomic refCount is thread-safe.
  // 2. if we release fiber during another tryDispose. the very last release lead to realDispose.
  def release(): Unit = {
    assert(refCount > 0, "release a non-used fiber")
    _refCount.decrementAndGet()
  }

  def tryDisposeWithoutWait(): Boolean = {
    require(fiberId != null, "FiberId shouldn't be null for this FiberCache")
    val writeLockOp = OapRuntime.get.map(_.fiberLockManager.getFiberLock(fiberId).writeLock())
    writeLockOp match {
      case None => return true // already stopped OapRuntime
      case Some(writeLock) =>
        if (refCount != 0) {
          // LRU access (get and occupy) done, but fiber was still occupied by at least one
          // reader, so it needs to sleep some time to see if the reader done.
          // Otherwise, it becomes a polling loop.
          // TODO: use lock/sync-obj to leverage the concurrency APIs instead of explicit sleep.
          return false
        } else {
          if (writeLock.tryLock(200, TimeUnit.MILLISECONDS)) {
            try {
              if (refCount == 0) {
                realDispose()
                return true
              }
            } finally {
              writeLock.unlock()
            }
          }
        }
    }
    logWarning(s"Fiber Cache Dispose waiting detected for $fiberId")
    false
  }

  def tryDispose(): Boolean = {
    require(fiberId != null, "FiberId shouldn't be null for this FiberCache")
    val startTime = System.currentTimeMillis()
    val writeLockOp = OapRuntime.get.map(_.fiberLockManager.getFiberLock(fiberId).writeLock())
    writeLockOp match {
      case None => return true // already stopped OapRuntime
      case Some(writeLock) =>
        // Give caller a chance to deal with the long wait case.
        while (System.currentTimeMillis() - startTime <= DISPOSE_TIMEOUT) {
          if (refCount != 0) {
            // LRU access (get and occupy) done, but fiber was still occupied by at least one
            // reader, so it needs to sleep some time to see if the reader done.
            // Otherwise, it becomes a polling loop.
            // TODO: use lock/sync-obj to leverage the concurrency APIs instead of explicit sleep.
            Thread.sleep(100)
          } else {
            if (writeLock.tryLock(200, TimeUnit.MILLISECONDS)) {
              try {
                if (refCount == 0) {
                  realDispose()
                  return true
                }
              } finally {
                writeLock.unlock()
              }
            }
          }
        }
    }
    logWarning(s"Fiber Cache Dispose waiting detected for $fiberId")
    false
  }

  protected var disposed = false
  def isDisposed: Boolean = disposed
  protected[filecache] def realDispose(): Unit = {
    if (!disposed) {
      if (!isFailedMemoryBlock()) {
        OapRuntime.get.foreach(_.memoryManager.free(fiberData))
      } else {
        this.column = null
        this.originByteArray = null
      }
      OapRuntime.get.foreach(_.fiberLockManager.removeFiberLock(fiberId))
    }
    disposed = true
  }

  // For debugging
  def toArray: Array[Byte] = {
    // TODO: Handle overflow
    val intSize = if (originByteArray != null) {
      originByteArray.length
    } else {
      Ints.checkedCast(size())
    }
    val bytes = new Array[Byte](intSize)
    copyMemoryToBytes(0, bytes)
    bytes
  }

  protected def getBaseObj: AnyRef = {
    // NOTE: A trick here. Since every function need to get memory data has to get here first.
    // So, here check the if the memory has been freed.
    if (disposed) {
      throw new OapException("Try to access a freed memory")
    }

    if (isFailedMemoryBlock && originByteArray != null) {
      originByteArray
    } else {
      fiberData.baseObject
    }
  }
  def getBaseOffset: Long = {
    if (isFailedMemoryBlock && originByteArray != null) {
      Platform.BYTE_ARRAY_OFFSET
    } else {
      fiberData.baseOffset
    }
  }

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

  private def copyMemoryToBytes(offset: Long, dst: Array[Byte]): Unit = {
    Platform.copyMemory(
      getBaseObj, getBaseOffset + offset, dst, Platform.BYTE_ARRAY_OFFSET, dst.length)
  }

  def size(): Long = fiberData.length

  // Return the occupied size and it's typically larger than the required data size due to memory
  // alignments from underlying allocator
  def getOccupiedSize(): Long = fiberData.occupiedSize

  def setMemBlockCacheType(cacheType: CacheEnum.CacheEnum): FiberCache = {
    this.fiberData.cacheType = cacheType
    this
  }
}

class DecompressBatchedFiberCache (
     override val fiberData: MemoryBlockHolder, var batchedCompressed: Boolean = false,
     fiberCache: FiberCache) extends FiberCache (fiberData = fiberData) {
  override  def release(): Unit = if (fiberCache !=  null) {
      fiberCache.release()
    }
}

case class CompressedBatchedFiberInfo(
    startAddress: Long, endAddress: Long,
    compressed: Boolean, length: Long)

object FiberCache {
  //  For test purpose :convert Array[Byte] to FiberCache
  private[oap] def apply(data: Array[Byte]): FiberCache = {
    val memoryBlockHolder =
      MemoryBlockHolder(
        CacheEnum.GENERAL,
        data,
        Platform.BYTE_ARRAY_OFFSET,
        data.length,
        data.length)
    FiberCache(memoryBlockHolder)
  }
}
