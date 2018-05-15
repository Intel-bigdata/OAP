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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.unsafe.types.UTF8String

// TODO: make it an alias of MemoryBlock
case class FiberCache(protected val fiberData: MemoryBlock) extends Logging {

  // We use readLock to lock occupy. _refCount need be atomic to make sure thread-safe
  protected val _refCount = new AtomicLong(0)
  def refCount: Long = _refCount.get()

  def occupy(): Unit = {
    _refCount.incrementAndGet()
  }

  // TODO: seems we are safe even on lock for release.
  // 1. if we release fiber during another occupy. atomic refCount is thread-safe.
  // 2. if we release fiber during another tryDispose. the very last release lead to realDispose.
  def release(): Unit = {
    assert(refCount > 0, "release a non-used fiber")
    _refCount.decrementAndGet()
  }

  // TODO: Couple Fiber and FiberCache. Pass fiber as a parameter is weired.
  def tryDispose(fiber: Fiber, timeout: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    val writeLock = FiberLockManager.getFiberLock(fiber).writeLock()
    // Give caller a chance to deal with the long wait case.
    while (System.currentTimeMillis() - startTime <= timeout) {
      if (refCount != 0) {
        // LRU access (get and occupy) done, but fiber was still occupied by at least one reader,
        // so it needs to sleep some time to see if the reader done.
        // Otherwise, it becomes a polling loop.
        // TODO: use lock/sync-obj to leverage the concurrency APIs instead of explicit sleep.
        Thread.sleep(100)
      } else {
        if (writeLock.tryLock(200, TimeUnit.MILLISECONDS)) {
          try {
            if (refCount == 0) {
              realDispose(fiber)
              return true
            }
          } finally {
            writeLock.unlock()
          }
        }
      }
    }
    logWarning(s"Fiber Cache Dispose waiting detected for $fiber")
    false
  }

  protected var disposed = false
  def isDisposed: Boolean = disposed
  protected[filecache] def realDispose(fiber: Fiber): Unit = {
    if (!disposed) {
      MemoryManager.free(fiberData)
      FiberLockManager.removeFiberLock(fiber)
    }
    disposed = true
  }

  // For debugging
  def toArray: Array[Byte] = {
    // TODO: Handle overflow
    val bytes = new Array[Byte](fiberData.size().toInt)
    copyMemoryToBytes(0, bytes)
    bytes
  }

  protected def getBaseObj: AnyRef = {
    // NOTE: A trick here. Since every function need to get memory data has to get here first.
    // So, here check the if the memory has been freed.
    if (disposed) {
      throw new OapException("Try to access a freed memory")
    }
    fiberData.getBaseObject
  }
  protected def getBaseOffset: Long = fiberData.getBaseOffset

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

  def size(): Long = fiberData.size()
}

// TODO: Need modify `OapBitmapWrappedFiberCache` to not depend on this.
case class WrappedFiberCache(fc: FiberCache) {
  private var released = false

  def release(): Unit = synchronized {
    if (!released) {
      try {
        fc.release()
      } finally {
        released = true
      }
    }
  }
}

object FiberCache {
  //  For test purpose :convert Array[Byte] to FiberCache
  private[oap] def apply(data: Array[Byte]): FiberCache = {
    val memoryBlock = new MemoryBlock(data, Platform.BYTE_ARRAY_OFFSET, data.length)
    FiberCache(memoryBlock)
  }
}
