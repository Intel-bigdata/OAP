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

import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.unsafe.types.UTF8String

case class FiberCache(val fiberId: FiberId, protected val fiberData: MemoryBlock) extends Logging {

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
    if (refCount == 0 && fiberData != null &&
      OapRuntime.getOrCreate.fiberCacheManager.removeFromEvictedQueue(fiberId, this)) {
      realDispose()
    }
  }

  private var disposed: Boolean = false
  def isDisposed(): Boolean = disposed
  def realDispose(): Unit = {
    if (!disposed) {
      disposed = true
      OapRuntime.get.foreach(_.memoryManager.free(fiberData))
    }
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
      throw new OapException("The memory is freed already.")
    }
    fiberData.getBaseObject
  }
  def getBaseOffset: Long = fiberData.getBaseOffset

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

object FiberCache {
  // For test purpose.
  private[oap] def apply(out: ByteArrayOutputStream): FiberCache = {
    val bytes = out.toByteArray
    val memoryBlock = new MemoryBlock(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length)
    FiberCache(null, memoryBlock)
  }
}
