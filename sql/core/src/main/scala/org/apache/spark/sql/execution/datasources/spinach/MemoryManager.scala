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

package org.apache.spark.sql.execution.datasources.spinach

private[spinach] trait MemoryManager {
  def maxMemoryInByte: Long
  def allocate(numOfBytes: Int): FiberByteData
  def free(fiber: FiberByteData)
  def remain(): Long
}

private[spinach] trait MemoryMode
private[spinach] case object OffHeap extends MemoryMode
private[spinach] case object OnHeap extends MemoryMode

private[spinach] class OnHeapMemoryManager(var maxMemoryInByte: Long) extends MemoryManager {
  def allocate(numOfBytes: Int): FiberByteData = synchronized {
    if (maxMemoryInByte - numOfBytes >= 0) {
      maxMemoryInByte -= numOfBytes
      FiberByteData(new Array[Byte](numOfBytes))
    } else {
      null
    }
  }

  def free(fiber: FiberByteData): Unit = synchronized {
    maxMemoryInByte += fiber.buf.length
  }

  def remain(): Long = maxMemoryInByte
}

private[spinach] class OffHeapMemoryManager(var maxMemoryInByte: Long) extends MemoryManager {
  def allocate(numOfBytes: Int): FiberByteData = synchronized {
    throw new NotImplementedError("")
  }

  def free(fiber: FiberByteData): Unit = synchronized {
    throw new NotImplementedError("")
  }

  def remain(): Long = maxMemoryInByte
}

private[spinach] object MemoryManager {
  // TODO make it configurable
  val SPINACH_FIBER_CACHE_SIZE_IN_BYTES: Long = 1024 * 1024 * 1024L // 1GB
  val MEMORY_MODE: MemoryMode = OnHeap

  val instance: MemoryManager = MEMORY_MODE match {
    case OnHeap => new OnHeapMemoryManager(SPINACH_FIBER_CACHE_SIZE_IN_BYTES)
    case OffHeap => new OffHeapMemoryManager(SPINACH_FIBER_CACHE_SIZE_IN_BYTES)
    case a => throw new NotImplementedError(a.toString)
  }
}
