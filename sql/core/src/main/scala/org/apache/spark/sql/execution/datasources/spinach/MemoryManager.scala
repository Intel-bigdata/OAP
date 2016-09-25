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

import org.apache.spark.unsafe.memory.{MemoryAllocator, MemoryBlock}

// TODO: make it an alias of MemoryBlock
case class FiberCacheData(fiberData: MemoryBlock)

/**
 * Used to cache data in MemoryBlock (on-heap or off-heap)
 */
private[spinach] case class IndexFiberCacheData(
    fiberData: MemoryBlock, dataEnd: Int, rootOffset: Int)

private[spinach] trait MemoryMode
private[spinach] case object OffHeap extends MemoryMode
private[spinach] case object OnHeap extends MemoryMode

private[spinach] object MemoryManager {
  // TODO make it configurable
  // TODO temporarily using Long.MaxValue
  val capacity: Long = Long.MaxValue
  var maxMemoryInByte: Long = capacity
  var memoryMode: MemoryMode = OffHeap

  def getMemoryMode: MemoryMode = memoryMode

  def setMemoryMode(memoryMode: MemoryMode): Unit = {
    this.memoryMode = memoryMode
  }

  def allocate(numOfBytes: Int): FiberCacheData = synchronized {
    if (maxMemoryInByte - numOfBytes >= 0) {
      maxMemoryInByte -= numOfBytes
      val fiberData = memoryMode match {
        case OnHeap => MemoryAllocator.HEAP.allocate(numOfBytes)
        case OffHeap => MemoryAllocator.UNSAFE.allocate(numOfBytes)
        case _ => MemoryAllocator.HEAP.allocate(numOfBytes)
      }
      FiberCacheData(fiberData)
    } else {
      null
    }
  }

  def free(fiber: FiberCacheData): Unit = synchronized {
    memoryMode match {
      case OnHeap => MemoryAllocator.HEAP.free(fiber.fiberData)
      case OffHeap => MemoryAllocator.UNSAFE.free(fiber.fiberData)
      case _ => MemoryAllocator.HEAP.free(fiber.fiberData)
    }
    maxMemoryInByte += fiber.fiberData.size()
  }

  def getCapacity(): Long = capacity

  def remain(): Long = maxMemoryInByte
}
