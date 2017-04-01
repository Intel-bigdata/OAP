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

package org.apache.spark.sql.execution.datasources.spinach.filecache

import org.apache.spark.unsafe.memory.{MemoryAllocator, MemoryBlock}


// TODO: make it an alias of MemoryBlock
trait FiberCache {
  def fiberData: MemoryBlock
}

// Data fiber caching, the in-memory representation can be found at [[DataFiberBuilder]]
case class DataFiberCache(fiberData: MemoryBlock) extends FiberCache

// Index fiber caching, only used internally by Spinach
private[spinach] case class IndexFiberCacheData(
    fiberData: MemoryBlock, dataEnd: Long, rootOffset: Long) extends FiberCache

private[spinach] object MemoryManager {
  private val indexCapacity: Long = Long.MaxValue / 2
  private val dataCapacity: Long = Long.MaxValue / 2

  def allocate(numOfBytes: Int): DataFiberCache = {
    val fiberData = MemoryAllocator.UNSAFE.allocate(numOfBytes)
    DataFiberCache(fiberData)
  }

  def free(fiber: FiberCache): Unit = {
    MemoryAllocator.UNSAFE.free(fiber.fiberData)
  }

  def getCapacity(): Long = indexCapacity + dataCapacity
  def getIndexCacheCapacity(): Long = indexCapacity
  def getDataCacheCapacity(): Long = dataCapacity
}
