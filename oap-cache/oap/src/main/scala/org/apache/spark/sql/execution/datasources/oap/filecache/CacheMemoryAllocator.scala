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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.oap.OapConf

/**
 * A cache memory allocator which handles memory allocating from underlayer
 * memory manager and handles index and data cache seperation.
 */
private[filecache] class CacheMemoryAllocator(sparkEnv: SparkEnv)
  extends Logging {
  private val _separateMemory = checkSeparateMemory()
  private val (memoryManager, indexMemoryManager) = init()
  private val (_dataCacheMemorySize, _indexCacheMemorySize,
              _dataCacheGuardianMemorySize, _indexCacheGuardianMemorySize) = calculateSizes()

  private def checkSeparateMemory(): Boolean = {
    val memoryManagerOpt =
      sparkEnv.conf.get(OapConf.OAP_FIBERCACHE_MEMORY_MANAGER.key, "offheap").toLowerCase
    val cacheName =
      sparkEnv.conf.get(OapConf.OAP_FIBERCACHE_STRATEGY.key, "guava").toLowerCase

    memoryManagerOpt match {
      case ("mix") => if (cacheName.equals("mix")) {
        true
      } else {
        throw new UnsupportedOperationException("In order to enable mix memory manager," +
          "you need also to set to spark.oap.cache.strategy to mix")
      }
      case _ => false
    }
  }
  private def calculateSizes(): (Long, Long, Long, Long) = {
    // TO DO: make the 0.9 : 0.1 ration configurable
    if (_separateMemory) {
      ((memoryManager.memorySize * 0.9).toLong, (indexMemoryManager.memorySize * 0.9).toLong,
        (memoryManager.memorySize * 0.1).toLong, (indexMemoryManager.memorySize * 0.1).toLong)
    } else {
      val cacheRatio = sparkEnv.conf.getDouble(
        OapConf.OAP_DATAFIBER_USE_FIBERCACHE_RATIO.key,
        OapConf.OAP_DATAFIBER_USE_FIBERCACHE_RATIO.defaultValue.get)
      require(cacheRatio >= 0 && cacheRatio <= 1,
        "Data and index cache ratio should be between 0 and 1")

      val memorySize = memoryManager.memorySize
      ((memorySize * 0.9 * cacheRatio).toLong,
        (memorySize * 0.9 * (1 - cacheRatio)).toLong,
        (memorySize * 0.1).toLong, 0)
    }
  }

  private def init(): (MemoryManager, MemoryManager) = {
    if (_separateMemory) {
      val dataManager = MemoryManager(sparkEnv, OapConf.OAP_FIBERCACHE_STRATEGY, FiberType.DATA)
      val indexManager = MemoryManager(sparkEnv, OapConf.OAP_FIBERCACHE_STRATEGY, FiberType.INDEX)
      if (indexManager.getClass.equals(dataManager.getClass)) {
        throw new UnsupportedOperationException(
          "Index Cache type and Data Cache type need to be different in Mixed mode")
      }

      (dataManager, indexManager)
    } else {
      val manager = MemoryManager(sparkEnv)
      (manager, null)
    }
  }

  def allocateDataMemory(size: Long): MemoryBlockHolder = {
    memoryManager.allocate(size)
  }

  def allocateIndexMemory(size: Long): MemoryBlockHolder = {
    if (_separateMemory) {
      indexMemoryManager.allocate(size)
    } else {
      memoryManager.allocate(size)
    }
  }

  def freeDataMemory(block: MemoryBlockHolder): Unit = {
    memoryManager.free(block)
  }

  def freeIndexMemory(block: MemoryBlockHolder): Unit = {
    if (_separateMemory) {
      indexMemoryManager.free(block)
    } else {
      memoryManager.free(block)
    }
  }

  def isDcpmmUsed(): Boolean = {
    memoryManager.isDcpmmUsed() ||
    (indexMemoryManager != null && indexMemoryManager.isDcpmmUsed())
  }

  def stop(): Unit = {
    memoryManager.stop()
    if (_separateMemory) {
      indexMemoryManager.stop()
    }
  }

  def dataCacheMemorySize: Long = _dataCacheMemorySize
  def indexCacheMemorySize: Long = _indexCacheMemorySize
  def dataCacheGuardianMemorySize: Long = _dataCacheGuardianMemorySize
  def indexCacheGuardianMemorySize: Long = _indexCacheGuardianMemorySize
  def separateMemory: Boolean = _separateMemory
}

private[sql] object CacheMemoryAllocator {
  def apply(sparkEnv: SparkEnv): CacheMemoryAllocator = {
    new CacheMemoryAllocator(sparkEnv)
  }
}
