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

import scala.util.Random
import org.apache.spark.sql.execution.datasources.oap.OapEnv
import org.apache.spark.sql.test.SharedSQLContext

class FiberCacheManagerSuite extends SharedSQLContext {
  private val random = new Random(0)
  private def generateData(size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    random.nextBytes(bytes)
    bytes
  }
  sparkConf.set("spark.memory.offHeap.size", "100m")

  private def memoryManager = OapEnv.get.memoryManager
  private def fiberCacheManager = OapEnv.get.fiberCacheManager
  private def toInputStream(data: Array[Byte]): FiberInputStream = {
    FiberInputStream(new ByteArrayInputStream(data), 0, data.length)
  }

  test("unit test") {
    val MB: Double = 1024 * 1024
    val memorySizeInMB = (memoryManager.maxMemory / MB).toInt
    val origStats = fiberCacheManager.getStats
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(1024)
      val fiber =
        TestFiber(s"test fiber #$i")
      val fiberCache = fiberCacheManager.get(fiber, toInputStream(data))
      val fiberCache2 = fiberCacheManager.get(fiber, toInputStream(data))
      assert(fiberCache.toArray sameElements data)
      assert(fiberCache2.toArray sameElements data)
      fiberCache.release()
      fiberCache2.release()
    }
    val stats = fiberCacheManager.getStats.minus(origStats)
    assert(stats.missCount() == memorySizeInMB * 2)
    assert(stats.hitCount() == memorySizeInMB * 2)
    assert(stats.evictionCount() >= memorySizeInMB)
  }

  test("remove a fiber is in use") {
    val MB: Double = 1024 * 1024
    val memorySizeInMB = (memoryManager.maxMemory / MB).toInt

    val dataInUse = generateData(1024)
    val fiberInUse =
      TestFiber(s"test fiber #0")
    val fiberCacheInUse = fiberCacheManager.get(fiberInUse, toInputStream(dataInUse))
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(1024)
      val fiber = TestFiber(s"test fiber #$i")
      val fiberCache = fiberCacheManager.get(fiber, toInputStream(data))
      assert(fiberCache.toArray sameElements data)
      fiberCache.release()
    }
    assert(!fiberCacheInUse.isDisposed)
    fiberCacheInUse.release()
  }

  test("add a very large fiber") {
    val MB: Double = 1024 * 1024
    val memorySizeInMB = (memoryManager.maxMemory / MB).toInt
    // Cache concurrency is 4, means maximum ENTRY size is memory size / 4
    val data = generateData(memorySizeInMB * 1024 * 1024 / 8)
    val fiber =
      TestFiber(s"test fiber #0")
    val fiberCache = fiberCacheManager.get(fiber, toInputStream(data))
    fiberCache.release()
    assert(!fiberCache.isDisposed)

    val data1 = generateData(memorySizeInMB * 1024 * 1024 / 2)
    val fiber1 =
      TestFiber(s"test fiber #1")
    val fiberCache1 = fiberCacheManager.get(fiber1, toInputStream(data1))
    assert(fiberCache1 == null)
  }
}
