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

import com.google.common.cache.CacheStats
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.SparkFunSuite

class FiberCacheManagerSuite extends SparkFunSuite {

  test("Default Size") {
    // Default cache memory is set to 300MB
    val configuration = new Configuration()
    configuration.setBoolean(SQLConf.SPINACH_FIBERCACHE_STATS.key, true)

    TestFiberCacheManger.reset()
    (0 until 150).foreach { i =>
      TestFiberCacheManger(TestFiber(i), configuration): DataFiberCache
    }
    assert(TestFiberCacheManger.getStat.evictionCount() > 0)
    assert(TestFiberCacheManger.getStat.evictionCount() < 20)
  }

  test("1GB Size") {
    val configuration = new Configuration()
    configuration.setBoolean(SQLConf.SPINACH_FIBERCACHE_STATS.key, true)
    configuration.setLong(SQLConf.SPINACH_FIBERCACHE_SIZE.key, 1024 * 1024)

    TestFiberCacheManger.reset()
    (0 until 512).foreach{ i =>
      TestFiberCacheManger(TestFiber(i), configuration): DataFiberCache
    }
    assert(TestFiberCacheManger.getStat.evictionCount() > 0)
    assert(TestFiberCacheManger.getStat.evictionCount() < 40)
  }

  test("8GB Size, Guava overflow issue if weigher use Byte as Unit (#239)") {
    val configuration = new Configuration()
    configuration.setBoolean(SQLConf.SPINACH_FIBERCACHE_STATS.key, true)
    configuration.setLong(SQLConf.SPINACH_FIBERCACHE_SIZE.key, 8 * 1024 * 1024)

    TestFiberCacheManger.reset()
    (0 until 4 * 1024).foreach{ i =>
      TestFiberCacheManger(TestFiber(i), configuration): DataFiberCache
    }
    assert(TestFiberCacheManger.getStat.evictionCount() > 0)
    assert(TestFiberCacheManger.getStat.evictionCount() < 100)
  }

  test("1TB Size, Enough size for most scenario") {
    val configuration = new Configuration()
    configuration.setBoolean(SQLConf.SPINACH_FIBERCACHE_STATS.key, true)
    configuration.setLong(SQLConf.SPINACH_FIBERCACHE_SIZE.key, 1024 * 1024 * 1024)

    TestFiberCacheManger.reset()
    (0 until 512 * 1024).foreach{ i =>
      TestFiberCacheManger(TestFiber(i), configuration): DataFiberCache
    }
    assert(TestFiberCacheManger.getStat.evictionCount() > 0)
    assert(TestFiberCacheManger.getStat.evictionCount() < 1000)
  }

  test("4TB Size, Exceed the maximum memory limit") { // Weight 4G = 2M * 2 * 1024
    val configuration = new Configuration()
    configuration.setBoolean(SQLConf.SPINACH_FIBERCACHE_STATS.key, true)
    configuration.setLong(SQLConf.SPINACH_FIBERCACHE_SIZE.key, 4 * 1024 * 1024 * 1024L)

    TestFiberCacheManger.reset()
    (0 until 2 * 1024 * 1024 + 1).foreach { i =>
      TestFiberCacheManger(TestFiber(i), configuration): DataFiberCache
    }
    assert(TestFiberCacheManger.getStat.evictionCount() > 0)
  }
}

case class TestFiber(id: Int) extends Fiber

object TestFiberCacheManger extends AbstractFiberCacheManger {

  // 2 MB test fiber data
  private val testFiberData = MemoryManager.allocate(2 * 1024 * 1024)

  override def fiber2Data(fiber: Fiber, conf: Configuration): FiberCache = {
    fiber match {
      case TestFiber(_) => testFiberData
    }
  }

  override def freeFiberData(fiberCache: FiberCache): Unit = {}

  def getStat: CacheStats = cache.stats()

  def reset(): Unit = cache = null
}
