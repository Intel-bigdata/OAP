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
import org.apache.spark.sql.test.oap.SharedOapContext

class MemoryManagerConfigSuite extends SharedOapContext with Logging{

  override def afterAll(): Unit = {
    // restore oapSparkConf to default
    oapSparkConf.set("spark.oap.cache.strategy", "guava")
    oapSparkConf.set("spark.sql.oap.cache.memory.manager", "offheap")
    oapSparkConf.set("spark.sql.oap.mix.data.cache.backend", "guava")
  }

  test("guava cache with offheap memory manager") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "guava")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "offheap")
    val memoryManager = MemoryManager(sparkEnv)
    assert(memoryManager.isInstanceOf[OffHeapMemoryManager])

  }

  test("guava cache with pm memory manager") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "guava")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "pm")
    val memoryManager = MemoryManager(sparkEnv)
    assert(memoryManager.isInstanceOf[PersistentMemoryManager])
  }

  test("vmem with tmp memory manager") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "vmem")
    val memoryManager = MemoryManager(sparkEnv)
    assert(memoryManager.isInstanceOf[TmpDramMemoryManager])
  }

  test("vmem with memory manager set to pm") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "vmem")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "pm")
    val memoryManager = MemoryManager(sparkEnv)
    assert(memoryManager.isInstanceOf[TmpDramMemoryManager])
  }

  test("noevict with hybrid memory manager") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "noevict")
    val memoryManager = MemoryManager(sparkEnv)
    assert(memoryManager.isInstanceOf[HybridMemoryManager])
  }

  test("noevict with memory manager set to pm") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "noevict")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "pm")
    val memoryManager = MemoryManager(sparkEnv)
    assert(memoryManager.isInstanceOf[HybridMemoryManager])
  }

  test("mix cache with offheap as index memory manager") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "mix")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "mix")
    sparkEnv.conf.set("spark.sql.oap.mix.index.memory.manager", "offheap")
    val indexMemoryManager = MemoryManager(sparkEnv,
      OapConf.OAP_FIBERCACHE_STRATEGY, FiberType.INDEX)
    assert(indexMemoryManager.isInstanceOf[OffHeapMemoryManager])
  }

  test("mix cache with persistent memory as index memory manager") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "mix")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "mix")
    sparkEnv.conf.set("spark.sql.oap.mix.index.memory.manager", "pm")
    val indexMemoryManager = MemoryManager(sparkEnv,
      OapConf.OAP_FIBERCACHE_STRATEGY, FiberType.INDEX)
    assert(indexMemoryManager.isInstanceOf[PersistentMemoryManager])
  }

  test("mix cache with offheap as data memory manager") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "mix")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "mix")
    sparkEnv.conf.set("spark.sql.oap.mix.data.memory.manager", "offheap")
    val dataMemoryManager = MemoryManager(sparkEnv,
      OapConf.OAP_FIBERCACHE_STRATEGY, FiberType.DATA)
    assert(dataMemoryManager.isInstanceOf[OffHeapMemoryManager])
  }

  test("mix cache with pm as data memory manager") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "mix")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "mix")
    sparkEnv.conf.set("spark.sql.oap.mix.data.cache.backend", "guava")
    sparkEnv.conf.set("spark.sql.oap.mix.data.memory.manager", "pm")
    val dataMemoryManager = MemoryManager(sparkEnv,
      OapConf.OAP_FIBERCACHE_STRATEGY, FiberType.DATA)
    assert(dataMemoryManager.isInstanceOf[PersistentMemoryManager])
  }

  test("mix cache with separate memory manager using vmem as cache backend") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "mix")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "mix")
    sparkEnv.conf.set("spark.sql.oap.mix.data.cache.backend", "vmem")
    val dataMemoryManager = MemoryManager(sparkEnv,
      OapConf.OAP_FIBERCACHE_STRATEGY, FiberType.DATA)
    assert(dataMemoryManager.isInstanceOf[TmpDramMemoryManager])
  }

  test("mix cache with separate memory manager using guava as cache backend") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "mix")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "mix")
    sparkEnv.conf.set("spark.sql.oap.mix.data.memory.manager", "offheap")
    sparkEnv.conf.set("spark.sql.oap.mix.data.cache.backend", "guava")
    val dataMemoryManager = MemoryManager(sparkEnv,
      OapConf.OAP_FIBERCACHE_STRATEGY, FiberType.DATA)
    assert(dataMemoryManager.isInstanceOf[OffHeapMemoryManager])
  }

  test("mix cache with separate memory manager using incorrect guava setting") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "mix")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "mix")
    sparkEnv.conf.set("spark.sql.oap.mix.data.memory.manager", "hybrid")
    sparkEnv.conf.set("spark.sql.oap.mix.data.cache.backend", "guava")
    assertThrows[UnsupportedOperationException](
      MemoryManager(sparkEnv, OapConf.OAP_FIBERCACHE_STRATEGY, FiberType.DATA))
  }

  test("mix cache with separate memory manager using incorrect vmem setting") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "mix")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "mix")
    sparkEnv.conf.set("spark.sql.oap.mix.data.memory.manager", "hybrid")
    sparkEnv.conf.set("spark.sql.oap.mix.data.cache.backend", "vmem")
    val dataMemoryManager = MemoryManager(sparkEnv,
      OapConf.OAP_FIBERCACHE_STRATEGY, FiberType.DATA)
    assert(dataMemoryManager.isInstanceOf[TmpDramMemoryManager])
  }

  test("mix cache with unified memory manager pm") {
    val sparkEnv = SparkEnv.get
    sparkEnv.conf.set("spark.oap.cache.strategy", "mix")
    sparkEnv.conf.set("spark.sql.oap.cache.memory.manager", "pm")
    sparkEnv.conf.set("spark.sql.oap.mix.data.cache.backend", "vmem")
    val dataMemoryManager = MemoryManager(sparkEnv)
    assert(dataMemoryManager.isInstanceOf[PersistentMemoryManager])
  }
}
