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

import scala.util.Random
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class CacheManagerSuite extends SparkFunSuite {

  val random = new Random(0)
  def generateData(size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    random.nextBytes(bytes)
    bytes
  }

  test("test CacheManager") {

    new SparkContext(
      "local[2]",
      "MemoryManagerSuite",
      new SparkConf().set("spark.memory.offHeap.size", "10k"))

    val configuration = new Configuration()

    val data1024 = generateData(1024)
    val data2048 = generateData(2048)
    val data4096 = generateData(4096)
    val data7168 = generateData(7168)
    val data10240 = generateData(10240)

    val fiber1024 = TestFiber(() => data1024, s"test fiber #1, size 1024")
    val fiber2048 = TestFiber(() => data2048, s"test fiber #2, size 2048")
    val fiber4096 = TestFiber(() => data4096, s"test fiber #3, size 4096")
    val fiber7168 = TestFiber(() => data7168, s"test fiber #4, size 7168")
    val fiber10240 = TestFiber(() => data10240, s"test fiber #5, size 10240")

    // Put 3 fibers into empty cache
    CacheManager.getOrElseUpdate(fiber1024, configuration)
    CacheManager.getOrElseUpdate(fiber2048, configuration)
    CacheManager.getOrElseUpdate(fiber4096, configuration)
    assert(CacheManager.getMissCount == 3)

    // Get 3 fibers from cache
    CacheManager.getOrElseUpdate(fiber1024, configuration)
    CacheManager.getOrElseUpdate(fiber2048, configuration)
    CacheManager.getOrElseUpdate(fiber4096, configuration)
    assert(CacheManager.getHitCount == 3)

    // Put 1 large fiber into cache will cause eviction
    CacheManager.getOrElseUpdate(fiber7168, configuration)
    assert(CacheManager.getMissCount == 4)
    // Get large fiber from cache
    CacheManager.getOrElseUpdate(fiber7168, configuration)
    assert(CacheManager.getHitCount == 4)

    // Get 3 removed fibers from cache
    CacheManager.getOrElseUpdate(fiber1024, configuration)
    CacheManager.getOrElseUpdate(fiber2048, configuration)
    CacheManager.getOrElseUpdate(fiber4096, configuration)
    assert(CacheManager.getMissCount == 7)

    // Put too large fiber into cache, will not remove other fibers
    CacheManager.getOrElseUpdate(fiber10240, configuration)
    assert(CacheManager.getMissCount == 8)
    CacheManager.getOrElseUpdate(fiber2048, configuration)
    assert(CacheManager.getMissCount == 8)
  }
}
