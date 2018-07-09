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

package org.apache.spark.sql.execution.vectorized

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryAllocator
import org.apache.spark.unsafe.types.UTF8String

class OnHeapColumnVectorFiberSuite extends SparkFunSuite with SharedOapContext with Logging {
  test("test dumpBytesToCache with nativeAddress") {
    val count = 100
    val vector = new OnHeapColumnVector(count, IntegerType)
    (0 until count).foreach(i => vector.putInt(i, i * 2))
    val memoryBlock = MemoryAllocator.UNSAFE.allocate((4 + 1) * count)
    val fiber = new OnHeapColumnVectorFiber(vector, count, IntegerType)
    val offset = memoryBlock.getBaseOffset
    fiber.dumpBytesToCache(memoryBlock.getBaseOffset)
    (0 until count).foreach(i => assert(Platform.getInt(null, offset + i * 4) == i * 2))
  }

  test("test dumpBytesToCache with FiberCache") {
    val count = 100
    val vector = new OnHeapColumnVector(count, StringType)
    (0 until count).map { i =>
      val utf8 = s"str$i".getBytes("utf8")
      vector.appendByteArray(utf8, 0, utf8.length)
    }
    val fiber = new OnHeapColumnVectorFiber(vector, count, StringType)
    val fiberCache = fiber.dumpBytesToCache()
    (0 until count).map { i =>
      val length = fiberCache.getInt(i * 4)
      val offset = fiberCache.getInt(count * 4 + i * 4)
      assert(fiberCache.getUTF8String(count * 9 + offset, length).
        equals(UTF8String.fromString(s"str$i")))
    }
  }

  test("test loadBytesFromCache") {
    val count = 100
    val vector = new OnHeapColumnVector(count, IntegerType)
    val fiber = new OnHeapColumnVectorFiber(vector, count, IntegerType)
    val memoryBlock = MemoryAllocator.UNSAFE.allocate((4 + 1) * count)
    val offset = memoryBlock.getBaseOffset
    (0 until count).foreach(i =>
      Platform.putInt(null, offset + i * 4, i * 2)
    )
    Platform.setMemory(offset + count * 4, 0, count)
    fiber.loadBytesFromCache(offset)
    (0 until count).foreach(i => assert(vector.getInt(i) == 2 * i ))
  }
}
