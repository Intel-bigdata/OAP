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

package com.intel.oap.common.unsafe;

import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import sun.nio.ch.DirectBuffer;
import sun.misc.Unsafe;

public class PersistentMemoryPlatformTest {

  private static long PMEM_SIZE = 32 * 1024 * 1024;
  private static String PATH = "/mnt/pmem";

  @Before
  public void setUp() {
    PersistentMemoryPlatform.initialize(PATH, PMEM_SIZE, 0);
  }

  @Test
  public void testCopyMemoryFromOnHeapToPmem() {
    int size = 3 * 1024 * 1024;
    byte[] src = new byte[size];
    byte[] dst = new byte[size];
    for (int i = 0; i < size; ++i) {
      src[i] = (byte)i;
      dst[i] = (byte)(255 - i);
    }
    long pmAddress = PersistentMemoryPlatform.allocateVolatileMemory(size);
    PersistentMemoryPlatform.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET,
        null, pmAddress, size);
    PersistentMemoryPlatform.copyMemory(null, pmAddress,
        dst, Unsafe.ARRAY_BYTE_BASE_OFFSET, size);
    Assert.assertArrayEquals(src, dst);
    PersistentMemoryPlatform.freeMemory(pmAddress);
  }

  @Test
  public void testCopyMemoryFromOffHeapToPmem() {
    int size = 3 * 1024 * 1024 + 45;
    byte[] src = new byte[size];
    byte[] dst = new byte[size];
    for (int i = 0; i < size; ++i) {
      src[i] = (byte)i;
      dst[i] = (byte)(255 - i);
    }
    ByteBuffer bb = ByteBuffer.allocateDirect(size);
    long addr = ((DirectBuffer)bb).address();
    bb.put(src);
    long pmAddress = PersistentMemoryPlatform.allocateVolatileMemory(size);
    PersistentMemoryPlatform.copyMemory(null, addr,
        null, pmAddress, size);
    PersistentMemoryPlatform.copyMemory(null, pmAddress,
        dst, Unsafe.ARRAY_BYTE_BASE_OFFSET, size);
    Assert.assertArrayEquals(src, dst);
    PersistentMemoryPlatform.freeMemory(pmAddress);
  }
}

