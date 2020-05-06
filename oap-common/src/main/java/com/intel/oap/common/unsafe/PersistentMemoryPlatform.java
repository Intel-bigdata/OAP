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

import com.google.common.base.Preconditions;
import com.intel.oap.common.util.NativeLibraryLoader;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import sun.misc.Cleaner;

/**
 * A platform used to allocate/free volatile memory from
 * <a href="https://en.wikipedia.org/wiki/Persistent_memory>Persistent Memory</a></a>
 * e.g. Intel Optane DC persistent memory.
 */
public class PersistentMemoryPlatform {

  private static volatile boolean initialized = false;
  private static final String LIBNAME = "pmplatform";
  static {
    NativeLibraryLoader.load(LIBNAME);
  }

  /**
   * Initialize the persistent memory.
   * @param path The initial path which should be a directory.
   * @param size The initial size
   */
  public static void initialize(String path, long size, int pattern) {
    synchronized (PersistentMemoryPlatform.class) {
      if (!initialized) {
        Preconditions.checkNotNull(path, "Persistent memory initial path can't be null");
        File dir = new File(path);
        Preconditions.checkArgument(dir.exists() && dir.isDirectory(), "Persistent memory " +
          "initial path should be a directory");
        Preconditions.checkArgument(size > 0,
          "Persistent memory initial size must be a positive number");
        try {
          initializeNative(path, size, pattern);
        } catch (Exception e) {
          throw new ExceptionInInitializerError("Persistent memory initialize (path: " + path +
            ", size: " + size + ") failed. Please check the path permission and initial size.");
        }
        initialized = true;
      }
    }
  }

  /**
   * Initialize the persistent memory with dax kmem node.
   */
  public static void initialize() {
    synchronized (PersistentMemoryPlatform.class) {
      if (!initialized) {
        initializeKmem();
        initialized = true;
      }
    }
  }

  private static native void initializeKmem();

  private static native void initializeNative(String path, long size, int pattern);

  /**
   * For DAX KMEM usage only
   * @param daxNodeId the numa node created from persistent memory.
   *                  memkind will set it as MEMKIND_DAX_KMEM_NODES env.
   *                  by using MEMKIND_DAX_KMEM_NODES, memkind will recognize this node
   * @param regularNodeId the numa node from dram
   */
  public static native void setNUMANode(String daxNodeId, String regularNodeId);

  /**
   * Allocate volatile memory from persistent memory.
   * @param size the requested size
   * @return the address which same as Platform.allocateMemory, it can be operated by
   * Platform which same as OFF_HEAP memory.
   */
  public static native long allocateVolatileMemory(long size);

  /**
   * Allocate direct buffer from persistent memory.
   * @param size the requested size
   * @return the byte buffer which same as Platform.allocateDirectBuffer, it can be operated by
   * Platform which same as OFF_HEAP memory.
   */
  public static ByteBuffer allocateVolatileDirectBuffer(int size) {
    try {
      Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
      Constructor<?> constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
      constructor.setAccessible(true);
      Field cleanerField = cls.getDeclaredField("cleaner");
      cleanerField.setAccessible(true);
      final long memory = allocateVolatileMemory(size);
      ByteBuffer buffer = (ByteBuffer) constructor.newInstance(memory, size);
      Cleaner cleaner = Cleaner.create(buffer, new Runnable() {
        @Override
        public void run() {
          freeMemory(memory);
        }
      });
      cleanerField.set(buffer, cleaner);
      return buffer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the actual occupied size of the given address. The occupied size should be different
   * with the requested size because of the memory management of Intel Optane DC persistent
   * memory is based on jemalloc.
   * @param address the memory block address.
   * @return actual occupied size.
   */
  public static native long getOccupiedSize(long address);

  /**
   * Free the memory by address.
   */
  public static native void freeMemory(long address);
}
