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

package com.intel.oap.vectorized;

import org.apache.arrow.memory.NativeMemoryReservation;
import org.apache.arrow.util.VisibleForTesting;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reserving Java direct memory bytes from java.nio.Bits. Used by Java Dataset API's C++ memory
 * pool implementation. This makes memory allocated by the pool to be controlled by JVM option
 * "-XX:MaxDirectMemorySize".
 */
public class NativeDirectMemoryReservation extends NativeMemoryReservation {
  private final Method methodReserve;
  private final Method methodUnreserve;

  private NativeDirectMemoryReservation() {
    try {
      final Class<?> classBits = Class.forName("java.nio.Bits");
      methodReserve = classBits.getDeclaredMethod("reserveMemory", long.class, int.class);
      methodReserve.setAccessible(true);
      methodUnreserve = classBits.getDeclaredMethod("unreserveMemory", long.class, int.class);
      methodUnreserve.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final NativeDirectMemoryReservation INSTANCE = new NativeDirectMemoryReservation();

  public static NativeDirectMemoryReservation instance() {
    return INSTANCE;
  }

  /**
   * Reserve bytes by invoking java.nio.java.Bitjava.nio.Bitss#reserveMemory.
   */
  @Override
  public void reserve(long size) {
    try {
      if (size > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("reserve size should be of type int");
      }
      methodReserve.invoke(null, (int) size, (int) size);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Unreserve bytes by invoking java.nio.java.Bitjava.nio.Bitss#unreserveMemory.
   */
  @Override
  public void unreserve(long size) {
    try {
      if (size > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("unreserve size should be of type int");
      }
      methodUnreserve.invoke(null, (int) size, (int) size);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get current reservation of jVM direct memory. Visible for testing.
   */
  @VisibleForTesting
  public long getCurrentDirectMemReservation() {
    try {
      final Class<?> classBits = Class.forName("java.nio.Bits");
      final Field f = classBits.getDeclaredField("reservedMemory");
      f.setAccessible(true);
      return ((AtomicLong) f.get(null)).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
