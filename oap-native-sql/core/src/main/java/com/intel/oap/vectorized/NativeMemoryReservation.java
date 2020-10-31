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

import org.apache.arrow.dataset.jni.JniLoader;

/**
 * Abstract class for buffer memory reservation. Used by native datasets.
 */
public abstract class NativeMemoryReservation {

  static {
    JniLoader.get().ensureLoaded();
  }

  /**
   * Set a specific instance of NativeMemoryReservation. Note the instance will be globally accessible in
   * the current process. All native based datasets will be using this instance as a call back for
   * memory reservation.
   */
  public static synchronized native void setGlobal(NativeMemoryReservation reservation);

  /**
   * Reserve bytes.
   *
   * @throws RuntimeException if request size cannot be granted
   */
  public abstract void reserve(long size);

  /**
   * Unreserve bytes.
   */
  public abstract void unreserve(long size);
}
