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


import org.apache.arrow.memory.ReservationListener;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;

import java.io.IOException;

/**
 * Native memory pool's Java mapped instance.
 */
public class ExpressionMemoryPool implements AutoCloseable {
  private final long nativeInstanceId;

  static {
    try {
      JniUtils.getInstance();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public ExpressionMemoryPool(long nativeInstanceId) {
    this.nativeInstanceId = nativeInstanceId;
  }

  public static ExpressionMemoryPool getDefault() {
    return new ExpressionMemoryPool(getDefaultMemoryPool());
  }

  public static ExpressionMemoryPool createListenable(ReservationListener listener) {
    return new ExpressionMemoryPool(createListenableMemoryPool(listener));
  }

  public static ExpressionMemoryPool forSpark() {
    if (TaskContext.get() == null) {
      return getDefault();
    } else {
      ExpressionMemoryPool pool = createListenable(SparkMemoryUtils.reservationListener());
      SparkMemoryUtils.addLeakSafeTaskCompletionListener(context -> {
        try {
          pool.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException();
        }
      });
      return pool;
    }
  }

  public long getNativeInstanceId() {
    return nativeInstanceId;
  }

  @Override
  public void close() throws Exception {
    releaseMemoryPool(nativeInstanceId);
  }

  private static native long getDefaultMemoryPool();

  private static native long createListenableMemoryPool(ReservationListener listener);

  private static native void releaseMemoryPool(long id);
}
