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

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;
import scala.Function1;

import java.io.IOException;
import java.util.IdentityHashMap;

/**
 * Reserve Spark managed memory.
 */
public class NativeSparkManagedMemoryReservation extends NativeMemoryReservation {

  public static final NativeSparkManagedMemoryReservation INSTANCE =
      new NativeSparkManagedMemoryReservation();

  private final IdentityHashMap<TaskContext, ConsumerImpl> consumers = new IdentityHashMap<>();

  private NativeSparkManagedMemoryReservation() {

  }

  @Override
  public void reserve(long size) {
    if (size == 0) {
      return;
    }
    final TaskContext tc = TaskContext.get();
    if (tc == null) {
      return;
    }
    final MemoryConsumer consumer = getOrCreateConsumer(tc);
    long granted = consumer.acquireMemory(size);
    if (granted < size) {
      throw new OutOfMemoryException("Not enough spark off-heap execution memory. " +
          "Acquired: " + size + ", granted: " + granted + ". " +
          "Try tweaking config option spark.memory.offHeap.size to " +
          "get larger space to run this application. ");
    }
  }

  @Override
  public void unreserve(long size) {
    if (size == 0) {
      return;
    }
    final TaskContext tc = TaskContext.get();
    if (tc == null) {
      return;
    }
    final MemoryConsumer consumer = getOrCreateConsumer(tc);
    consumer.freeMemory(size);
  }

  private ConsumerImpl getOrCreateConsumer(TaskContext tc) {
    final ConsumerImpl consumer;
    synchronized (consumers) {
      if (!consumers.containsKey(tc)) {
        consumer = new ConsumerImpl(tc.taskMemoryManager());
        consumers.put(tc, consumer);
        SparkMemoryUtils.addLeakSafeTaskCompletionListener(new Function1<TaskContext, Object>() {
          @Override
          public Object apply(TaskContext tc) {
            synchronized (consumers) {
              consumers.remove(tc);
            }
            return null;
          }
        });
      } else {
        consumer = consumers.get(tc);
      }
    }
    return consumer;
  }

  private static class ConsumerImpl extends MemoryConsumer {

    protected ConsumerImpl(TaskMemoryManager taskMemoryManager) {
      super(taskMemoryManager);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      return 0L; // not spillable
    }
  }
}
