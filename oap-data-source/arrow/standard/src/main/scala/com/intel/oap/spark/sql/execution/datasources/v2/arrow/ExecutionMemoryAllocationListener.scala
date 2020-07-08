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
package com.intel.oap.spark.sql.execution.datasources.v2.arrow

import org.apache.arrow.memory.{AllocationListener, OutOfMemoryException}

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}

class ExecutionMemoryAllocationListener(mm: TaskMemoryManager)
  extends MemoryConsumer(mm, mm.pageSizeBytes(), MemoryMode.OFF_HEAP) with AllocationListener {


  override def onPreAllocation(size: Long): Unit = {
    if (size == 0) {
      return
    }
    val granted = acquireMemory(size)
    if (granted < size) {
      throw new OutOfMemoryException("Failed allocating spark execution memory. Acquired: " +
        size + ", granted: " + granted)
    }
  }

  override def onRelease(size: Long): Unit = {
    freeMemory(size)
  }

  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    // not spillable
    0L
  }
}
