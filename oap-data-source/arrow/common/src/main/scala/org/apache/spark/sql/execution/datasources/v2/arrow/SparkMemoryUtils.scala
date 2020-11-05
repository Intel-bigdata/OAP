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

package org.apache.spark.sql.execution.datasources.v2.arrow

import java.util.UUID

import com.intel.oap.spark.sql.execution.datasources.v2.arrow.SparkManagedReservationListener
import org.apache.arrow.memory.{AllocationListener, BaseAllocator, BufferAllocator, DirectReservationListener, OutOfMemoryException, ReservationListener}

import org.apache.spark.TaskContext
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.util.TaskCompletionListener

object SparkMemoryUtils {
  private val taskToAllocatorMap = new java.util.IdentityHashMap[TaskContext, BufferAllocator]()
  private val taskToReservationListenerMap =
    new java.util.IdentityHashMap[TaskContext, SparkManagedReservationListener]()

  private class ExecutionMemoryAllocationListener(mm: TaskMemoryManager)
    extends MemoryConsumer(mm, mm.pageSizeBytes(), MemoryMode.OFF_HEAP) with AllocationListener {

    override def onPreAllocation(size: Long): Unit = {
      if (size == 0) {
        return
      }
      val granted = acquireMemory(size)
      if (granted < size) {
        throw new OutOfMemoryException("Not enough spark off-heap execution memory. " +
          "Acquired: " + size + ", granted: " + granted + ". " +
          "Try tweaking config option spark.memory.offHeap.size to " +
          "get larger space to run this application. ")
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

  private def getLocalTaskContext: TaskContext = TaskContext.get()

  private def getTaskMemoryManager(): TaskMemoryManager = {
    getLocalTaskContext.taskMemoryManager()
  }

  private def inSparkTask(): Boolean = {
    getLocalTaskContext != null
  }

  def addLeakSafeTaskCompletionListener[U](f: TaskContext => U): TaskContext = {
    arrowAllocator()
    getLocalTaskContext.addTaskCompletionListener(f)
  }

  def arrowAllocator(): BaseAllocator = {
    if (!inSparkTask()) {
      return org.apache.spark.sql.util.ArrowUtils.rootAllocator
    }
    val tc = getLocalTaskContext
    val allocator = taskToAllocatorMap.synchronized {
      if (taskToAllocatorMap.containsKey(tc)) {
        taskToAllocatorMap.get(tc).asInstanceOf[BaseAllocator]
      } else {
        val al = new ExecutionMemoryAllocationListener(getTaskMemoryManager())
        val parent = org.apache.spark.sql.util.ArrowUtils.rootAllocator
        val newInstance = parent.newChildAllocator("Spark Managed Allocator - " +
          UUID.randomUUID().toString, al, 0, parent.getLimit).asInstanceOf[BaseAllocator]
        taskToAllocatorMap.put(tc, newInstance)
        getLocalTaskContext.addTaskCompletionListener(
          new TaskCompletionListener {
            override def onTaskCompletion(context: TaskContext): Unit = {
              taskToAllocatorMap.synchronized {
                if (taskToAllocatorMap.containsKey(context)) {
                  val allocator = taskToAllocatorMap.get(context)
                  val allocated = allocator.getAllocatedMemory
                  if (allocated == 0L) {
                    close(allocator)
                    taskToAllocatorMap.remove(context)
                  } else {
                    softClose(allocator)
                  }
                }
              }
            }
          })
        newInstance
      }
    }
    allocator
  }

  private def close(allocator: BufferAllocator): Unit = {
    allocator.getChildAllocators.forEach(close(_))
    allocator.close()
  }

  /**
   * Close the allocator quietly without having any OOM errors thrown. We rely on Spark's memory
   * management system to detect possible memory leaks after the task get successfully down. Any
   * leak shown right here is possibly not actual because buffers may be cleaned up after
   * this check code is executed. Having said that developers should manage to make sure
   * the specific clean up logic of operators is registered at last of the program which means
   * it will be executed earlier.
   *
   * @see org.apache.spark.executor.Executor.TaskRunner#run()
   */
  private def softClose(allocator: BufferAllocator): Unit = {
    // do nothing
  }

  def reservationListener(): ReservationListener = {
    if (!inSparkTask()) {
      return DirectReservationListener.instance()
    }
    val tc = getLocalTaskContext
    val listener = taskToReservationListenerMap.synchronized {
      if (taskToReservationListenerMap.containsKey(tc)) {
        taskToReservationListenerMap.get(tc)
      } else {
        val rl = new SparkManagedReservationListener(getTaskMemoryManager())
        taskToReservationListenerMap.put(tc, rl)
        getLocalTaskContext.addTaskCompletionListener(
          new TaskCompletionListener {
            override def onTaskCompletion(context: TaskContext): Unit = {
              taskToReservationListenerMap.synchronized {
                if (taskToReservationListenerMap.containsKey(context)) {
                  val rl = taskToReservationListenerMap.get(context)
                  val allocated = rl.getUsed
                  if (allocated == 0L) {
                    taskToReservationListenerMap.remove(context)
                  } else {
                    // do nothing
                  }
                }
              }
            }
          })
        rl
      }
    }
    listener
  }
}
