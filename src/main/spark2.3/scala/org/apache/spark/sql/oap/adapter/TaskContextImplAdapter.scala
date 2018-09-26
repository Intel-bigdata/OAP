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

package org.apache.spark.sql.oap.adapter

import java.util.Properties

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem


object TaskContextImplAdapter {
  /**
    * The Construction of TaskContextImpl has changed in the spark2.3 version.
    * Ignore it in the spark2.1, spark2.2 version
    */
  def createTaskContextImpl(
      stageId: Int,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int,
      taskMemoryManager: TaskMemoryManager,
      localProperties: Properties,
      metricsSystem: MetricsSystem): TaskContext = {
    new TaskContextImpl(
      stageId,
      stageAttemptNumber = 0,
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      localProperties,
      metricsSystem)
  }
}
