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

package org.apache.spark.sql.execution.datasources.oap

import scala.collection.mutable

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.sql.execution.datasources.oap.OapMessages.CacheDrop
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheManager
import org.apache.spark.util.Utils

private[spark] object OapMessageUtils {
  def sendMessageToExecutors(
      scheduler: CoarseGrainedSchedulerBackend, message: OapMessage): Unit = {
      // TODO: why we can't just use executorDataMap?
      val executorDataMapField =
        classOf[CoarseGrainedSchedulerBackend].getDeclaredField(
          "org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap")
      executorDataMapField.setAccessible(true)
      val executorDataMap =
        executorDataMapField.get(scheduler).asInstanceOf[mutable.HashMap[String, AnyRef]]
      for ((_, executorData) <- executorDataMap) {
        val c = Utils.classForName("org.apache.spark.scheduler.cluster.ExecutorData")
        val executorEndpointField = c.getDeclaredField("executorEndpoint")
        executorEndpointField.setAccessible(true)
        val executorEndpoint =
          executorEndpointField.get(executorData).asInstanceOf[RpcEndpointRef]
        executorEndpoint.send(message)
      }
  }

  def handleOapMessage(message: OapMessage): Unit = message match {
    case CacheDrop(indexName) =>
      FiberCacheManager.removeIndexCache(indexName)
  }
}
