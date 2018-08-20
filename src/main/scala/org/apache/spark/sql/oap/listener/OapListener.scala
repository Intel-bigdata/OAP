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

package org.apache.spark.sql.oap.listener

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.datasources.oap.io.OapIndexInfo
import org.apache.spark.sql.oap.OapRuntime

@DeveloperApi
case class SparkListenerCustomInfoUpdate(
    hostName: String,
    executorId: String,
    clazzName: String,
    customizedInfo: String) extends SparkListenerEvent {
  override def logEvent: Boolean = false
}

@DeveloperApi
case class SparkListenerOapIndexInfoUpdate(
    hostName: String,
    executorId: String,
    oapIndexInfo: String) extends SparkListenerEvent {
  override def logEvent: Boolean = false
}

class OapListener extends SparkListener {
  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case customInfo: SparkListenerCustomInfoUpdate =>
      if (customInfo.clazzName.contains("OapFiberCacheHeartBeatMessager")) {
        OapRuntime.getOrCreate.fiberSensor.updateLocations(customInfo)
      } else if (customInfo.clazzName.contains("FiberCacheManagerMessager")) {
        OapRuntime.getOrCreate.fiberSensor.updateMetrics(customInfo)
      }
    case indexInfo: SparkListenerOapIndexInfoUpdate =>
      OapIndexInfo.update(indexInfo)
    case _ =>
  }
}
