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

package org.apache.spark.sql.oap.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.oap.filecache.{CacheStats, FiberCacheManagerSensor}
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.ui.exec.ExecutorsListener

private[ui] class FiberCacheManagerPage(parent: OapTab) extends WebUIPage("") with Logging {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      <div>
        {
        <div id="active-cms"></div> ++
          <script src={UIUtils.prependBaseUri("/static/utils.js")}></script> ++
          <script src={UIUtils.prependBaseUri("/static/oap/oap.js")}></script>
        }
      </div>

    UIUtils.headerSparkPage("FiberCacheManager", content, parent, useDataTables = true)
  }

}

private[spark] object FiberCacheManagerPage {
  /** Represent an executor's info as a map given a storage status index */
  def getFiberCacheManagerInfo(
      listener: ExecutorsListener,
      statusId: Int): FiberCacheManagerSummary = {
    val status = listener.activeStorageStatusList(statusId)
    val execId = status.blockManagerId.executorId
    val hostPort = status.blockManagerId.hostPort
    val memUsed = status.memUsed
    val maxMem = status.maxMem
    val cacheStats = if (FiberCacheManagerSensor.executorToCacheManager.containsKey(execId)) {
      FiberCacheManagerSensor.executorToCacheManager.get(execId)
    } else {
      CacheStats()
    }

    new FiberCacheManagerSummary(
      execId,
      hostPort,
      true,
      memUsed,
      maxMem,
      cacheStats.totalCacheSize,
      cacheStats.totalCacheCount,
      cacheStats.backendCacheSize,
      cacheStats.backendCacheCount,
      cacheStats.dataFiberSize,
      cacheStats.dataFiberCount,
      cacheStats.indexFiberSize,
      cacheStats.indexFiberCount,
      cacheStats.pendingFiberSize,
      cacheStats.pendingFiberCount,
      cacheStats.hitCount,
      cacheStats.missCount,
      cacheStats.loadCount,
      cacheStats.totalLoadTime,
      cacheStats.evictionCount
    )
  }
}


class FiberCacheManagerSummary private[spark](
    val id: String,
    val hostPort: String,
    val isActive: Boolean,
    val memoryUsed: Long,
    val maxMemory: Long,
    val cacheSize: Long,
    val cacheCount: Long,
    val backendCacheSize: Long,
    val backendCacheCount: Long,
    val dataFiberSize: Long,
    val dataFiberCount: Long,
    val indexFiberSize: Long,
    val indexFiberCount: Long,
    val pendingFiberSize: Long,
    val pendingFiberCount: Long,
    val hitCount: Long,
    val missCount: Long,
    val loadCount: Long,
    val loadTime: Long,
    val evictionCount: Long)
