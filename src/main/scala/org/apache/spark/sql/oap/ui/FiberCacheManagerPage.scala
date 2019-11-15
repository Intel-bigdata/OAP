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
import org.apache.spark.sql.execution.datasources.oap.filecache.CacheStats
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class FiberCacheManagerPage(parent: OapTab) extends WebUIPage("") with Logging {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      <div>
        {
        <div id="active-cms"></div> ++
          <script src={UIUtils.prependBaseUri(request, parent.basePath,
            "/static/utils.js")}></script> ++
          <script src={UIUtils.prependBaseUri(request, parent.basePath,
            "/static/oap/oap.js")}></script>
        }
      </div>

    UIUtils.headerSparkPage(request, "FiberCacheManager", content, parent, useDataTables = true)
  }

}


class FiberCacheManagerSummary private[spark](
    val id: String,
    val hostPort: String,
    val isActive: Boolean,
    val indexDataCacheSeparationEnable: Boolean,
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
    val dataFiberHitCount: Long,
    val dataFiberMissCount: Long,
    val dataFiberLoadCount: Long,
    val dataTotalLoadTime: Long,
    val dataEvictionCount: Long,
    val indexFiberHitCount: Long,
    val indexFiberMissCount: Long,
    val indexFiberLoadCount: Long,
    val indexTotalLoadTime: Long,
    val indexEvictionCount: Long)
