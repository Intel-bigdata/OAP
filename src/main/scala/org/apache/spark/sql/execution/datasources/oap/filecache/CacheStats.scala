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

package org.apache.spark.sql.execution.datasources.oap.filecache

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.oap.OapConf

/**
 * Immutable class to present statistics of Cache. To record the change of cache stat in runtime,
 * please consider a counter class. [[CacheStats]] can be a snapshot of the counter class.
 *
 * implicit metrics:
 *    backEndCache(e.g. GuavaCache) = dataFiberCache + indexFiberCache
 *    allCache = backEndCache + pendingFiberCache
 */
case class CacheStats(
    dataFiberCount: Long,
    dataFiberSize: Long,
    indexFiberCount: Long,
    indexFiberSize: Long,
    pendingFiberCount: Long,
    pendingFiberSize: Long,
    hitCount: Long,
    missCount: Long,
    loadCount: Long,
    totalLoadTime: Long,
    evictionCount: Long) {

  require(dataFiberCount >= 0)
  require(dataFiberSize >= 0)
  require(indexFiberCount >= 0)
  require(indexFiberSize >= 0)
  require(pendingFiberCount >= 0)
  require(pendingFiberSize >= 0)
  require(hitCount >= 0)
  require(missCount >= 0)
  require(loadCount >= 0)
  require(totalLoadTime >= 0)
  require(evictionCount >= 0)

  def requestCount: Long = hitCount + missCount

  def hitRate: Double = {
    val rc = requestCount
    if (rc == 0) 1.0 else hitCount.toDouble / rc
  }

  def missRate: Double = {
    val rc = requestCount
    if (rc == 0) 0.0 else missCount.toDouble / rc
  }

  def averageLoadPenalty: Double = if (loadCount == 0) 0.0 else totalLoadTime.toDouble / loadCount

  def plus(other: CacheStats): CacheStats = this + other

  def minus(other: CacheStats): CacheStats = this - other

  def +(other: CacheStats): CacheStats =
    CacheStats(
      dataFiberCount + other.dataFiberCount,
      dataFiberSize + other.dataFiberSize,
      indexFiberCount + other.indexFiberCount,
      indexFiberSize + other.indexFiberSize,
      pendingFiberCount + other.pendingFiberCount,
      pendingFiberSize + other.pendingFiberSize,
      hitCount + other.hitCount,
      missCount + other.missCount,
      loadCount + other.loadCount,
      totalLoadTime + other.totalLoadTime,
      evictionCount + other.evictionCount)

  def -(other: CacheStats): CacheStats =
    CacheStats(
      math.max(0, dataFiberCount - other.dataFiberCount),
      math.max(0, dataFiberSize - other.dataFiberSize),
      math.max(0, indexFiberCount - other.indexFiberCount),
      math.max(0, indexFiberSize - other.indexFiberSize),
      math.max(0, pendingFiberCount - other.pendingFiberCount),
      math.max(0, pendingFiberSize - other.pendingFiberSize),
      math.max(0, hitCount - other.hitCount),
      math.max(0, missCount - other.missCount),
      math.max(0, loadCount - other.loadCount),
      math.max(0, totalLoadTime - other.totalLoadTime),
      math.max(0, evictionCount - other.evictionCount))

  def toDebugString: String = {
    s"CacheStats: { dataCacheCount/Size: $dataFiberCount/$dataFiberSize, " +
      s"indexCacheCount/Size: $indexFiberCount/$indexFiberSize, " +
      s"pendingCacheCount/Size: $pendingFiberCount/$pendingFiberSize, " +
      s"hitCount=$hitCount, missCount=$missCount, " +
      s"totalLoadTime=${totalLoadTime}ns, evictionCount=$evictionCount }"
  }

  def toJson: JValue = {
    ("dataFiberCount" -> dataFiberCount) ~
      ("dataFiberSize" -> dataFiberSize) ~
      ("indexFiberCount" -> indexFiberCount) ~
      ("indexFiberSize" -> indexFiberSize) ~
      ("pendingFiberCount" -> pendingFiberCount) ~
      ("pendingFiberSize" -> pendingFiberSize) ~
      ("hitCount" -> hitCount) ~
      ("missCount" -> missCount) ~
      ("loadCount" -> loadCount) ~
      ("totalLoadTime" -> totalLoadTime) ~
      ("evictionCount" -> evictionCount)
  }
}

object CacheStats extends Logging {
  private implicit val format = DefaultFormats
  private var updateInterval: Long = -1
  private var lastUpdateTime: Long = 0

  def apply(): CacheStats = CacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

  def apply(json: String): CacheStats = CacheStats(parse(json))

  def apply(json: JValue): CacheStats = CacheStats(
    (json \ "dataFiberCount").extract[Long],
      (json \ "dataFiberSize").extract[Long],
      (json \ "indexFiberCount").extract[Long],
      (json \ "indexFiberSize").extract[Long],
      (json \ "pendingFiberCount").extract[Long],
      (json \ "pendingFiberSize").extract[Long],
      (json \ "hitCount").extract[Long],
      (json \ "missCount").extract[Long],
      (json \ "loadCount").extract[Long],
      (json \ "totalLoadTime").extract[Long],
      (json \ "evictionCount").extract[Long])

  def status(cacheStats: CacheStats, conf: SparkConf): String = {
    updateInterval = if (updateInterval != -1) {
      updateInterval
    } else {
      conf.getLong(OapConf.OAP_UPDATE_FIBER_CACHE_METRICS_INTERVAL_SEC.key,
        OapConf.OAP_UPDATE_FIBER_CACHE_METRICS_INTERVAL_SEC.defaultValue.get) * 1000
    }
    if (System.currentTimeMillis() - lastUpdateTime > updateInterval) {
      lastUpdateTime = System.currentTimeMillis()
      compact(render(cacheStats.toJson))
    } else {
      ""
    }
  }

  // used for unit test
  def reset: Unit = {
    updateInterval = -1
    lastUpdateTime = 0
  }
}
