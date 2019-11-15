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

import org.json4s.{DefaultFormats, StringInput}
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
    dataFiberHitCount: Long,
    dataFiberMissCount: Long,
    dataFiberLoadCount: Long,
    dataTotalLoadTime: Long,
    dataEvictionCount: Long,
    indexFiberHitCount: Long,
    indexFiberMissCount: Long,
    indexFiberLoadCount: Long,
    indexTotalLoadTime: Long,
    indexEvictionCount: Long) {

  require(dataFiberCount >= 0)
  require(dataFiberSize >= 0)
  require(indexFiberCount >= 0)
  require(indexFiberSize >= 0)
  require(pendingFiberCount >= 0)
  require(pendingFiberSize >= 0)
  require(dataFiberHitCount >= 0)
  require(dataFiberMissCount >= 0)
  require(dataFiberLoadCount >= 0)
  require(dataTotalLoadTime >= 0)
  require(dataEvictionCount >= 0)
  require(indexFiberHitCount >= 0)
  require(indexFiberMissCount >= 0)
  require(indexFiberLoadCount >= 0)
  require(indexTotalLoadTime >= 0)
  require(indexEvictionCount >= 0)

  def requestCount: Long =
    dataFiberHitCount
  + dataFiberMissCount
  + indexFiberHitCount
  + indexFiberMissCount

  def hitRate: Double = {
    val rc = requestCount
    if (rc == 0) 1.0 else (dataFiberHitCount + indexFiberHitCount).toDouble / rc
  }

  def missRate: Double = {
    val rc = requestCount
    if (rc == 0) 0.0 else (dataFiberMissCount + indexFiberMissCount).toDouble / rc
  }

  def backendCacheSize: Long = indexFiberSize + dataFiberSize

  def totalCacheSize: Long = backendCacheSize + pendingFiberSize

  def backendCacheCount: Long = indexFiberCount + dataFiberCount

  def totalCacheCount: Long = backendCacheCount + pendingFiberCount

  def averageLoadPenalty: Double = {
    if ((indexFiberLoadCount + dataFiberLoadCount) == 0) {
      0.0
    }
    else {
      (indexTotalLoadTime + dataTotalLoadTime).toDouble / (indexFiberLoadCount + dataFiberLoadCount)
    }
  }

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
      dataFiberHitCount + other.dataFiberHitCount,
      dataFiberMissCount + other.dataFiberMissCount,
      dataFiberLoadCount + other.dataFiberLoadCount,
      dataTotalLoadTime + other.dataTotalLoadTime,
      dataEvictionCount + other.dataEvictionCount,
      indexFiberHitCount + other.indexFiberHitCount,
      indexFiberMissCount + other.indexFiberMissCount,
      indexFiberLoadCount + other.indexFiberLoadCount,
      indexTotalLoadTime + other.indexTotalLoadTime,
      indexEvictionCount + other.indexEvictionCount)

  def -(other: CacheStats): CacheStats =
    CacheStats(
      math.max(0, dataFiberCount - other.dataFiberCount),
      math.max(0, dataFiberSize - other.dataFiberSize),
      math.max(0, indexFiberCount - other.indexFiberCount),
      math.max(0, indexFiberSize - other.indexFiberSize),
      math.max(0, pendingFiberCount - other.pendingFiberCount),
      math.max(0, pendingFiberSize - other.pendingFiberSize),
      math.max(0, dataFiberHitCount - other.dataFiberHitCount),
      math.max(0, dataFiberMissCount - other.dataFiberMissCount),
      math.max(0, dataFiberLoadCount - other.dataFiberLoadCount),
      math.max(0, dataTotalLoadTime - other.dataTotalLoadTime),
      math.max(0, dataEvictionCount - other.dataEvictionCount),
      math.max(0, indexFiberHitCount - other.indexFiberHitCount),
      math.max(0, indexFiberMissCount - other.indexFiberMissCount),
      math.max(0, indexFiberLoadCount - other.indexFiberLoadCount),
      math.max(0, indexTotalLoadTime - other.indexTotalLoadTime),
      math.max(0, indexEvictionCount - other.indexEvictionCount))

  def toDebugString: String = {
    s"CacheStats: { dataCacheCount/Size: $dataFiberCount/$dataFiberSize, " +
      s"indexCacheCount/Size: $indexFiberCount/$indexFiberSize, " +
      s"pendingCacheCount/Size: $pendingFiberCount/$pendingFiberSize, " +
      s"dataFiberHitCount=$dataFiberHitCount, dataFiberMissCount=$dataFiberMissCount, " +
      s"dataFiberLoadCount=${dataFiberLoadCount}ns, dataEvictionCount=$dataEvictionCount, " +
      s"indexFiberHitCount=$indexFiberHitCount, indexFiberMissCount=$indexFiberMissCount, " +
      s"indexTotalLoadTime=${indexTotalLoadTime}ns, indexEvictionCount=$indexEvictionCount }"
  }

  def toJson: JValue = {
    ("dataFiberCount" -> dataFiberCount) ~
      ("dataFiberSize" -> dataFiberSize) ~
      ("indexFiberCount" -> indexFiberCount) ~
      ("indexFiberSize" -> indexFiberSize) ~
      ("pendingFiberCount" -> pendingFiberCount) ~
      ("pendingFiberSize" -> pendingFiberSize) ~
      ("dataFiberHitCount" -> dataFiberHitCount) ~
      ("dataFiberMissCount" -> dataFiberMissCount) ~
      ("dataFiberLoadCount" -> dataFiberLoadCount) ~
      ("dataTotalLoadTime" -> dataTotalLoadTime) ~
      ("dataEvictionCount" -> dataEvictionCount) ~
      ("indexFiberHitCount" -> indexFiberHitCount) ~
      ("indexFiberMissCount" -> indexFiberMissCount) ~
      ("indexFiberLoadCount" -> indexFiberLoadCount) ~
      ("indexTotalLoadTime" -> indexTotalLoadTime) ~
      ("indexEvictionCount" -> indexEvictionCount)
  }
}

object CacheStats extends Logging {
  private implicit val format = DefaultFormats
  private var updateInterval: Long = -1
  private var lastUpdateTime: Long = 0

  def apply(): CacheStats = CacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

  def apply(json: String): CacheStats = CacheStats(parse(StringInput(json), false))

  def apply(json: JValue): CacheStats = CacheStats(
    (json \ "dataFiberCount").extract[Long],
      (json \ "dataFiberSize").extract[Long],
      (json \ "indexFiberCount").extract[Long],
      (json \ "indexFiberSize").extract[Long],
      (json \ "pendingFiberCount").extract[Long],
      (json \ "pendingFiberSize").extract[Long],
      (json \ "dataFiberHitCount").extract[Long],
      (json \ "dataFiberMissCount").extract[Long],
      (json \ "dataFiberLoadCount").extract[Long],
      (json \ "dataTotalLoadTime").extract[Long],
      (json \ "dataEvictionCount").extract[Long],
      (json \ "indexFiberHitCount").extract[Long],
      (json \ "indexFiberMissCount").extract[Long],
      (json \ "indexFiberLoadCount").extract[Long],
      (json \ "indexTotalLoadTime").extract[Long],
      (json \ "indexEvictionCount").extract[Long])

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
