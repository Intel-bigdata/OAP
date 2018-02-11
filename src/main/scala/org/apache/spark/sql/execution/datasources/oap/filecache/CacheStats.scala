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

case class CacheStatsInternal(size: Long, count: Long) {
  require(size >= 0)
  require(count >= 0)

  def plus(other: CacheStatsInternal): CacheStatsInternal = this + other

  def minus(other: CacheStatsInternal): CacheStatsInternal = this - other

  def +(other: CacheStatsInternal): CacheStatsInternal =
    CacheStatsInternal(size + other.size, count + other.count)

  def -(other: CacheStatsInternal): CacheStatsInternal =
    CacheStatsInternal(
      math.max(0, size - other.size),
      math.max(0, count - other.count))

  override def toString: String = s"{size:$size, count:$count}"

  def toJson: JValue = ("size" -> size) ~ ("count" -> count)
}
object CacheStatsInternal {
  private implicit val format = DefaultFormats

  def apply(): CacheStatsInternal = CacheStatsInternal(0, 0)

  def apply(json: String): CacheStatsInternal = CacheStatsInternal(parse(json))

  def apply(json: JValue): CacheStatsInternal = CacheStatsInternal(
    (json \ "size").extract[Long],
    (json \ "count").extract[Long]
  )
}

/**
 * Immutable class to present statistics of Cache. To record the change of cache stat in runtime,
 * please consider a counter class. [[CacheStats]] can be a snapshot of the counter class.
 */
case class CacheStats(
    cache: CacheStatsInternal,
    backEndCache: CacheStatsInternal,
    dataFiberCache: CacheStatsInternal,
    indexFiberCache: CacheStatsInternal,
    pendingFiberCache: CacheStatsInternal,
    hitCount: Long,
    missCount: Long,
    loadCount: Long,
    totalLoadTime: Long,
    evictionCount: Long) {

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
      cache + other.cache,
      backEndCache + other.backEndCache,
      dataFiberCache + other.dataFiberCache,
      indexFiberCache + other.indexFiberCache,
      pendingFiberCache + other.pendingFiberCache,
      hitCount + other.hitCount,
      missCount + other.missCount,
      loadCount + other.loadCount,
      totalLoadTime + other.totalLoadTime,
      evictionCount + other.evictionCount)

  def -(other: CacheStats): CacheStats =
    CacheStats(
      cache - other.cache,
      backEndCache - other.backEndCache,
      dataFiberCache - other.dataFiberCache,
      indexFiberCache - other.indexFiberCache,
      pendingFiberCache - other.pendingFiberCache,
      math.max(0, hitCount - other.hitCount),
      math.max(0, missCount - other.missCount),
      math.max(0, loadCount - other.loadCount),
      math.max(0, totalLoadTime - other.totalLoadTime),
      math.max(0, evictionCount - other.evictionCount))

  def toDebugString: String = {
    s"CacheStats: { cache: $cache, backEndCache:$backEndCache, dataFiber:$dataFiberCache, " +
      s"indexFiber:$indexFiberCache, pendingFiber:$pendingFiberCache hitCount=$hitCount, " +
      s"missCount=$missCount, totalLoadTime=${totalLoadTime} ns, evictionCount=$evictionCount }"
  }

  def toJson: JValue = {
    ("cache" -> cache.toJson)~
      ("backEndCache" -> backEndCache.toJson)~
      ("dataFiber" -> dataFiberCache.toJson)~
      ("indexFiber" -> indexFiberCache.toJson)~
      ("pendingFiber" -> pendingFiberCache.toJson)~
      ("hitCount" -> hitCount)~
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

  def apply(): CacheStats = CacheStats(
    CacheStatsInternal(), CacheStatsInternal(), CacheStatsInternal(),
    CacheStatsInternal(), CacheStatsInternal(), 0, 0, 0, 0, 0)

  def apply(json: String): CacheStats = CacheStats(parse(json))

  def apply(json: JValue): CacheStats = CacheStats(
    CacheStatsInternal(json \ "cache"),
    CacheStatsInternal(json \ "backEndCache"),
    CacheStatsInternal(json \ "dataFiber"),
    CacheStatsInternal(json \ "indexFiber"),
    CacheStatsInternal(json \ "pendingFiber"),
      (json \ "hitCount").extract[Long],
      (json \ "missCount").extract[Long],
      (json \ "loadCount").extract[Long],
      (json \ "totalLoadTime").extract[Long],
      (json \ "evictionCount").extract[Long])

  def status(cacheStats: CacheStats, conf: SparkConf): String = {
    updateInterval = if (updateInterval != -1) {
      updateInterval
    } else {
      conf.getLong("oap.update.fiber.cache.metrics.interval.sec", 60L) * 1000
    }
    if (System.currentTimeMillis() - lastUpdateTime > updateInterval) {
      lastUpdateTime = System.currentTimeMillis()
      compact(render(cacheStats.toJson))
    } else {
      ""
    }
  }
}
