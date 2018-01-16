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

import com.google.common.cache.AbstractCache.StatsCounter

/**
 * Statistics about the performance of [[OapCache]]. Instances of this class are immutable.
 * Need another class like [[StatsCounter]] to record stats and return current [[CacheStats]].
 * Note: But for now, we don't need to record the stats since Guava has done it for us.
 *
 * @param hitCount incremented when a cache lookup encounters an existing cache entry
 * @param missCount incremented when a cache lookup first encounters a missing cache entry
 * @param loadCount incremented when a new entry is loaded
 * @param totalLoadTime incremented when a new entry is loaded
 * @param evictionCount incremented when an entry is evicted from the cache
 */
case class CacheStats(
    hitCount: Long, missCount: Long, loadCount: Long, totalLoadTime: Long, evictionCount: Long) {

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

  def averageLoadPenalty: Double = if (loadCount == 0) 0.0 else totalLoadTime / loadCount

  def plus(other: CacheStats): CacheStats = this + other

  def minus(other: CacheStats): CacheStats = this - other

  def +(other: CacheStats): CacheStats =
    CacheStats(
      hitCount + other.hitCount,
      missCount + other.missCount,
      loadCount + other.loadCount,
      totalLoadTime + other.totalLoadTime,
      evictionCount + other.evictionCount)

  def -(other: CacheStats): CacheStats =
    CacheStats(
      math.max(0, hitCount - other.hitCount),
      math.max(0, missCount - other.missCount),
      math.max(0, loadCount - other.loadCount),
      math.max(0, totalLoadTime - other.totalLoadTime),
      math.max(0, evictionCount - other.evictionCount))
}

object CacheStats {
  def apply(guavaCacheStats: com.google.common.cache.CacheStats): CacheStats =
    CacheStats(
      guavaCacheStats.hitCount(),
      guavaCacheStats.missCount(),
      guavaCacheStats.loadCount(),
      guavaCacheStats.totalLoadTime(),
      guavaCacheStats.evictionCount())
}
