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

package org.apache.spark.sql.execution.datasources

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis

import org.apache.spark.internal.Logging

abstract class CacheMetaInfo(key: String, value: CacheMetaInfoValue)
  extends Logging {
}

case class CacheMetaInfoValue(host: String, offSet: Long, length: Long) {
  override def toString: String = {
    "host: " + host + ", offset: " + offSet + ", length: " + length + "."
  }
}

case class StoreCacheMetaInfo(key: String, value: CacheMetaInfoValue)
  extends CacheMetaInfo(key, value) {

  def doUpsert(jedisClientInstance: Jedis): Unit = {
    val cacheMetaInfoJson = ("offSet" -> value.offSet) ~
      ("length" -> value.length) ~
      ("host" -> value.host)
    // TODO remove this log, it's user confidential
    logDebug("upsert key: " + key +
      "cacheMetaInfo is: " + value.toString)
    jedisClientInstance
      .zadd(key, value.offSet, compact(render(cacheMetaInfoJson)))
  }
}

case class EvictCacheMetaInfo(key: String, value: CacheMetaInfoValue)
  extends CacheMetaInfo(key, value) {

  def doUpsert(jedisClientInstance: Jedis): Unit = {
    val cacheMetaInfoJson = ("offSet" -> value.offSet) ~
      ("length" -> value.length) ~
      ("host" -> value.host)
    // TODO remove this log, it's user confidential
    logDebug("evict key: " + key +
      "cacheMetaInfo is: " + value.toString)
    jedisClientInstance
      .zrem(key, compact(render(cacheMetaInfoJson)))
  }
}
