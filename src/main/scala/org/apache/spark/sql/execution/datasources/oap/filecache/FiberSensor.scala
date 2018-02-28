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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerCustomInfoUpdate
import org.apache.spark.sql.execution.datasources.oap.IndexMeta
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileHandle
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.util.collection.BitSet

private[oap] case class FiberCacheStatus(
    file: String,
    bitmask: BitSet,
    meta: OapDataFileHandle) {

  val cachedFiberCount = bitmask.cardinality()

  def moreCacheThan(other: FiberCacheStatus): Boolean = {
    if (cachedFiberCount >= other.cachedFiberCount) {
      true
    } else {
      false
    }
  }
}

private[oap] case class IndexFiberCacheStatus(file: String, meta: IndexMeta)


// TODO FiberSensor doesn't consider the fiber cache, but only the number of cached
// fiber count
private[oap] trait AbstractFiberSensor extends Logging {
  val OAP_CACHE_HOST_PREFIX = "OAP_HOST_"
  val OAP_CACHE_EXECUTOR_PREFIX = "_OAP_EXECUTOR_"
  case class HostFiberCache(host: String, status: FiberCacheStatus)

  private val fileToHost = new ConcurrentHashMap[String, HostFiberCache]

  def update(fiberInfo: SparkListenerCustomInfoUpdate): Unit = {
    val updateExecId = fiberInfo.executorId
    val updateHostName = fiberInfo.hostName
    val host = OAP_CACHE_HOST_PREFIX + updateHostName +
      OAP_CACHE_EXECUTOR_PREFIX + updateExecId
    val fibersOnExecutor = CacheStatusSerDe.deserialize(fiberInfo.customizedInfo)
    logDebug(s"Got updated fiber info from host: $updateHostName, executorId: $updateExecId," +
      s"host is $host, info array len is ${fibersOnExecutor.size}")
    fibersOnExecutor.foreach { status =>
      fileToHost.get(status.file) match {
        case null => fileToHost.put(status.file, HostFiberCache(host, status))
        case HostFiberCache(_, fcs) if status.moreCacheThan(fcs) =>
          // only cache a single executor ID, TODO need to considered the fiber id required
          // replace the old HostFiberCache as the new one has more data cached
          fileToHost.put(status.file, HostFiberCache(host, status))
        case _ =>
      }
    }
  }

  /**
   * get hosts that has fiber cached for fiber file.
   * Current implementation only returns one host, but still using API name with [[getHosts]]
   * @param filePath fiber file's path
   * @return
   */
  def getHosts(filePath: String): Option[String] = {
    fileToHost.get(filePath) match {
      case HostFiberCache(host, status) => Some(host)
      case null => None
    }
  }
}

object FiberSensor extends AbstractFiberSensor
