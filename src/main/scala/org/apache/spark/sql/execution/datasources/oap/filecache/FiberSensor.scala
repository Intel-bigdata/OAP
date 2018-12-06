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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Throwables

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberSensor.HostFiberCache
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.oap.listener.SparkListenerCustomInfoUpdate
import org.apache.spark.util.collection.BitSet

private[oap] case class FiberCacheStatus(
    file: String,
    bitmask: BitSet,
    groupCount: Int,
    fieldCount: Int) {

  val cachedFiberCount = bitmask.cardinality()

}

// FiberSensor is the FiberCache info recorder on Driver, it contains a file cache location mapping
// (for cache locality) and metrics info
private[sql] class FiberSensor extends Logging {

  private[filecache] val fileToHosts = new ConcurrentHashMap[String, ArrayBuffer[HostFiberCache]]

  private[filecache] def updateRecordingMap(fromHost: String, commingStatus: FiberCacheStatus) =
    synchronized {
    val currentHostsForFile = if (fileToHosts.containsKey(commingStatus.file)) {
      fileToHosts.get(commingStatus.file)
    } else {
      new ArrayBuffer[HostFiberCache](0)
    }
    val (_, theRest) = currentHostsForFile.partition(_.host == fromHost)
    val newHostsForFile = (HostFiberCache(fromHost, commingStatus) +: theRest)
      .sorted.takeRight(FiberSensor.MAX_HOSTS_MAINTAINED)
    fileToHosts.put(commingStatus.file, newHostsForFile)
  }

  private[filecache] def discardOutdatedInfo(host: String) = synchronized {
    for ((k: String, v: ArrayBuffer[HostFiberCache]) <- fileToHosts.asScala) {
      val(_, kept) = v.partition(_.host == host)
      if (kept.size == 0) {
        fileToHosts.remove(k)
      } else {
        fileToHosts.put(k, kept)
      }
    }
  }

  def updateLocations(fiberInfo: SparkListenerCustomInfoUpdate): Unit = {
    val updateExecId = fiberInfo.executorId
    val updateHostName = fiberInfo.hostName
    val host = FiberSensor.OAP_CACHE_HOST_PREFIX + updateHostName +
      FiberSensor.OAP_CACHE_EXECUTOR_PREFIX + updateExecId
    val fiberCacheStatus = CacheStatusSerDe.deserialize(fiberInfo.customizedInfo)
    logDebug(s"Got updated fiber info from host: $updateHostName, executorId: $updateExecId," +
      s"host is $host, info array len is ${fiberCacheStatus.size}")
    // Coming information of a certain executor requires discarding previous records so as to
    // reflect Fiber eviction
    discardOutdatedInfo(host)
    fiberCacheStatus.foreach(updateRecordingMap(host, _))
  }

  // TODO: define a function to wrap this and make it private
  private[sql] val executorToCacheManager = new ConcurrentHashMap[String, CacheStats]()

  def updateMetrics(fiberInfo: SparkListenerCustomInfoUpdate): Unit = {
    if (fiberInfo.customizedInfo.nonEmpty) {
      try {
        val cacheMetrics = CacheStats(fiberInfo.customizedInfo)
        executorToCacheManager.put(fiberInfo.executorId, cacheMetrics)
        logDebug(s"execID:${fiberInfo.executorId}, host:${fiberInfo.hostName}," +
          s" ${cacheMetrics.toDebugString}")
      } catch {
        case t: Throwable =>
          val stack = Throwables.getStackTraceAsString(t)
          logError(s"FiberSensor parse json failed, $stack")
      }
    }
  }

  def getExecutorToCacheManager(): ConcurrentHashMap[String, CacheStats] = {
    executorToCacheManager
  }

  /**
   * get hosts that has fiber cached for fiber file.
   * Current implementation only returns one host, but still using API name with [[getHosts]]
   * @param filePath fiber file's path
   * @return
   */
  def getHosts(filePath: String): Seq[String] = {
    val hosts = if (fileToHosts.containsKey(filePath)) {
      fileToHosts.get(filePath)
    } else {
      new ArrayBuffer[HostFiberCache](0)
    }
    hosts.sorted.takeRight(FiberSensor.NUM_GET_HOSTS).reverse.map(_.host)
  }
}

private[sql] object FiberSensor {

  case class HostFiberCache(host: String, status: FiberCacheStatus)
      extends Ordered[HostFiberCache] {
    override def compare(another: HostFiberCache): Int = {
      status.cachedFiberCount - another.status.cachedFiberCount
    }
  }

  private def conf = OapRuntime.getOrCreate.sparkSession.conf

  val NUM_GET_HOSTS = conf.get(
    OapConf.OAP_CACHE_FIBERSENSOR_GETHOSTS_NUM,
    OapConf.OAP_CACHE_FIBERSENSOR_GETHOSTS_NUM.defaultValue.get)
  val MAX_HOSTS_MAINTAINED = conf.get(
    OapConf.OAP_CACHE_FIBERSENSOR_MAXHOSTSMAINTAINED_NUM,
    OapConf.OAP_CACHE_FIBERSENSOR_MAXHOSTSMAINTAINED_NUM.defaultValue.get)
  val OAP_CACHE_HOST_PREFIX = "OAP_HOST_"
  val OAP_CACHE_EXECUTOR_PREFIX = "_OAP_EXECUTOR_"
}
