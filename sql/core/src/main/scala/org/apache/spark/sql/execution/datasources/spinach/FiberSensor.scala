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

package org.apache.spark.sql.execution.datasources.spinach

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.Logging
import org.apache.spark.scheduler.SparkListenerCustomInfoUpdate
import org.apache.spark.sql.execution.datasources.spinach.utils.CacheStatusSerDe
import org.apache.spark.util.collection.BitSet

// TODO FiberSensor doesn't consider the fiber cache, but only the number of cached
// fiber count
private[spinach] trait AbstractFiberSensor extends Logging {
  case class HostFiberCache(host: String, status: FiberCacheStatus)

  private val fileToHost = new ConcurrentHashMap[String, HostFiberCache]

  def update(fiberInfo: SparkListenerCustomInfoUpdate): Unit = {
    val execId = fiberInfo.executorId
    val fibersOnExecutor = CacheStatusSerDe.deserialize(fiberInfo.customizedInfo)
    fibersOnExecutor.foreach { case status =>
      fileToHost.get(status.file) match {
        case null => fileToHost.put(status.file, HostFiberCache(execId, status))
        case HostFiberCache(_, fcs) if (status.moreCacheThan(fcs)) =>
          // only cache a single executor ID, TODO need to considered the fiber id requried
          // replace the old HostFiberCache as the new one has more data cached
          fileToHost.put(status.file, HostFiberCache(execId, status))
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


private[spinach] case class FiberCacheStatus(file: String, bitmask: BitSet, meta: DataFileMeta) {
  val cachedFiberCount = bitmask.cardinality()

  def moreCacheThan(other: FiberCacheStatus): Boolean = {
    if (cachedFiberCount >= other.cachedFiberCount) {
      true
    } else {
      false
    }
  }
}

private[spinach] case class IndexFiberCacheStatus(file: String, meta: IndexMeta)
