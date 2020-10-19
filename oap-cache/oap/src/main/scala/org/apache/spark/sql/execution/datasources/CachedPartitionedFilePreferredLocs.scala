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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

object CachedPartitionedFilePreferredLocs extends Logging{

  private var externalDBClient: ExternalDBClient = null

  def getPreferredLocsByCache(files: Array[PartitionedFile]): Seq[String] = {
    if (null == externalDBClient) {
      externalDBClient = ExternalDBClientFactory.getDBClientInstance(SparkEnv.get)
    }

    var preferredLocs = new ArrayBuffer[String]

    val hostToCachedBytes = mutable.HashMap.empty[String, Long]

    files.foreach { file =>
      val cacheMetaInfoValueArr = externalDBClient.get(file.filePath, file.start, file.length)
      if (cacheMetaInfoValueArr.size > 0) {
        // host<-> cachedBytes
        cacheMetaInfoValueArr.foreach(x => {
          hostToCachedBytes.put(x.host, hostToCachedBytes.getOrElse(x.host, 0L) + x.length)
        })
      }
    }

    // TODO if cachedBytes <<< hdfsPreferLocBytes
    hostToCachedBytes.toSeq.sortWith(_._2 > _._2).take(3).foreach(x => preferredLocs.+=(x._1))
    preferredLocs.take(3)
  }

}
