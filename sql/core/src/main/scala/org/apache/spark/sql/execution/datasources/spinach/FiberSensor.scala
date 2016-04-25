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

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import org.apache.spark.Logging
import org.apache.spark.scheduler.SparkListenerCustomInfoUpdate
import org.apache.spark.sql.execution.datasources.spinach.utils.CacheStatusSerDe
import org.apache.spark.util.collection.BitSet

import org.json4s.jackson.JsonMethods._

object FiberSensor extends Logging {
  // maps that maintain the relations of "executor id, fiber file path, fiber cached bitSet of
  // the fiber files, and fibers file meta", 4 items in total
  val fileToExecBitSetMap = new ConcurrentHashMap[String, Map[String, BitSet]]().asScala
  // TODO this map currently not used, will be used in future implementation
  val fileToDataFileMetaMap = new ConcurrentHashMap[String, DataFileMeta]().asScala

  def update(fiberInfo: SparkListenerCustomInfoUpdate): Unit = {
    val execId = fiberInfo.executorId
    val status = CacheStatusSerDe.statusRawDataArrayFromJson(parse(fiberInfo.customizedInfo))
    status.foreach { case (fiberFilePath, fiberCacheBitSet, dataFileMeta) =>
      fileToExecBitSetMap.getOrElseUpdate(
        fiberFilePath, new HashMap[String, BitSet]())(execId) = fiberCacheBitSet
      fileToDataFileMetaMap(fiberFilePath) = dataFileMeta
    }
  }

  def getHosts(filePath: String): Array[String] = {
    val hosts = new ArrayBuffer[String]()
    fileToExecBitSetMap.get(filePath).map { execToBitSet =>
      execToBitSet.toArray.sortWith {
        (left, right) => left._2.cardinality() > right._2.cardinality()
      }.foreach { case (executor, bitSet) =>
        if (bitSet.nextSetBit(0) != -1) hosts += executor
      }
    }
    hosts.toArray
  }
}
