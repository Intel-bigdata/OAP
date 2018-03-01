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

package org.apache.spark.sql.execution.datasources.oap.utils

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheStatus
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileHandle
import org.apache.spark.util.collection.BitSet

/**
 * This is user defined Json protocol for SerDe, here the format of Json output should like
 * following:
 *   {"statusRawDataArray" :
 *     ["fiberFilePath" : ""
 *      "bitSetJValue" :
 *        {"bitSet" :
 *          ["word" : Long,
 *           "word" : Long,
 *           "word" : Long, ...]}
 *      "dataFileMetaJValue" : {
 *        "rowCountInEachGroup" : Int
 *        "rowCountInLastGroup" : Int
 *        "groupCount" : Int
 *        "fieldCount" : Int
 *      }]
 *     []...[]}
 */
private[oap] object CacheStatusSerDe extends SerDe[String, Seq[FiberCacheStatus]] {
  import org.json4s.jackson.JsonMethods._

  override def serialize(statusRawDataArray: Seq[FiberCacheStatus]): String = {
    val statusJArray = JArray(statusRawDataArray.map(statusRawDataToJson).toList)
    compact(render("statusRawDataArray" -> statusJArray))
  }

  private implicit val format = DefaultFormats

  override def deserialize(json: String): Seq[FiberCacheStatus] = {
    (parse(json) \ "statusRawDataArray").extract[List[JValue]].map(statusRawDataFromJson)
  }

  private[oap] def bitSetToJson(bitSet: BitSet): JValue = {
    val words: Array[Long] = bitSet.toLongArray()
    val bitSetJson = JArray(words.map(word => ("word" -> word): JValue).toList)
    ("bitSet" -> bitSetJson)
  }

  private[oap] def bitSetFromJson(json: JValue): BitSet = {
    val words: Array[Long] = (json \ "bitSet").extract[List[JValue]].map { word =>
      (word \ "word").extract[Long]
    }.toArray[Long]
    new BitSet(words)
  }

  // we only transfer 4 items in DataFileMeta to driver, ther are rowCountInEachGroup,
  // rowCountInLastGroup, groupCount, fieldCount respectively
  private[oap] def dataFileMetaToJson(dataFileMeta: OapDataFileHandle): JValue = {
    ("rowCountInEachGroup" -> dataFileMeta.rowCountInEachGroup) ~
      ("rowCountInLastGroup" -> dataFileMeta.rowCountInLastGroup) ~
      ("groupCount" -> dataFileMeta.groupCount) ~
      ("fieldCount" -> dataFileMeta.fieldCount)
  }

  private[oap] def dataFileMetaFromJson(json: JValue): OapDataFileHandle = {
    val rowCountInEachGroup = (json \ "rowCountInEachGroup").extract[Int]
    val rowCountInLastGroup = (json \ "rowCountInLastGroup").extract[Int]
    val groupCount = (json \ "groupCount").extract[Int]
    val fieldCount = (json \ "fieldCount").extract[Int]
    new OapDataFileHandle(
      rowCountInEachGroup = rowCountInEachGroup,
      rowCountInLastGroup = rowCountInLastGroup,
      groupCount = groupCount,
      fieldCount = fieldCount)
  }

  private[oap] def statusRawDataToJson(statusRawData: FiberCacheStatus): JValue = {
    ("fiberFilePath" -> statusRawData.file) ~
      ("bitSetJValue" -> bitSetToJson(statusRawData.bitmask)) ~
      ("dataFileMetaJValue" -> dataFileMetaToJson(statusRawData.meta))
  }

  private[oap] def statusRawDataFromJson(json: JValue): FiberCacheStatus = {
    val path = (json \ "fiberFilePath").extract[String]
    val bitSet = bitSetFromJson(json \ "bitSetJValue")
    val dataFileMeta = dataFileMetaFromJson(json \ "dataFileMetaJValue")
    FiberCacheStatus(path, bitSet, dataFileMeta)
  }
}
