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

package org.apache.spark.sql.execution.datasources.spinach.utils

import org.apache.spark.sql.execution.datasources.spinach.DataFileMeta
import org.apache.spark.util.collection.BitSet

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.JsonAST._

object JsonSerDe {
  private implicit val format = DefaultFormats

  def bitSetToJson(bitSet: BitSet): JValue = {
    val words: Array[Long] = bitSet.toLongArray()
    val bitSetJson = JArray(words.map(word => ("word" -> word): JValue).toList)
    ("bitSet" -> bitSetJson)
  }

  def bitSetFromJson(json: JValue): BitSet = {
    val words: Array[Long] = (json \ "bitSet").extract[List[JValue]].map { word =>
      (word \ "word").extract[Long]
    }.toArray[Long]
    new BitSet(words)
  }

  // we only transfer 4 items in DataFileMeta to driver, ther are rowCountInEachGroup,
  // rowCountInLastGroup, groupCount, fieldCount respectively
  def dataFileMetaToJson(dataFileMeta: DataFileMeta): JValue = {
    ("rowCountInEachGroup" -> dataFileMeta.rowCountInEachGroup)~
    ("rowCountInLastGroup" -> dataFileMeta.rowCountInLastGroup)~
    ("groupCount" -> dataFileMeta.groupCount)~
    ("fieldCount" -> dataFileMeta.fieldCount)
  }

  def dataFileMetaFromJson(json: JValue): DataFileMeta = {
    val rowCountInEachGroup = (json \ "rowCountInEachGroup").extract[Int]
    val rowCountInLastGroup = (json \ "rowCountInLastGroup").extract[Int]
    val groupCount = (json \ "groupCount").extract[Int]
    val fieldCount = (json \ "fieldCount").extract[Int]
    new DataFileMeta(
      rowCountInEachGroup = rowCountInEachGroup,
      rowCountInLastGroup = rowCountInLastGroup,
      groupCount = groupCount,
      fieldCount = fieldCount)
  }

  def statusRawDataArrayToJson(
      statusRawDataArray: Array[(String, BitSet, DataFileMeta)]): JValue = {
    val statusJArray = JArray(statusRawDataArray.map(statusRawDataToJson).toList)
    ("statusRawDataArray" -> statusJArray)
  }

  def statusRawDataToJson(statusRawData: (String, BitSet, DataFileMeta)): JValue = {
    val fiberFilePath = statusRawData._1
    val bitSetJValue = bitSetToJson(statusRawData._2)
    val dataFileMetaJValue = dataFileMetaToJson(statusRawData._3)
    ("fiberFilePath" -> fiberFilePath)~
    ("bitSetJValue" -> bitSetJValue)~
    ("dataFileMetaJValue" -> dataFileMetaJValue)
  }

  def statusRawDataArrayFromJson(json: JValue): Array[(String, BitSet, DataFileMeta)] = {
    val statusRawDataArray: Array[(String, BitSet, DataFileMeta)] =
      (json \ "statusRawDataArray").extract[List[JValue]].map(statusRawDataFromJson).toArray
    statusRawDataArray
  }

  def statusRawDataFromJson(json: JValue): (String, BitSet, DataFileMeta) = {
    val path = (json \ "fiberFilePath").extract[String]
    val bitSet = bitSetFromJson(json \ "bitSetJValue")
    val dataFileMeta = dataFileMetaFromJson(json \ "dataFileMetaJValue")
    (path, bitSet, dataFileMeta)
  }
}
