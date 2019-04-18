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

import org.json4s.{DefaultFormats, StringInput}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import org.apache.spark.sql.execution.datasources.oap.io.OapIndexInfoStatus

/**
 * This is user defined Json protocol for SerDe, here the format of Json outputs are like
 * following:
 *   {"oapIndexInfoStatusRawDataArray" :
 *     [{"partitionFilePath" : ""
 *      "useOapIndex" : "true/false"}
 *     {} ... {}]}
 */
private[oap] object OapIndexInfoStatusSerDe extends SerDe[String, Seq[OapIndexInfoStatus]] {
  import org.json4s.jackson.JsonMethods._

  private implicit val format = DefaultFormats

  override def serialize(indexStatusRawDataArray: Seq[OapIndexInfoStatus]): String = {
    val statusJArray = JArray(indexStatusRawDataArray.map(indexStatusRawDataToJson).toList)
    compact(render("oapIndexInfoStatusRawDataArray" -> statusJArray))
  }

  override def deserialize(json: String): Seq[OapIndexInfoStatus] = {
    (parse(StringInput(json), false) \ "oapIndexInfoStatusRawDataArray").extract[List[JValue]].map(
      indexStatusRawDataFromJson(_))
  }

  private[oap] def indexStatusRawDataFromJson(json: JValue): OapIndexInfoStatus = {
    val path = (json \ "partitionFilePath").extract[String]
    val useIndex = (json \ "useOapIndex").extract[Boolean]
    OapIndexInfoStatus(path, useIndex)
  }

  private[oap] def indexStatusRawDataToJson(statusRawData: OapIndexInfoStatus): JValue = {
    ("partitionFilePath" -> statusRawData.path)~
      ("useOapIndex" -> statusRawData.useIndex)
  }
}
