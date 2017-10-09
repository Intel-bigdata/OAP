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

import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.oap.io.OapIndexInfoStatus

class OapIndexInfoStatusSerDeSuite extends SparkFunSuite {
  private def assertStringEquals(json1: String, json2: String) {
    val formatJsonString = (json: String) => json.replaceAll("[\\s|]", "")
    assert(formatJsonString(json1) === formatJsonString(json2),
      s"input ${formatJsonString(json1)} got ${formatJsonString(json2)}")
  }

  test("test oap index info status raw data") {
    val path = "partitionFile"
    val useIndex = true
    val indexInfoStatusRawData = OapIndexInfoStatus(path, useIndex)
    val newIndexInfoStatusRawData =
      OapIndexInfoStatusSerDe.indexStatusRawDataFromJson(
        OapIndexInfoStatusSerDe.indexStatusRawDataToJson(indexInfoStatusRawData))
    assertStatusRawDataEquals(indexInfoStatusRawData, newIndexInfoStatusRawData)
  }

  test("test oap index infor status ser and deser") {
    val path1 = "partitionFile1"
    val useIndex1 = true
    val path2 = "partitionFile2"
    val useIndex2 = false
    val rawData1 = OapIndexInfoStatus(path1, useIndex1)
    val rawData2 = OapIndexInfoStatus(path2, useIndex2)
    val indexInfoStatusSeq = Seq(rawData1, rawData2)
    val indexInfoStatusSerializeStr = OapIndexInfoStatusSerDe.serialize(indexInfoStatusSeq)
    assertStringEquals(indexInfoStatusSerializeStr,
      OapIndexInfoStatusSerDeTestStrs.indexInfoStatusRawDataSeqString)
    val indexInfoDeserializeSeq = OapIndexInfoStatusSerDe.deserialize(indexInfoStatusSerializeStr)
    assert(indexInfoStatusSeq.length === indexInfoDeserializeSeq.length)
    var i = 0
    while (i < indexInfoDeserializeSeq.length) {
      assertStatusRawDataEquals(indexInfoDeserializeSeq(i), indexInfoDeserializeSeq(i))
      i += 1
    }
  }

  private def assertStatusRawDataEquals(data1: OapIndexInfoStatus, data2: OapIndexInfoStatus) = {
    assert(data1.path === data2.path)
    assert(data1.useIndex === data2.useIndex)
  }
}

private[oap] object OapIndexInfoStatusSerDeTestStrs {
  val indexInfoStatusRawDataSeqString =
    s"""
       |{
       |  "oapIndexInfoStatusRawDataArray" : [
       |    {
       |      "partitionFilePath" : "partitionFile1",
       |      "useOapIndex" : true
       |    },
       |    {
       |      "partitionFilePath" : "partitionFile2",
       |      "useOapIndex" : false
       |    }
       |  ]
       |}
     """
}
