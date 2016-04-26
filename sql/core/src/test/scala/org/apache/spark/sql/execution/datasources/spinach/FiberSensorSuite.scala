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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, SparkFunSuite}
import org.apache.spark.scheduler.SparkListenerCustomInfoUpdate
import org.apache.spark.sql.execution.datasources.spinach.utils.CacheStatusSerDe
import org.apache.spark.util.collection.BitSet

import org.json4s.jackson.JsonMethods._

class FiberSensorSuite extends SparkFunSuite with Logging {

  test("test get hosts from FiberSensor") {
    // clear the map first
    FiberSensor.fileToExecBitSetMap.clear()

    val filePath = "file1"
    val dataFileMeta = new DataFileMeta(new ArrayBuffer[RowGroupMeta](), 10, 2, 30, 3)

    // executor1 update
    val execId1 = "executor1"
    val bitSet1 = new BitSet(90)
    bitSet1.set(1)
    bitSet1.set(2)
    val statusRawDataArray1 = new ArrayBuffer[(String, BitSet, DataFileMeta)]
    statusRawDataArray1 += ((filePath, bitSet1, dataFileMeta))
    val fiberInfo1 = SparkListenerCustomInfoUpdate(execId1, compact(render(CacheStatusSerDe
      .statusRawDataArrayToJson(statusRawDataArray1.toArray))))
    FiberSensor.update(fiberInfo1)
    assert(FiberSensor.fileToExecBitSetMap.size === 1)
    assert(FiberSensor.fileToExecBitSetMap.contains(filePath))
    assert(FiberSensor.fileToExecBitSetMap.get(filePath).size === 1)

    // executor2 update
    val execId2 = "executor2"
    val bitSet2 = new BitSet(90)
    bitSet2.set(3)
    bitSet2.set(4)
    bitSet2.set(5)
    bitSet2.set(6)
    bitSet2.set(7)
    bitSet2.set(8)
    val statusRawDataArray2 = new ArrayBuffer[(String, BitSet, DataFileMeta)]
    statusRawDataArray2 += ((filePath, bitSet2, dataFileMeta))
    val fiberInfo2 = SparkListenerCustomInfoUpdate(execId2, compact(render(CacheStatusSerDe
      .statusRawDataArrayToJson(statusRawDataArray2.toArray))))
    FiberSensor.update(fiberInfo2)
    assert(FiberSensor.fileToExecBitSetMap.size === 1)
    FiberSensor.fileToExecBitSetMap.get(filePath).foreach(m => assert(m.contains(execId2)))
    assert(FiberSensor.fileToExecBitSetMap.get(filePath).get.size === 2)

    // executor3 update
    val execId3 = "executor3"
    val bitSet3 = new BitSet(90)
    bitSet3.set(7)
    bitSet3.set(8)
    bitSet3.set(9)
    bitSet3.set(10)
    val statusRawDataArray3 = new ArrayBuffer[(String, BitSet, DataFileMeta)]
    statusRawDataArray3 += ((filePath, bitSet3, dataFileMeta))
    val fiberInfo3 = SparkListenerCustomInfoUpdate(execId3, compact(render(CacheStatusSerDe
      .statusRawDataArrayToJson(statusRawDataArray3.toArray))))
    FiberSensor.update(fiberInfo3)
    assert(FiberSensor.fileToExecBitSetMap.size === 1)
    assert(FiberSensor.fileToExecBitSetMap.get(filePath).get.size === 3)

    val hosts = FiberSensor.getHosts(filePath)
    // executor2 have more fibers cached, then executor3, and then executor1
    assert(hosts === Array(execId2, execId3, execId1))
  }
}