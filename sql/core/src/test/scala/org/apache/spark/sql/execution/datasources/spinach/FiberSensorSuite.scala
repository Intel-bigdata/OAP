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
import org.apache.spark.sql.execution.datasources.spinach.utils.{CacheStatusSerDe, FiberCacheStatus}
import org.apache.spark.util.collection.BitSet
import org.json4s.jackson.JsonMethods._

class FiberSensorSuite extends SparkFunSuite with AbstractFiberSensor with Logging {

  test("test get hosts from FiberSensor") {
    val filePath = "file1"
    val dataFileMeta = new DataFileMeta(new ArrayBuffer[RowGroupMeta](), 10, 2, 30, 3)

    // executor1 update
    val execId1 = "executor1"
    val bitSet1 = new BitSet(90)
    bitSet1.set(1)
    bitSet1.set(2)
    val fcs = Seq(FiberCacheStatus(filePath, bitSet1, dataFileMeta))
    val fiberInfo = SparkListenerCustomInfoUpdate(execId1, CacheStatusSerDe.serialize(fcs))
    this.update(fiberInfo)
    assert(this.getHosts(filePath) == Option(execId1))

    // executor2 update
    val execId2 = "executor2"
    val bitSet2 = new BitSet(90)
    bitSet2.set(3)
    bitSet2.set(4)
    bitSet2.set(5)
    bitSet2.set(6)
    bitSet2.set(7)
    bitSet2.set(8)

    val fiberInfo2 = SparkListenerCustomInfoUpdate(execId2, CacheStatusSerDe
      .serialize(Seq(FiberCacheStatus(filePath, bitSet2, dataFileMeta))))
    this.update(fiberInfo2)
    assert(this.getHosts(filePath) == Some(execId2))

    // executor3 update
    val execId3 = "executor3"
    val bitSet3 = new BitSet(90)
    bitSet3.set(7)
    bitSet3.set(8)
    bitSet3.set(9)
    bitSet3.set(10)
    val fiberInfo3 = SparkListenerCustomInfoUpdate(execId3, CacheStatusSerDe
      .serialize(Seq(FiberCacheStatus(filePath, bitSet3, dataFileMeta))))
    this.update(fiberInfo3)
    assert(this.getHosts(filePath) === Some(execId2))
  }
}
