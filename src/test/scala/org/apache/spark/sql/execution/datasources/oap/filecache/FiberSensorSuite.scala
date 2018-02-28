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

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerCustomInfoUpdate
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileHandle
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.util.collection.BitSet

class FiberSensorSuite extends SparkFunSuite with AbstractFiberSensor with Logging {

  test("test get hosts from FiberSensor") {
    val filePath = "file1"
    val dataFileMeta = new OapDataFileHandle(
      rowCountInEachGroup = 10, rowCountInLastGroup = 2, groupCount = 30, fieldCount = 3)

    // executor1 update
    val host1 = "host1"
    val execId1 = "executor1"
    val bitSet1 = new BitSet(90)
    bitSet1.set(1)
    bitSet1.set(2)
    val fcs = Seq(FiberCacheStatus(filePath, bitSet1, dataFileMeta))
    val fiberInfo = SparkListenerCustomInfoUpdate(host1, execId1, CacheStatusSerDe.serialize(fcs))
    this.update(fiberInfo)
    assert(this.getHosts(filePath) contains (FiberSensor.OAP_CACHE_HOST_PREFIX + host1 +
      FiberSensor.OAP_CACHE_EXECUTOR_PREFIX + execId1))

    // executor2 update
    val host2 = "host2"
    val execId2 = "executor2"
    val bitSet2 = new BitSet(90)
    bitSet2.set(3)
    bitSet2.set(4)
    bitSet2.set(5)
    bitSet2.set(6)
    bitSet2.set(7)
    bitSet2.set(8)

    val fiberInfo2 = SparkListenerCustomInfoUpdate(host2, execId2, CacheStatusSerDe
      .serialize(Seq(FiberCacheStatus(filePath, bitSet2, dataFileMeta))))
    this.update(fiberInfo2)
    assert(this.getHosts(filePath) contains  (FiberSensor.OAP_CACHE_HOST_PREFIX + host2 +
      FiberSensor.OAP_CACHE_EXECUTOR_PREFIX + execId2))

    // executor3 update
    val host3 = "host3"
    val execId3 = "executor3"
    val bitSet3 = new BitSet(90)
    bitSet3.set(7)
    bitSet3.set(8)
    bitSet3.set(9)
    bitSet3.set(10)
    val fiberInfo3 = SparkListenerCustomInfoUpdate(host3, execId3, CacheStatusSerDe
      .serialize(Seq(FiberCacheStatus(filePath, bitSet3, dataFileMeta))))
    this.update(fiberInfo3)
    assert(this.getHosts(filePath) === Some(FiberSensor.OAP_CACHE_HOST_PREFIX + host2 +
      FiberSensor.OAP_CACHE_EXECUTOR_PREFIX + execId2))
  }
}
