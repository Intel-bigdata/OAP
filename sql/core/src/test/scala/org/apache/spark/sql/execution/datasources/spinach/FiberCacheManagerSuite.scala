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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet


class FiberCacheManagerSuite extends SparkFunSuite with Logging {

  test("test getting right status") {
    object testFiberManager extends AbstractFiberCacheManger {
      override def fiber2Data(key: Fiber): FiberCacheData = MemoryManager.allocate(100)
    }
    val conf: Configuration = new Configuration()
    val rowGroupsMeta = new ArrayBuffer[RowGroupMeta](30)
    val fieldCount = 3
    val groupCount = 30
    val rowCountInEachGroup = 10
    val rowCountInLastGroup = 3

    val filePath = "file1.data"
    val dataMeta = new DataFileMeta(
      rowGroupsMeta, rowCountInEachGroup, rowCountInLastGroup, groupCount, fieldCount)

    // DataFileScanner will read file back and get dataMeta and cache it
    val fileScanner = new DataFileScanner(filePath, new StructType(), conf) {
      override lazy val meta: DataFileMeta = dataMeta
    }

    val columnIndex = 1
    val rowGroupId = 1
    testFiberManager(Fiber(fileScanner, columnIndex, rowGroupId))

    val fiberBitSet = new BitSet(groupCount * fieldCount)
    fiberBitSet.set(columnIndex + fieldCount * rowGroupId)
  }

}
