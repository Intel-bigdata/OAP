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

import java.io.File

import org.apache.spark.sql.execution.datasources.spinach.utils.CacheStatusSerDe

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobID, TaskID, TaskAttemptID, TaskAttemptContext}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.util.StringUtils
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet
import org.apache.spark.{Logging, SparkFunSuite}

import org.scalatest.BeforeAndAfterAll

import org.json4s.jackson.JsonMethods._

class FiberCacheManagerSuite extends SparkFunSuite with Logging with BeforeAndAfterAll {
  private var file: File = null
  val attemptContext: TaskAttemptContext = new TaskAttemptContextImpl(
    new Configuration(),
    new TaskAttemptID(new TaskID(new JobID(), true, 0), 0))
  val ctx: Configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(attemptContext)

  override def beforeAll(): Unit = {
    file = Utils.createTempDir()
    file.delete()
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(file)
  }

  test("test getting right status") {
    FiberCacheManager.cache.cleanUp()
    DataMetaCacheManager.cache.cleanUp()
    FiberDataFileHandler.cache.cleanUp()
    val path = new Path(StringUtils.unEscapeString(file.toURI.toString))
    val out = FileSystem.get(ctx).create(path, true)
    val rowGroupsMeta = new Array[RowGroupMeta](30)
    val fieldCount = 3
    val groupCount = 30
    val rowCountInEachGroup = 10
    val rowCountInLastGroup = 3
    var i = 0
    while (i < rowGroupsMeta.length) {
      rowGroupsMeta(i) =
        new RowGroupMeta()
          .withNewStart(i * 4 * fieldCount)
          .withNewEnd(i * 4 * fieldCount + 4 * fieldCount)
          .withNewFiberLens(Array(1, 2, 3))
      i += 1
    }
    val dataMeta = new DataFileMeta(rowGroupsMeta.toBuffer.asInstanceOf[ArrayBuffer[RowGroupMeta]],
      rowCountInEachGroup, rowCountInLastGroup, groupCount, fieldCount)
    dataMeta.write(out)
    out.close()
    // DataFileScanner will read file back and get dataMeta and cache it
    val fileScanner = DataFileScanner(path.toUri.toString, new StructType(), attemptContext)
    assert(DataMetaCacheManager.cache.asMap.containsKey(fileScanner))
    // meta data that read back from files should be the same with the original
    val dataMetaReadBack = DataMetaCacheManager.cache.asMap.get(fileScanner)
    assert(dataMetaReadBack.rowCountInEachGroup === rowCountInEachGroup)
    assert(dataMetaReadBack.rowCountInLastGroup === rowCountInLastGroup)
    assert(dataMetaReadBack.groupCount === groupCount)
    assert(dataMetaReadBack.fieldCount === fieldCount)
    // FiberDataFileHandler will cache fileSacnner after DataMeta is cached
    assert(FiberDataFileHandler.cache.asMap.containsKey(fileScanner))

    val columnIndex = 1
    val rowGroupId = 1
    val fiber = Fiber(fileScanner, columnIndex, rowGroupId)
    FiberCacheManager(fiber)
    assert(FiberCacheManager.cache.asMap().containsKey(fiber))

    val fiberBitSet = new BitSet(groupCount * fieldCount)
    fiberBitSet.set(columnIndex + fieldCount * rowGroupId)
    val statusRawDataArr = Array((path.toUri.toString, fiberBitSet, dataMeta))
    assert(FiberCacheManager.status === compact(
      render(CacheStatusSerDe.statusRawDataArrayToJson(statusRawDataArr))))
  }

}
