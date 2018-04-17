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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListenerCustomInfoUpdate
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileHandle
import org.apache.spark.sql.execution.datasources.oap.listener.FiberInfoListener
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

class FiberSensorSuite extends QueryTest with SharedOapContext
  with AbstractFiberSensor with BeforeAndAfterEach {

  import testImplicits._

  private var currentPath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.addSparkListener(new FiberInfoListener)
  }

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
    currentPath = path

    sql(s"""CREATE TEMPORARY VIEW oap_test (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path')""".stripMargin)
    FiberCacheManager.clearAllFibers()
    FiberCacheManagerSensor.executorToCacheManager.clear()
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
  }

  test("test FiberCacheManagerSensor with sql") {
    // make each dataFile has 2 rowGroup.
    // 3000 columns in total, 2 data file by default, 1500 columns each file.
    // So, each file will have 2 rowGroup.
    sqlConf.setConfString(OapConf.OAP_ROW_GROUP_SIZE.key, "1000")

    // Insert data, build index and query, expected hit-index, range ensure all
    // row groups are cached.
    val data: Seq[(Int, String)] = (1 to 3000).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    checkAnswer(sql("SELECT * FROM oap_test"), Seq.empty[Row])
    sql("insert overwrite table oap_test select * from t")
    withIndex(TestIndex("oap_test", "index1")) {
      sql("create oindex index1 on oap_test (a) using btree")
      checkAnswer(sql("SELECT * FROM oap_test WHERE a > 500 AND a < 2500"),
        data.filter(r => r._1 > 500 && r._1 < 2500).map(r => Row(r._1, r._2)))
      CacheStats.reset

      // Data/Index file statistic
      val files = FileSystem.get(new Configuration()).listStatus(new Path(currentPath))
      var indexFileCount = 0L
      var dataFileCount = 0L
      for (file <- files) {
        if (file.getPath.getName.endsWith(".index")) {
          indexFileCount += 1L
        } else if (file.getPath.getName.endsWith(".data")) {
          dataFileCount += 1L
        }
      }

      // Only one executor in local-mode, each data file has 4 dataFiber(2 cols * 2 rgs/col)
      // wait for a heartbeat
      Thread.sleep(20 * 1000)
      val summary = FiberCacheManagerSensor.summary()
      logWarning(s"Summary1: ${summary.toDebugString}")
      assertResult(1)(FiberCacheManagerSensor.executorToCacheManager.size())
      assertResult(dataFileCount * 4)(summary.dataFiberCount)

      // all data are cached when run another sql.
      // Expect: 1.hitCount increase; 2.missCount equal
      // wait for a heartbeat period
      checkAnswer(sql("SELECT * FROM oap_test WHERE a > 200 AND a < 2400"),
        data.filter(r => r._1 > 200 && r._1 < 2400).map(r => Row(r._1, r._2)))
      CacheStats.reset
      Thread.sleep(15 * 1000)
      val summary2 = FiberCacheManagerSensor.summary()
      logWarning(s"Summary2: ${summary2.toDebugString}")
      assertResult(1)(FiberCacheManagerSensor.executorToCacheManager.size())
      assert(summary.hitCount < summary2.hitCount)
      assertResult(summary.missCount)(summary2.missCount)
      assertResult(summary.dataFiberCount)(summary2.dataFiberCount)
      assertResult(summary.dataFiberSize)(summary2.dataFiberSize)
      assertResult(summary.indexFiberCount)(summary2.indexFiberCount)
      assertResult(summary.indexFiberSize)(summary2.indexFiberSize)
    }
  }

  test("test FiberCacheManagerSensor onCustomInfoUpdate FiberCacheManagerMessager") {
    val host = "0.0.0.0"
    val execID = "exec1"
    val messager = "FiberCacheManagerMessager"
    // Test json empty, no more executor added
    new FiberInfoListener onCustomInfoUpdate SparkListenerCustomInfoUpdate(
      host, execID, messager, "")
    assertResult(0)(FiberCacheManagerSensor.executorToCacheManager.size())

    // Test json error, no more executor added
    new FiberInfoListener onCustomInfoUpdate SparkListenerCustomInfoUpdate(
      host, execID, messager, "error msg")
    assertResult(0)(FiberCacheManagerSensor.executorToCacheManager.size())

    // Test normal msg
    CacheStats.reset
    val conf: SparkConf = new SparkConf()
    conf.set(OapConf.OAP_UPDATE_FIBER_CACHE_METRICS_INTERVAL_SEC.key, 0L.toString)
    val cacheStats = CacheStats(2, 19, 10, 2, 0, 0, 213, 23, 23, 123131, 2)
    new FiberInfoListener onCustomInfoUpdate SparkListenerCustomInfoUpdate(
      host, execID, messager, CacheStats.status(cacheStats, conf))
    assertResult(1)(FiberCacheManagerSensor.executorToCacheManager.size())
    assertResult(cacheStats.toJson)(
      FiberCacheManagerSensor.executorToCacheManager.get(execID).toJson)
  }

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
    val fiberInfo = SparkListenerCustomInfoUpdate(host1, execId1,
      "OapFiberCacheHeartBeatMessager", CacheStatusSerDe.serialize(fcs))
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

    val fiberInfo2 = SparkListenerCustomInfoUpdate(host2, execId2,
      "OapFiberCacheHeartBeatMessager", CacheStatusSerDe
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
    val fiberInfo3 = SparkListenerCustomInfoUpdate(host3, execId3,
      "OapFiberCacheHeartBeatMessager", CacheStatusSerDe
        .serialize(Seq(FiberCacheStatus(filePath, bitSet3, dataFileMeta))))
    this.update(fiberInfo3)
    assert(this.getHosts(filePath) === Some(FiberSensor.OAP_CACHE_HOST_PREFIX + host2 +
      FiberSensor.OAP_CACHE_EXECUTOR_PREFIX + execId2))
  }
}
