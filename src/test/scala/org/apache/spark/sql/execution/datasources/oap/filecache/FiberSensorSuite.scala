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

import java.util.concurrent.ConcurrentHashMap
import java.util.function

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberSensor.HostFiberCache
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.oap.listener.{OapListener, SparkListenerCustomInfoUpdate}
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OapBitSet

class FiberSensorSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {

  import testImplicits._

  private var currentPath: String = _

  private var fiberSensor: FiberSensor = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    fiberSensor = OapRuntime.getOrCreate.fiberSensor
    sparkContext.addSparkListener(new OapListener)
  }

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
    currentPath = path

    sql(s"""CREATE TEMPORARY VIEW oap_test (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path')""".stripMargin)
    OapRuntime.getOrCreate.fiberCacheManager.clearAllFibers()
    fiberSensor.executorToCacheManager.clear()
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
  }

  test("FiberSensor with sql") {

    def getCacheStats(fiberSensor: FiberSensor): CacheStats =
      fiberSensor.executorToCacheManager.asScala.toMap.values.foldLeft(
        CacheStats())((sum, cache) => sum + cache)

    // make each dataFile has 2 rowGroup.
    // 3000 columns in total, 2 data file by default, 1500 columns each file.
    // So, each file will have 2 rowGroup.
    sqlContext.conf.setConfString(OapConf.OAP_ROW_GROUP_SIZE.key, "1000")

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
      val summary = getCacheStats(fiberSensor)
      logWarning(s"Summary1: ${summary.toDebugString}")
      assertResult(1)(fiberSensor.executorToCacheManager.size())

      // all data are cached when run another sql.
      // Expect: 1.hitCount increase; 2.missCount equal
      // wait for a heartbeat period
      checkAnswer(sql("SELECT * FROM oap_test WHERE a > 200 AND a < 2400"),
        data.filter(r => r._1 > 200 && r._1 < 2400).map(r => Row(r._1, r._2)))
      CacheStats.reset
      Thread.sleep(15 * 1000)
      val summary2 = getCacheStats(fiberSensor)
      logWarning(s"Summary2: ${summary2.toDebugString}")
      assertResult(1)(fiberSensor.executorToCacheManager.size())
      assert(summary.hitCount < summary2.hitCount)
      assertResult(summary.missCount)(summary2.missCount)
      assertResult(summary.dataFiberCount)(summary2.dataFiberCount)
      assertResult(summary.dataFiberSize)(summary2.dataFiberSize)
      assertResult(summary.indexFiberCount)(summary2.indexFiberCount)
      assertResult(summary.indexFiberSize)(summary2.indexFiberSize)
    }
  }

  test("FiberSensor onCustomInfoUpdate FiberCacheManagerMessager") {
    val host = "0.0.0.0"
    val execID = "exec1"
    val messager = "FiberCacheManagerMessager"
    // Test json empty, no more executor added
    val listener = new OapListener
    listener.onOtherEvent(SparkListenerCustomInfoUpdate(host, execID, messager, ""))
    assertResult(0)(fiberSensor.executorToCacheManager.size())

    // Test json error, no more executor added
    listener.onOtherEvent(SparkListenerCustomInfoUpdate(host, execID, messager, "error msg"))
    assertResult(0)(fiberSensor.executorToCacheManager.size())

    // Test normal msg
    CacheStats.reset
    val conf: SparkConf = new SparkConf()
    conf.set(OapConf.OAP_UPDATE_FIBER_CACHE_METRICS_INTERVAL_SEC.key, 0L.toString)
    val cacheStats = CacheStats(2, 19, 10, 2, 0, 0, 213, 23, 23, 123131, 2)
    listener.onOtherEvent(SparkListenerCustomInfoUpdate(
      host, execID, messager, CacheStats.status(cacheStats, conf)))
    assertResult(1)(fiberSensor.executorToCacheManager.size())
    assertResult(cacheStats.toJson)(
      fiberSensor.executorToCacheManager.get(execID).toJson)
  }

  test("get hosts from FiberSensor") {

    withSQLConf(OapConf.OAP_CACHE_FIBERSENSOR_GETHOSTS_NUM.key -> "2") {

      def getAns(file: String): Seq[String] = fiberSensor.getHosts(file)

      def checkExistence(host: String, execId: String, in: String): Boolean =
        in.contains(host) && in.contains(execId)

      val filePath = "file1"
      val groupCount = 30
      val fieldCount = 3

      // executor1 update
      val host1 = "host1"
      val execId1 = "executor1"
      val bitSet1 = new OapBitSet(90)
      bitSet1.set(1)
      bitSet1.set(2)
      val fcs = Seq(FiberCacheStatus(filePath, bitSet1, groupCount, fieldCount))
      val fiberInfo = SparkListenerCustomInfoUpdate(host1, execId1,
        "OapFiberCacheHeartBeatMessager", CacheStatusSerDe.serialize(fcs))
      fiberSensor.updateLocations(fiberInfo)
      assert(checkExistence(host1, execId1, getAns(filePath)(0)))

      // executor2 update
      val host2 = "host2"
      val execId2 = "executor2"
      val bitSet2 = new OapBitSet(90)
      bitSet2.set(3)
      bitSet2.set(4)
      bitSet2.set(5)
      bitSet2.set(6)
      bitSet2.set(7)
      bitSet2.set(8)

      val fiberInfo2 = SparkListenerCustomInfoUpdate(host2, execId2,
        "OapFiberCacheHeartBeatMessager", CacheStatusSerDe
            .serialize(Seq(FiberCacheStatus(filePath, bitSet2, groupCount, fieldCount))))
      fiberSensor.updateLocations(fiberInfo2)
      assert(checkExistence(host2, execId2, getAns(filePath)(0)))
      assert(checkExistence(host1, execId1, getAns(filePath)(1)))

      // Another file cache update, doesn't influence filePath
      val filePath2 = "file2"
      val host3 = "host3"
      val execId3 = "executor2"
      val bitSet3 = new OapBitSet(90)
      bitSet3.set(7)
      bitSet3.set(8)
      bitSet3.set(9)
      bitSet3.set(10)
      val fiberInfo3 = SparkListenerCustomInfoUpdate(host3, execId3,
        "OapFiberCacheHeartBeatMessager", CacheStatusSerDe
            .serialize(Seq(FiberCacheStatus(filePath2, bitSet3, groupCount, fieldCount))))
      fiberSensor.updateLocations(fiberInfo3)
      assert(checkExistence(host2, execId2, getAns(filePath)(0)))
      assert(checkExistence(host1, execId1, getAns(filePath)(1)))

      // New info for filePath host2:executor2, less cached may because of eviction
      val bitSet4 = new OapBitSet(90)
      bitSet4.set(1)

      val fiberInfo4 = SparkListenerCustomInfoUpdate(host2, execId2,
        "OapFiberCacheHeartBeatMessager", CacheStatusSerDe
            .serialize(Seq(FiberCacheStatus(filePath, bitSet4, groupCount, fieldCount))))
      fiberSensor.updateLocations(fiberInfo4)
      // Now host1: execId1 comes first
      assert(checkExistence(host1, execId1, getAns(filePath)(0)))
      assert(checkExistence(host2, execId2, getAns(filePath)(1)))

      // New info for filePath, host1: execId2, Driver maintaining 3 records for it, while
      // NUM_GET_HOSTS returned
      val bitSet5 = new OapBitSet(90)
      bitSet5.set(1)

      val fiberInfo5 = SparkListenerCustomInfoUpdate(host1, execId2,
        "OapFiberCacheHeartBeatMessager", CacheStatusSerDe
            .serialize(Seq(FiberCacheStatus(filePath, bitSet5, groupCount, fieldCount))))
      fiberSensor.updateLocations(fiberInfo5)
      assert(getAns(filePath).length == FiberSensor.NUM_GET_HOSTS)
    }
  }

  test("updateRecordingMap will preserve at most FiberSensor.MAX_HOSTS_MAINTAINED records for " +
      "each file") {
    val filePath = "file"
    val groupCount = 30
    val fieldCount = 3

    val host = "host"
    val execId = "executor"
    val bitSet = new OapBitSet(90)

    (0 until FiberSensor.MAX_HOSTS_MAINTAINED + 1).foreach { i =>
      // The host on the next have more Fibers than this one
      bitSet.set(i)
      val status = FiberCacheStatus(filePath, bitSet, groupCount, fieldCount)
      fiberSensor.updateRecordingMap(s"$host$i $execId$i", status)
      // For the last(MAX_HOSTS_MAINTAINED + 1 th) Fiber inserted, number of records won't increase
      if (i != FiberSensor.MAX_HOSTS_MAINTAINED) {
        assert(fiberSensor.fileToHosts.get(filePath).length == i + 1)
      }
    }

    assert(fiberSensor.fileToHosts.get(filePath).length == FiberSensor.MAX_HOSTS_MAINTAINED)
  }

  test("Discard outdated info") {
    val host = "host"
    fiberSensor.fileToHosts.put(host, new ArrayBuffer[HostFiberCache](0))
    assert(fiberSensor.fileToHosts.get(host) != null)

    fiberSensor.discardOutdatedInfo(host)
    assert(fiberSensor.fileToHosts.get(host) == null)
  }

  test("OAP-1023 test getHosts threadsafe case") {
    val testHostName = "test_host"
    val mockFileToHosts =
      Mockito.mock(classOf[ConcurrentHashMap[String, ArrayBuffer[HostFiberCache]]])
    val tmpHosts = ArrayBuffer(HostFiberCache(testHostName, null))

    Mockito.when(mockFileToHosts.contains(any(classOf[String]))).thenReturn(true)
    Mockito.when(mockFileToHosts.get(any(classOf[String]))).thenReturn(null)
    Mockito.when(mockFileToHosts.computeIfAbsent(any(classOf[String]),
      any(classOf[function.Function[String, ArrayBuffer[HostFiberCache]]]))).thenReturn(tmpHosts)
    val tmpFiberSensor = new FiberSensor(mockFileToHosts)
    val retHosts = tmpFiberSensor.getHosts("test_file")
    assert(retHosts.size == 1 && retHosts.head.equals(testHostName))
  }
}
