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

package org.apache.spark.shuffle.remote

import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.model.{HttpRequest, HttpResponse}

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.util.Utils

class RemoteShuffleManagerSuite extends SparkFunSuite with LocalSparkContext {

  testWithMultiplePath("repartition")(repartition(100, 10, 20))
  testWithMultiplePath("re-large-partition")(repartition(1000000, 3, 2))

  testWithMultiplePath(
    "repartition with some map output empty")(repartitionWithEmptyMapOutput)

  testWithMultiplePath("sort")(sort(500, 13, true))
  testWithMultiplePath("sort large partition")(sort(500000, 2))

  test("disable bypass-merge-sort shuffle writer by default") {
    sc = new SparkContext("local", "test", new SparkConf(true))
    val partitioner = new HashPartitioner(100)
    val rdd = sc.parallelize((1 to 10).map(x => (x, x + 1)), 10)
    val dependency = new ShuffleDependency[Int, Int, Int](rdd, partitioner)
    assert(RemoteShuffleManager.shouldBypassMergeSort(new SparkConf(true), dependency)
        == false)
  }

  test("Remote shuffle and external shuffle service cannot be enabled at the same time") {
    intercept[Exception] {
      sc = new SparkContext(
        "local",
        "test",
        new SparkConf(true)
            .set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager")
            .set("spark.shuffle.service.enabled", "true"))
    }
  }

  test("request HDFS configuration from remote storage master") {
    val expectKey = "whatever"
    val expectVal = "55555"
    val mockHadoopConf: String = s"<configuration><property><name>$expectKey</name>" +
        s"<value>$expectVal</value></property></configuration>"
    val port = 56789

    val mockServer = startClientAndServer(port)
    mockServer.when(HttpRequest.request.withPath("/conf"))
        .respond(HttpResponse.response().withBody(mockHadoopConf))

    try {
      val conf = new SparkConf(false)
          .set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager")
          .set(RemoteShuffleConf.STORAGE_HDFS_MASTER_UI_PORT, port.toString)
          .set("spark.shuffle.remote.storageMasterUri", "hdfs://localhost:9001")
      val manager = new RemoteShuffleManager(conf)
      assert(manager.getHadoopConf.get(expectKey) == expectVal)
    }
    finally {
      mockServer.stop()
    }
  }

  test("request HDFS configuration from remote storage master:" +
    " unset port or no connection cause no exception") {
    val conf = new SparkConf(false)
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager")
      .set("spark.shuffle.remote.storageMasterUri", "hdfs://localhost:9001")
    val manager = new RemoteShuffleManager(conf)
    manager.getHadoopConf
  }

  // Optimized shuffle writer & non-optimized shuffle writer
  private def testWithMultiplePath(name: String, loadDefaults: Boolean = true)
      (body: (SparkConf => Unit)): Unit = {
    test(name + " with general shuffle path") {
      body(createSparkConf(loadDefaults, bypassMergeSort = false, unsafeOptimized = false))
    }
    test(name + " with optimized shuffle path") {
      body(createSparkConf(loadDefaults, bypassMergeSort = false, unsafeOptimized = true))
    }
    test(name + " with bypass-merge-sort shuffle path") {
      body(createSparkConf(loadDefaults, bypassMergeSort = true, unsafeOptimized = false))
    }
    test(name + " with bypass-merge-sort shuffle path + index cache") {
      body(createSparkConf(loadDefaults,
        bypassMergeSort = true, unsafeOptimized = false, indexCache = true))
    }
    test(name + " with optimized shuffle path + index cache") {
      body(createSparkConf(loadDefaults,
        bypassMergeSort = false, unsafeOptimized = true, indexCache = true))
    }
    test(name + " with whatever shuffle write path + constraining maxBlocksPerAdress") {
      body(createSparkConf(loadDefaults, indexCache = false, setMaxBlocksPerAdress = true))
    }
    test(name + " with whatever shuffle write path + index cache + constraining maxBlocksPerAdress")
    {
      body(createSparkConf(loadDefaults, indexCache = true, setMaxBlocksPerAdress = true))
    }
    val default = RemoteShuffleConf.DATA_FETCH_EAGER_REQUIREMENT.defaultValue.get
    val testWith = (true ^ default)
    test(name + s" with eager requirement = ${testWith}")
    {
      body(createSparkConf(loadDefaults, indexCache = true)
        .set(RemoteShuffleConf.DATA_FETCH_EAGER_REQUIREMENT.key, testWith.toString))
    }
  }

  private def repartition(
      dataSize: Int, preShuffleNumPartitions: Int, postShuffleNumPartitions: Int)
      (conf: SparkConf): Unit = {
    sc = new SparkContext("local", "test_repartition", conf)
    val data = 0 until dataSize
    val rdd = sc.parallelize(data, preShuffleNumPartitions)
    val newRdd = rdd.repartition(postShuffleNumPartitions)
    assert(newRdd.collect().sorted === data)
  }

  private def repartitionWithEmptyMapOutput(conf: SparkConf): Unit = {
    sc = new SparkContext("local", "test_repartition_empty", conf)
    val data = 0 until 20
    val rdd = sc.parallelize(data, 30)
    val newRdd = rdd.repartition(40)
    assert(newRdd.collect().sorted === data)
  }

  private def sort(
      dataSize: Int, numPartitions: Int, differentMapSidePartitionLength: Boolean = false)
      (conf: SparkConf): Unit = {
    sc = new SparkContext("local", "sort", conf)
    val data = if (differentMapSidePartitionLength) {
      List.fill(dataSize/2)(0) ++ (dataSize / 2 until dataSize)
    } else {
      0 until dataSize
    }
    val rdd = sc.parallelize(Utils.randomize(data), numPartitions)

    val newRdd = rdd.sortBy((x: Int) => x.toLong)
    assert(newRdd.collect() === data)
  }

  private def createSparkConf(
      loadDefaults: Boolean, bypassMergeSort: Boolean = false, unsafeOptimized: Boolean = true,
      indexCache: Boolean = false, setMaxBlocksPerAdress: Boolean = false): SparkConf = {
    val smallThreshold = 1
    val largeThreshold = 50
    val conf = createDefaultConf(loadDefaults)
        .set("spark.shuffle.optimizedPathEnabled", unsafeOptimized.toString)
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager")
        // Use a strict threshold as default so that Bypass-Merge-Sort shuffle writer won't be used
        .set("spark.shuffle.sort.bypassMergeThreshold", smallThreshold.toString)
    if (bypassMergeSort) {
      // Use a loose threshold
      conf.set("spark.shuffle.sort.bypassMergeThreshold", largeThreshold.toString)
    }
    if (indexCache) {
      conf.set("spark.shuffle.remote.index.cache.size", "3m")
    }
    if (setMaxBlocksPerAdress) {
      conf.set(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS.key, "1")
    }
    conf
  }

}
