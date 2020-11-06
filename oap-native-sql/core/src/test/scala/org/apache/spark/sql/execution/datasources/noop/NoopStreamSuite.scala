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

package org.apache.spark.sql.execution.datasources.noop

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamTest, StreamingQuery, Trigger}

class NoopStreamSuite extends StreamTest {
  import testImplicits._

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test")
      .set("spark.sql.parquet.columnarReaderBatchSize", "4096")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
      .set("spark.sql.execution.arrow.maxRecordsPerBatch", "4096")
      //.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "10m")
      .set("spark.sql.join.preferSortMergeJoin", "false")
      .set("spark.sql.columnar.codegen.hashAggregate", "false")
      .set("spark.oap.sql.columnar.wholestagecodegen", "false")
      .set("spark.sql.columnar.window", "false")
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      //.set("spark.sql.columnar.tmp_dir", "/codegen/nativesql/")
      .set("spark.sql.columnar.sort.broadcastJoin", "true")
      .set("spark.oap.sql.columnar.preferColumnar", "true")

  test("microbatch") {
    val input = MemoryStream[Int]
    val query = input.toDF().writeStream.format("noop").start()
    testMicroBatchQuery(query, input)
  }

  test("microbatch restart with checkpoint") {
    val input = MemoryStream[Int]
    withTempDir { checkpointDir =>
      def testWithCheckpoint(): Unit = {
        val query = input.toDF().writeStream
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .format("noop")
          .start()
        testMicroBatchQuery(query, input)
      }
      testWithCheckpoint()
      testWithCheckpoint()
    }
  }

  private def testMicroBatchQuery(
      query: StreamingQuery,
      input: MemoryStream[Int],
      data: Int*): Unit = {
    assert(query.isActive)
    try {
      input.addData(1, 2, 3)
      eventually(timeout(streamingTimeout)) {
        assert(query.recentProgress.map(_.numInputRows).sum == 3)
      }
    } finally {
      query.stop()
    }
  }

  test("continuous") {
    val input = getRateDataFrame()
    val query = input.writeStream.format("noop").trigger(Trigger.Continuous(200)).start()
    assert(query.isActive)
    query.stop()
  }

  test("continuous restart with checkpoint") {
    withTempDir { checkpointDir =>
      def testWithCheckpoint(): Unit = {
        val input = getRateDataFrame()
        val query = input.writeStream
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .format("noop")
          .trigger(Trigger.Continuous(200))
          .start()
        assert(query.isActive)
        query.stop()
      }
      testWithCheckpoint()
      testWithCheckpoint()
    }
  }

  private def getRateDataFrame(): DataFrame = {
    spark.readStream
      .format("rate")
      .option("numPartitions", "1")
      .option("rowsPerSecond", "5")
      .load()
      .select('value)
  }
}

