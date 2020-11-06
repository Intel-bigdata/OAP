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

package org.apache.spark.sql

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

class SingleLevelAggregateHashMapSuite extends DataFrameAggregateSuite with BeforeAndAfter {

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
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key, "false")

  // adding some checking after each test is run, assuring that the configs are not changed
  // in test code
  after {
    assert(sparkConf.get(SQLConf.CODEGEN_FALLBACK.key) == "false",
      "configuration parameter changed in test body")
    assert(sparkConf.get(SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key) == "false",
      "configuration parameter changed in test body")
  }
}

class TwoLevelAggregateHashMapSuite extends DataFrameAggregateSuite with BeforeAndAfter {

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
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key, "true")

  // adding some checking after each test is run, assuring that the configs are not changed
  // in test code
  after {
    assert(sparkConf.get(SQLConf.CODEGEN_FALLBACK.key) == "false",
      "configuration parameter changed in test body")
    assert(sparkConf.get(SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key) == "true",
      "configuration parameter changed in test body")
  }
}

class TwoLevelAggregateHashMapWithVectorizedMapSuite
  extends DataFrameAggregateSuite
  with BeforeAndAfter {

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
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key, "true")
      .set(SQLConf.ENABLE_VECTORIZED_HASH_MAP.key, "true")

  // adding some checking after each test is run, assuring that the configs are not changed
  // in test code
  after {
    assert(sparkConf.get(SQLConf.CODEGEN_FALLBACK.key) == "false",
      "configuration parameter changed in test body")
    assert(sparkConf.get(SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key) == "true",
      "configuration parameter changed in test body")
    assert(sparkConf.get(SQLConf.ENABLE_VECTORIZED_HASH_MAP.key) == "true",
      "configuration parameter changed in test body")
  }
}

