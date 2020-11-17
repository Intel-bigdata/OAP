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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

/**
 * Read schema suites have the following hierarchy and aims to guarantee users
 * a backward-compatible read-schema change coverage on file-based data sources, and
 * to prevent future regressions.
 *
 *   ReadSchemaSuite
 *     -> CSVReadSchemaSuite
 *     -> HeaderCSVReadSchemaSuite
 *
 *     -> JsonReadSchemaSuite
 *
 *     -> OrcReadSchemaSuite
 *     -> VectorizedOrcReadSchemaSuite
 *     -> MergedOrcReadSchemaSuite
 *
 *     -> ParquetReadSchemaSuite
 *     -> VectorizedParquetReadSchemaSuite
 *     -> MergedParquetReadSchemaSuite
 *
 *     -> AvroReadSchemaSuite
 */

/**
 * All file-based data sources supports column addition and removal at the end.
 */
abstract class ReadSchemaSuite
  extends AddColumnTest
  with HideColumnAtTheEndTest {

  var originalConf: Boolean = _
}

class CSVReadSchemaSuite
  extends ReadSchemaSuite
  with IntegralTypeTest
  with ToDoubleTypeTest
  with ToDecimalTypeTest
  with ToStringTypeTest {

  override val format: String = "csv"

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
}

class HeaderCSVReadSchemaSuite
  extends ReadSchemaSuite
  with IntegralTypeTest
  with ToDoubleTypeTest
  with ToDecimalTypeTest
  with ToStringTypeTest {

  override val format: String = "csv"

  override val options = Map("header" -> "true")

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
}

class JsonReadSchemaSuite
  extends ReadSchemaSuite
  with AddColumnIntoTheMiddleTest
  with HideColumnInTheMiddleTest
  with AddNestedColumnTest
  with HideNestedColumnTest
  with ChangePositionTest
  with IntegralTypeTest
  with ToDoubleTypeTest
  with ToDecimalTypeTest
  with ToStringTypeTest {

  override val format: String = "json"

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
}

class OrcReadSchemaSuite
  extends ReadSchemaSuite
  with AddColumnIntoTheMiddleTest
  with HideColumnInTheMiddleTest
  with AddNestedColumnTest
  with HideNestedColumnTest
  with ChangePositionTest {

  override val format: String = "orc"

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

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalConf = spark.conf.get(SQLConf.ORC_VECTORIZED_READER_ENABLED)
    spark.conf.set(SQLConf.ORC_VECTORIZED_READER_ENABLED.key, "false")
  }

  override def afterAll(): Unit = {
    spark.conf.set(SQLConf.ORC_VECTORIZED_READER_ENABLED.key, originalConf)
    super.afterAll()
  }
}

class VectorizedOrcReadSchemaSuite
  extends ReadSchemaSuite
  with AddColumnIntoTheMiddleTest
  with HideColumnInTheMiddleTest
  with AddNestedColumnTest
  with HideNestedColumnTest
  with ChangePositionTest
  with BooleanTypeTest
  with IntegralTypeTest
  with ToDoubleTypeTest {

  override val format: String = "orc"
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


  override def beforeAll(): Unit = {
    super.beforeAll()
    originalConf = spark.conf.get(SQLConf.ORC_VECTORIZED_READER_ENABLED)
    spark.conf.set(SQLConf.ORC_VECTORIZED_READER_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.conf.set(SQLConf.ORC_VECTORIZED_READER_ENABLED.key, originalConf)
    super.afterAll()
  }
}

class MergedOrcReadSchemaSuite
  extends ReadSchemaSuite
  with AddColumnIntoTheMiddleTest
  with HideColumnInTheMiddleTest
  with AddNestedColumnTest
  with HideNestedColumnTest
  with ChangePositionTest
  with BooleanTypeTest
  with IntegralTypeTest
  with ToDoubleTypeTest {

  override val format: String = "orc"

  override protected def sparkConf: SparkConf =
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
      .set(SQLConf.ORC_SCHEMA_MERGING_ENABLED.key, "true")
}

class ParquetReadSchemaSuite
  extends ReadSchemaSuite
  with AddColumnIntoTheMiddleTest
  with HideColumnInTheMiddleTest
  with AddNestedColumnTest
  with HideNestedColumnTest
  with ChangePositionTest {

  override val format: String = "parquet"
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

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalConf = spark.conf.get(SQLConf.PARQUET_VECTORIZED_READER_ENABLED)
    spark.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "false")
  }

  override def afterAll(): Unit = {
    spark.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, originalConf)
    super.afterAll()
  }
}

class VectorizedParquetReadSchemaSuite
  extends ReadSchemaSuite
  with AddColumnIntoTheMiddleTest
  with HideColumnInTheMiddleTest
  with AddNestedColumnTest
  with HideNestedColumnTest
  with ChangePositionTest {

  override val format: String = "parquet"

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

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalConf = spark.conf.get(SQLConf.PARQUET_VECTORIZED_READER_ENABLED)
    spark.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, originalConf)
    super.afterAll()
  }
}

class MergedParquetReadSchemaSuite
  extends ReadSchemaSuite
  with AddColumnIntoTheMiddleTest
  with HideColumnInTheMiddleTest
  with AddNestedColumnTest
  with HideNestedColumnTest
  with ChangePositionTest {

  override val format: String = "parquet"

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

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalConf = spark.conf.get(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED)
    spark.conf.set(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.conf.set(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key, originalConf)
    super.afterAll()
  }
}
