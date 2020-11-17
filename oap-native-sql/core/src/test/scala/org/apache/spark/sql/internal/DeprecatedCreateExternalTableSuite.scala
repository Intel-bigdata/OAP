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

package org.apache.spark.sql.internal

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeprecatedCreateExternalTableSuite extends SharedSparkSession {

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

  test("createExternalTable with explicit path") {
    withTable("t") {
      withTempDir { dir =>
        val path = new File(dir, "test")
        spark.range(100).write.parquet(path.getAbsolutePath)
        spark.catalog.createExternalTable(
          tableName = "t",
          path = path.getAbsolutePath
        )
        assert(spark.sessionState.catalog.tableExists(TableIdentifier("t")))
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.tableType === CatalogTableType.EXTERNAL)
        assert(table.provider === Some("parquet"))
        assert(table.schema === new StructType().add("id", "long"))
        assert(table.storage.locationUri.get == makeQualifiedPath(path.getAbsolutePath))
      }
    }
  }

  test("createExternalTable with 'path' options") {
    withTable("t") {
      withTempDir { dir =>
        val path = new File(dir, "test")
        spark.range(100).write.parquet(path.getAbsolutePath)
        spark.catalog.createExternalTable(
          tableName = "t",
          source = "parquet",
          options = Map("path" -> path.getAbsolutePath))
        assert(spark.sessionState.catalog.tableExists(TableIdentifier("t")))
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.tableType === CatalogTableType.EXTERNAL)
        assert(table.provider === Some("parquet"))
        assert(table.schema === new StructType().add("id", "long"))
        assert(table.storage.locationUri.get == makeQualifiedPath(path.getAbsolutePath))
      }
    }
  }

  test("createExternalTable with explicit schema") {
    withTable("t") {
      withTempDir { dir =>
        val path = new File(dir, "test")
        spark.range(100).write.parquet(path.getAbsolutePath)
        spark.catalog.createExternalTable(
          tableName = "t",
          source = "parquet",
          schema = new StructType().add("i", "int"),
          options = Map("path" -> path.getAbsolutePath))
        assert(spark.sessionState.catalog.tableExists(TableIdentifier("t")))
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.tableType === CatalogTableType.EXTERNAL)
        assert(table.provider === Some("parquet"))
        assert(table.schema === new StructType().add("i", "int"))
        assert(table.storage.locationUri.get == makeQualifiedPath(path.getAbsolutePath))
      }
    }
  }
}
