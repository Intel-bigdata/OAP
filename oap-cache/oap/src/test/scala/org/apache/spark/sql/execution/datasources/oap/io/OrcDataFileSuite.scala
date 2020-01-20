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

package org.apache.spark.sql.execution.datasources.oap.io

import java.io.File

import org.apache.orc.mapred.OrcStruct
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

class OrcDataFileSuite extends QueryTest with SharedOapContext with BeforeAndAfter {
  import testImplicits._
  private var path: File = _
  private var fileName: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    path = Utils.createTempDir()
    path.delete()

    val df = sparkContext.parallelize(1 to 5000, 1)
      .map(i => (i, i + 100L, s"this is row $i"))
      .toDF("a", "b", "c")

    df.write.format("orc").mode(SaveMode.Overwrite).save(path.getAbsolutePath)

    for (iterator <- path.listFiles()) {
      if (iterator.toString.endsWith(".orc")) {
        fileName = iterator.toString()
      }
    }
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(path)
    } finally {
      super.afterAll()
    }
  }

  override def afterEach(): Unit = {
    configuration.unset(OapConf.OAP_ORC_BINARY_DATA_CACHE_ENABLED.key)
  }

  private val partitionSchema: StructType = new StructType()
    .add(StructField("int32_field", IntegerType))
    .add(StructField("int64_field", LongType))
    .add(StructField("string_field", StringType))

  private val partitionValues: InternalRow =
    InternalRow(1, 101L, UTF8String.fromString("this is row 1"))

  private val requestSchema: StructType = new StructType()
    .add(StructField("int32_field", IntegerType))
    .add(StructField("string_field", StringType))

  test("read by columnIds with columnar batch") {
    val reader = OrcDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0, 2)
    val context = OrcDataFileContext(partitionSchema, partitionValues, true, requestSchema,
      partitionSchema, false, false, requiredIds)
    reader.setOrcDataFileContext(context)
    val iterator = reader.iterator(requiredIds)
      .asInstanceOf[OapCompletionIterator[ColumnarBatch]]
    var totalRows = 0
    var totalBatches = 0
    // By default, the columnar batch has 4096 rows.
    while (iterator.hasNext) {
      val columnarBatch = iterator.next
      assert(columnarBatch.isInstanceOf[ColumnarBatch])
      totalRows += columnarBatch.numRows
      totalBatches += 1
    }
    iterator.close()
    assert(totalRows == 5000)
    assert(totalBatches == (5000 / 4096 + 1))
  }

  test("read by columnIds with columnar batch when cache") {
    configuration.setBoolean(OapConf.OAP_ORC_BINARY_DATA_CACHE_ENABLED.key, true)
    OapRuntime.getOrCreate.fiberCacheManager.clearAllFibers()
    val reader = OrcDataFile(fileName, partitionSchema, configuration)
    val requiredIds = Array(0, 2)
    val context = OrcDataFileContext(partitionSchema, partitionValues, true, requestSchema,
      partitionSchema, false, true, requiredIds)
    reader.setOrcDataFileContext(context)
    val iterator = reader.iterator(requiredIds)
      .asInstanceOf[OapCompletionIterator[ColumnarBatch]]
    var totalRows = 0
    var totalBatches = 0
    // By default, the columnar batch has 4096 rows.
    while (iterator.hasNext) {
      val columnarBatch = iterator.next
      assert(columnarBatch.isInstanceOf[ColumnarBatch])
      totalRows += columnarBatch.numRows
      totalBatches += 1
    }
    iterator.close()
    assert(totalRows == 5000)
    assert(totalBatches == (5000 / 4096 + 1))
  }

  test("read by columnIds and rowIds with columnar batch") {
    val reader = OrcDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0, 2)
    val context = OrcDataFileContext(partitionSchema, partitionValues, true, requestSchema,
      partitionSchema, false, false, requiredIds)
    reader.setOrcDataFileContext(context)
    val rowIds = Array(0, 1, 7, 8, 120, 121, 381, 382)
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[ColumnarBatch]]
    var totalRows = 0
    var totalBatches = 0
    while (iterator.hasNext) {
      val columnarBatch = iterator.next
      assert(columnarBatch.isInstanceOf[ColumnarBatch])
      totalRows += columnarBatch.numRows
      totalBatches += 1
    }
    iterator.close()
    // The batch is only 1, since the row Ids are covered by the same batch.
    assert(totalRows == 4096)
    assert(totalBatches == 1)
  }

  test("read by columnIds and rowIds with columnar batch when cache") {
    configuration.setBoolean(OapConf.OAP_ORC_BINARY_DATA_CACHE_ENABLED.key, true)
    OapRuntime.getOrCreate.fiberCacheManager.clearAllFibers()
    val reader = OrcDataFile(fileName, partitionSchema, configuration)
    val requiredIds = Array(0, 2)
    val context = OrcDataFileContext(partitionSchema, partitionValues, true, requestSchema,
      partitionSchema, false, true, requiredIds)
    reader.setOrcDataFileContext(context)
    val rowIds = Array(0, 1, 7, 8, 120, 121, 381, 382)
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[ColumnarBatch]]
    var totalRows = 0
    var totalBatches = 0
    while (iterator.hasNext) {
      val columnarBatch = iterator.next
      assert(columnarBatch.isInstanceOf[ColumnarBatch])
      totalRows += columnarBatch.numRows
      totalBatches += 1
    }
    iterator.close()
    // The batch is only 1, since the row Ids are covered by the same batch.
    assert(totalRows == 4096)
    assert(totalBatches == 1)
  }

  test("read by columnIds and empty rowIds with columnar batch") {
    val reader = OrcDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0, 2)
    val context = OrcDataFileContext(partitionSchema, partitionValues, true, requestSchema,
      partitionSchema, false, false, requiredIds)
    reader.setOrcDataFileContext(context)
    val rowIds = Array.emptyIntArray
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[ColumnarBatch]]
    assert(!iterator.hasNext)
    val e = intercept[java.util.NoSuchElementException] {
      iterator.next()
    }.getMessage
    iterator.close()
    assert(e.contains("next on empty iterator"))
  }

  test("read by columnIds without columnar batch") {
    val reader = OrcDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0, 2)
    val context = OrcDataFileContext(partitionSchema, partitionValues, false, requestSchema,
      partitionSchema, false, false, requiredIds)
    reader.setOrcDataFileContext(context)
    val iterator = reader.iterator(requiredIds)
      .asInstanceOf[OapCompletionIterator[OrcStruct]]
    var totalRows = 0
    while (iterator.hasNext) {
      val orcStruct = iterator.next
      assert(orcStruct.isInstanceOf[OrcStruct])
      totalRows += 1
    }
    iterator.close()
    assert(totalRows == 5000)
  }

  test("read by columnIds and rowIds without columnar batch") {
    val reader = OrcDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0, 2)
    val context = OrcDataFileContext(partitionSchema, partitionValues, false, requestSchema,
      partitionSchema, false, false, requiredIds)
    reader.setOrcDataFileContext(context)
    val rowIds = Array(0, 1, 7, 8, 120, 121, 381, 382)
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[OrcStruct]]
    var totalRows = 0
    while (iterator.hasNext) {
      val value = iterator.next
      assert(value.isInstanceOf[OrcStruct])
      totalRows += 1
    }
    iterator.close()
    assert(totalRows < 5000 && totalRows > rowIds.length)
  }

  test("test orc data file meta") {
    val reader = OrcDataFile(fileName, requestSchema, configuration)
    val meta = reader.getDataFileMeta()
    assert(meta != null && meta.isInstanceOf[OrcDataFileMeta])
    assert(meta.getFieldCount == 3)
  }

}
