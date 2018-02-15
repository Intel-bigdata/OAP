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
package org.apache.spark.sql.parquet.hadoop.meta

import java.util.Collections

import com.google.common.collect.Lists
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.schema.Types

import org.apache.spark.SparkFunSuite

class IndexedParquetMetadataSuite extends SparkFunSuite {

  test("IndexedParquetMetadata from footer and globalRowIds") {

    // prepare data
    val rowGroup0 = new BlockMetaData()
    rowGroup0.setRowCount(33L)
    val rowGroup1 = new BlockMetaData()
    rowGroup1.setRowCount(35L)
    val rowGroup2 = new BlockMetaData()
    rowGroup2.setRowCount(34L)

    val blocks = Lists.newArrayList(rowGroup0, rowGroup1, rowGroup2)

    val schema = Types.buildMessage().named("spark_schema")

    val createBy = "oapTest"

    val fileMeta = new FileMetaData(schema, Collections.emptyMap(), createBy)

    val footer = new ParquetMetadata(fileMeta, blocks)

    // 1, 3, 13, 32 => rowGroup0
    // 98 => rowGroup2
    // 133 => Out of bounds
    val rowIds = Array(1, 3, 13, 32, 98, 133)

    // process
    val indexedFooter = IndexedParquetMetadata.from(footer, rowIds)

    // verify
    assert(indexedFooter.getFileMetaData.eq(fileMeta))
    val resultBlocks = indexedFooter.getBlocks
    assert(resultBlocks.size() == 2)
    assert(resultBlocks.contains(rowGroup0))
    assert(resultBlocks.contains(rowGroup2))
    val rowIdsList = indexedFooter.getRowIdsList
    assert(rowIdsList.size() == 2)
    val rowIds0 = rowIdsList.get(0)
    val rowIds0Size = rowIds0.size()
    assert(rowIds0Size == 4)
    assert(rowIds0.get(0) == 1)
    assert(rowIds0.get(1) == 3)
    assert(rowIds0.get(2) == 13)
    assert(rowIds0.get(3) == 32)
    val rowIds1 = rowIdsList.get(1)
    val rowIds1Size = rowIds1.size()
    assert(rowIds1Size == 1)
    assert(rowIds1.get(0) == 30)
  }
}
