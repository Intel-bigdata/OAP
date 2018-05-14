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


class ParquetFooterSuite extends SparkFunSuite {

  test("ParquetFooter#toParquetMetadata(int[])") {

    // prepare data
    val rowGroup0 = new OrderedBlockMetaData(0, new BlockMetaData())
    rowGroup0.setRowCount(33L)
    val rowGroup1 = new OrderedBlockMetaData(1, new BlockMetaData())
    rowGroup1.setRowCount(35L)
    val rowGroup2 = new OrderedBlockMetaData(2, new BlockMetaData())
    rowGroup2.setRowCount(34L)

    val blocks = Lists.newArrayList(rowGroup0, rowGroup1, rowGroup2)

    val schema = Types.buildMessage().named("spark_schema")

    val createBy = "oapTest"

    val fileMeta = new FileMetaData(schema, Collections.emptyMap(), createBy)

    val footer = new ParquetFooter(fileMeta, blocks)

    // 1, 3, 13, 32 => rowGroup0
    // 98 => rowGroup2
    // 133 => Out of bounds
    val rowIds = Array(1, 3, 13, 32, 98, 133)

    // process
    val meta = footer.toParquetMetadata(rowIds)

    // verify
    assert(meta.getFileMetaData.eq(fileMeta))
    val resultBlocks = meta.getBlocks
    assert(resultBlocks.size() == 2)

    val resultRowGroup0 = resultBlocks.get(0).asInstanceOf[IndexedBlockMetaData]
    assert(resultRowGroup0.getRowGroupId.equals(rowGroup0.getRowGroupId))
    assert(resultRowGroup0.getMeta.equals(rowGroup0.getMeta))
    val rowIds0 = resultRowGroup0.getNeedRowIds
    val rowIds0Size = rowIds0.size()
    assert(rowIds0Size == 4)
    assert(rowIds0.get(0) == 1)
    assert(rowIds0.get(1) == 3)
    assert(rowIds0.get(2) == 13)
    assert(rowIds0.get(3) == 32)

    val resultRowGroup1 = resultBlocks.get(1).asInstanceOf[IndexedBlockMetaData]
    assert(resultRowGroup1.getRowGroupId.equals(rowGroup2.getRowGroupId))
    assert(resultRowGroup1.getMeta.equals(rowGroup2.getMeta))
    val rowIds1 = resultRowGroup1.getNeedRowIds
    val rowIds1Size = rowIds1.size()
    assert(rowIds1Size == 1)
    assert(rowIds1.get(0) == 30)
  }

  test("ParquetFooter#toParquetMetadata()") {

    // prepare data
    val rowGroup0 = new OrderedBlockMetaData(0, new BlockMetaData())
    rowGroup0.setRowCount(33L)
    val rowGroup1 = new OrderedBlockMetaData(1, new BlockMetaData())
    rowGroup1.setRowCount(35L)
    val rowGroup2 = new OrderedBlockMetaData(2, new BlockMetaData())
    rowGroup2.setRowCount(34L)

    val blocks = Lists.newArrayList(rowGroup0, rowGroup1, rowGroup2)

    val schema = Types.buildMessage().named("spark_schema")

    val createBy = "oapTest"

    val fileMeta = new FileMetaData(schema, Collections.emptyMap(), createBy)

    val footer = new ParquetFooter(fileMeta, blocks)

    // process
    val meta = footer.toParquetMetadata()

    // verify
    assert(meta.getFileMetaData.eq(fileMeta))
    val resultBlocks = meta.getBlocks
    assert(resultBlocks.size() == 3)

    val resultRowGroup0 = resultBlocks.get(0).asInstanceOf[OrderedBlockMetaData]
    assert(resultRowGroup0.getRowGroupId.equals(rowGroup0.getRowGroupId))
    assert(resultRowGroup0.getMeta.equals(rowGroup0.getMeta))

    val resultRowGroup1 = resultBlocks.get(1).asInstanceOf[OrderedBlockMetaData]
    assert(resultRowGroup1.getRowGroupId.equals(rowGroup1.getRowGroupId))
    assert(resultRowGroup1.getMeta.equals(rowGroup1.getMeta))

    val resultRowGroup2 = resultBlocks.get(2).asInstanceOf[OrderedBlockMetaData]
    assert(resultRowGroup2.getRowGroupId.equals(rowGroup2.getRowGroupId))
    assert(resultRowGroup2.getMeta.equals(rowGroup2.getMeta))
  }

  test("ParquetFooter#toParquetMetadata(int)") {

    // prepare data
    val rowGroup0 = new OrderedBlockMetaData(0, new BlockMetaData())
    rowGroup0.setRowCount(33L)
    val rowGroup1 = new OrderedBlockMetaData(1, new BlockMetaData())
    rowGroup1.setRowCount(35L)
    val rowGroup2 = new OrderedBlockMetaData(2, new BlockMetaData())
    rowGroup2.setRowCount(34L)

    val blocks = Lists.newArrayList(rowGroup0, rowGroup1, rowGroup2)

    val schema = Types.buildMessage().named("spark_schema")

    val createBy = "oapTest"

    val fileMeta = new FileMetaData(schema, Collections.emptyMap(), createBy)

    val footer = new ParquetFooter(fileMeta, blocks)

    // process
    val meta = footer.toParquetMetadata(1)

    // verify
    assert(meta.getFileMetaData.eq(fileMeta))
    val resultBlocks = meta.getBlocks
    assert(resultBlocks.size() == 1)

    val resultRowGroup = resultBlocks.get(0).asInstanceOf[OrderedBlockMetaData]
    assert(resultRowGroup.getRowGroupId.equals(rowGroup1.getRowGroupId))
    assert(resultRowGroup.getMeta.equals(rowGroup1.getMeta))
  }

  test("ParquetFooter#from(ParquetMetadata) ") {

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

    val meta = new ParquetMetadata(fileMeta, blocks)

    // process
    val footer = ParquetFooter.from(meta)

    // verify
    assert(footer.getFileMetaData.eq(fileMeta))
    val resultBlocks = footer.getBlocks
    assert(resultBlocks.size() == 3)

    val resultRowGroup0 = resultBlocks.get(0)
    assert(resultRowGroup0.getRowGroupId.equals(0))
    assert(resultRowGroup0.getMeta.equals(rowGroup0))

    val resultRowGroup1 = resultBlocks.get(1)
    assert(resultRowGroup1.getRowGroupId.equals(1))
    assert(resultRowGroup1.getMeta.equals(rowGroup1))

    val resultRowGroup2 = resultBlocks.get(2)
    assert(resultRowGroup2.getRowGroupId.equals(2))
    assert(resultRowGroup2.getMeta.equals(rowGroup2))
  }
}
