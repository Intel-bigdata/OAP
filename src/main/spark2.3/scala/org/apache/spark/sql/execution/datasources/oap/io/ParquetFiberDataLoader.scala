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

import java.io.IOException
import java.util.TimeZone

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFiberDataReader
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.utils.Collections3

import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupportWrapper, VectorizedColumnReader}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types._

/**
 * The main purpose of this loader is help to obtain
 * one column of one rowgroup in data loading phase.
 *
 * @param configuration hadoop configuration
 * @param reader which holds the inputstream at the life cycle of the cache load.
 * @param blockId represents which block will be load.
  */
private[oap] case class ParquetFiberDataLoader(
    configuration: Configuration,
    reader: ParquetFiberDataReader,
    blockId: Int) {

  @throws[IOException]
  def loadSingleColumn: FiberCache = {
    val footer = reader.getFooter
    val fileSchema = footer.getFileMetaData.getSchema
    val fileMetadata = footer.getFileMetaData.getKeyValueMetaData
    val readContext = new ParquetReadSupportWrapper()
      .init(new InitContext(configuration, Collections3.toSetMultiMap(fileMetadata), fileSchema))
    val requestedSchema = readContext.getRequestedSchema
    val sparkRequestedSchemaString =
      configuration.get(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
    val sparkSchema = StructType.fromString(sparkRequestedSchemaString)
    assert(sparkSchema.length == 1, s"Only can get single column every time " +
      s"by loadSingleColumn, the columns = ${sparkSchema.mkString}")
    val dataType = sparkSchema.fields(0).dataType
    // Notes: rowIds is IntegerType in oap index.
    val rowCount = reader.getFooter.getBlocks.get(blockId).getRowCount.toInt

    val column = new OnHeapColumnVector(rowCount, dataType)
    val columnDescriptor = requestedSchema.getColumns.get(0)
    val originalType = requestedSchema.asGroupType.getFields.get(0).getOriginalType
    val blockMetaData = footer.getBlocks.get(blockId)
    val fiberData = reader.readFiberData(blockMetaData, columnDescriptor)
    val columnReader =
      new VectorizedColumnReader(columnDescriptor, originalType,
        fiberData.getPageReader(columnDescriptor), TimeZone.getDefault)
    columnReader.readBatch(rowCount, column)
    ParquetDataFiberWriter.dumpToCache(column.asInstanceOf[OnHeapColumnVector], rowCount)
  }
}
