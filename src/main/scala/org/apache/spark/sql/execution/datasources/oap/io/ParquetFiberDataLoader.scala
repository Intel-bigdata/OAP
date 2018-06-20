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

import java.io.{Closeable, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFiberDataReader
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.utils.Collections3
import org.apache.parquet.schema.{MessageType, Type}

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupportWrapper, VectorizedColumnReader, VectorizedColumnReaderWrapper}
import org.apache.spark.sql.execution.vectorized.{ColumnVector, OapOnHeapColumnVectorFiber, OnHeapColumnVector}
import org.apache.spark.sql.types.StructType

private[oap] case class ParquetFiberDataLoader(
    configuration: Configuration,
    reader: ParquetFiberDataReader,
    blockId: Int,
    rowCount: Int) extends Closeable {

  private var fiber: OapOnHeapColumnVectorFiber = _

  @throws[IOException]
  def load: FiberUsable = {
    val footer = reader.getFooter
    val fileSchema = footer.getFileMetaData.getSchema
    val fileMetadata = footer.getFileMetaData.getKeyValueMetaData
    val readContext = new ParquetReadSupportWrapper()
      .init(new InitContext(configuration, Collections3.toSetMultiMap(fileMetadata), fileSchema))
    val requestedSchema = readContext.getRequestedSchema
    val sparkRequestedSchemaString =
      configuration.get(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
    val sparkSchema = StructType.fromString(sparkRequestedSchemaString)
    val dataType = sparkSchema.fields(0).dataType
    val vector = ColumnVector.allocate(rowCount, dataType, MemoryMode.ON_HEAP)
    this.fiber =
      new OapOnHeapColumnVectorFiber(vector.asInstanceOf[OnHeapColumnVector], rowCount, dataType)

    if (isMissingColumn(fileSchema, requestedSchema)) {
      vector.putNulls(0, rowCount)
      vector.setIsConstant()
    } else {
      val columnDescriptor = requestedSchema.getColumns.get(0)
      val blockMetaData = footer.getBlocks.get(blockId)
      val fiberData = reader.readFiberData(blockMetaData, columnDescriptor)
      val columnReader =
        new VectorizedColumnReaderWrapper(
          new VectorizedColumnReader(columnDescriptor, fiberData.getPageReader(columnDescriptor)))
      columnReader.readBatch(rowCount, vector)
    }
    fiber
  }

  @throws[UnsupportedOperationException]
  @throws[IOException]
  private def isMissingColumn(fileSchema: MessageType, requestedSchema: MessageType) = {
    val dataType = requestedSchema.getType(0)
    if (!dataType.isPrimitive || dataType.isRepetition(Type.Repetition.REPEATED)) {
      throw new UnsupportedOperationException(s"Complex types ${dataType.getName} not supported.")
    }
    val colPath = requestedSchema.getPaths.get(0)
    if (fileSchema.containsPath(colPath)) {
      val fd = fileSchema.getColumnDescription(colPath)
      if (!fd.equals(requestedSchema.getColumns.get(0))) {
        throw new UnsupportedOperationException("Schema evolution not supported.")
      }
      false
    } else {
      if (requestedSchema.getColumns.get(0).getMaxDefinitionLevel == 0) {
        throw new IOException(s"Required column is missing in data file. Col: ${colPath.mkString}")
      }
      true
    }
  }

  override def close(): Unit = {
    if (fiber != null) {
      fiber.close()
    }
  }

  def closeWithReader(): Unit = {
    close()
    if (reader != null) {
      reader.close()
    }
  }
}
