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

package org.apache.spark.sql.execution.datasources.v2.arrow

import java.net.URI
import java.util.{TimeZone, UUID}

import scala.collection.JavaConverters._

import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowOptions
import com.intel.oap.vectorized.ArrowWritableColumnVector
import org.apache.arrow.dataset.file.{FileSystem, SingleFileDatasetFactory}
import org.apache.arrow.dataset.scanner.ScanTask
import org.apache.arrow.memory.{AllocationListener, BaseAllocator}
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.TaskContext
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

object ArrowUtils {

  def getTaskMemoryManager(): TaskMemoryManager = {
    TaskContext.get().taskMemoryManager()
  }

  def readSchema(file: FileStatus, options: CaseInsensitiveStringMap): Option[StructType] = {
    val factory: SingleFileDatasetFactory =
      makeArrowDiscovery(file.getPath.toString, new ArrowOptions(options.asScala.toMap),
        AllocationListener.NOOP)
    val schema = factory.inspect()
    try {
      Option(org.apache.spark.sql.util.ArrowUtils.fromArrowSchema(schema))
    } finally {
      factory.close()
    }
  }

  def readSchema(files: Seq[FileStatus], options: CaseInsensitiveStringMap): Option[StructType] =
    readSchema(files.toList.head, options) // todo merge schema

  def makeArrowDiscovery(file: String, options: ArrowOptions,
                         al: AllocationListener): SingleFileDatasetFactory = {

    val format = getFormat(options).getOrElse(throw new IllegalStateException)
    val fs = getFs(options).getOrElse(throw new IllegalStateException)
    val parent = defaultAllocator()
    val allocator = parent
      .newChildAllocator("Spark Managed Allocator - " + UUID.randomUUID().toString, al,
        0, parent.getLimit)
    val factory = new SingleFileDatasetFactory(
      allocator,
      format,
      fs,
      rewriteFilePath(file))
    factory
  }

  def rewriteFilePath(file: String): String = {
    val uri = URI.create(file)
    if (uri.getScheme == "hdfs") {
      var query = uri.getQuery
      if (query == null) {
        query = "use_hdfs3=1"
      } else {
        query += "&use_hdfs3=1"
      }
      return new URI(uri.getScheme, uri.getAuthority, uri.getPath, query, uri.getFragment).toString
    }
    file
  }

  def toArrowSchema(t: StructType): Schema = {
    // fixme this might be platform dependent
    org.apache.spark.sql.util.ArrowUtils.toArrowSchema(t, TimeZone.getDefault.getID)
  }

  def loadVectors(bundledVectors: ScanTask.ArrowBundledVectors, partitionValues: InternalRow,
                  partitionSchema: StructType, dataSchema: StructType): ColumnarBatch = {
    val rowCount: Int = getRowCount(bundledVectors)
    val dataVectors = getDataVectors(bundledVectors, dataSchema)
    val dictionaryVectors = getDictionaryVectors(bundledVectors, dataSchema)

    val vectors = ArrowWritableColumnVector.loadColumns(rowCount, dataVectors.asJava,
      dictionaryVectors.asJava)
    val partitionColumns = ArrowWritableColumnVector.allocateColumns(rowCount, partitionSchema)
    (0 until partitionColumns.length).foreach(i => {
      ColumnVectorUtils.populate(partitionColumns(i), partitionValues, i)
      partitionColumns(i).setIsConstant()
    })

    val batch = new ColumnarBatch(vectors ++ partitionColumns, rowCount)
    batch
  }

  def defaultAllocator(): BaseAllocator = {
    org.apache.spark.sql.util.ArrowUtils.rootAllocator
  }

  private def getRowCount(bundledVectors: ScanTask.ArrowBundledVectors) = {
    val valueVectors = bundledVectors.valueVectors
    val rowCount = valueVectors.getRowCount
    rowCount
  }

  private def getDataVectors(bundledVectors: ScanTask.ArrowBundledVectors,
                             dataSchema: StructType): List[FieldVector] = {
    // TODO Deprecate following (bad performance maybe brought).
    // TODO Assert vsr strictly matches dataSchema instead.
    val valueVectors = bundledVectors.valueVectors
    dataSchema.map(f => {
      val vector = valueVectors.getVector(f.name)
      if (vector == null) {
        throw new IllegalStateException("Error: no vector named " + f.name + " in record bach")
      }
      vector
    }).toList
  }

  private def getDictionaryVectors(bundledVectors: ScanTask.ArrowBundledVectors,
                                   dataSchema: StructType): List[FieldVector] = {
    val valueVectors = bundledVectors.valueVectors
    val dictionaryVectorMap = bundledVectors.dictionaryVectors

    val fieldNameToDictionaryEncoding = valueVectors.getSchema.getFields.asScala.map(f => {
      f.getName -> f.getDictionary
    }).toMap

    val dictionaryVectorsWithNulls = dataSchema.map(f => {
      val de = fieldNameToDictionaryEncoding(f.name)

      Option(de) match {
        case None => null
        case _ =>
          if (de.getIndexType.getTypeID != ArrowTypeID.Int) {
            throw new IllegalArgumentException("Wrong index type: " + de.getIndexType)
          }
          dictionaryVectorMap.get(de.getId).getVector
      }
    }).toList
    dictionaryVectorsWithNulls
  }

  private def getFormat(
    options: ArrowOptions): Option[org.apache.arrow.dataset.file.FileFormat] = {
    Option(options.originalFormat match {
      case "parquet" => org.apache.arrow.dataset.file.FileFormat.PARQUET
      case _ => throw new IllegalArgumentException("Unrecognizable format")
    })
  }

  private def getFs(options: ArrowOptions): Option[FileSystem] = {
    Option(options.filesystem match {
      case "local" => FileSystem.LOCAL
      case "hdfs" => FileSystem.HDFS
      case _ => throw new IllegalArgumentException("Unrecognizable filesystem")
    })
  }
}
