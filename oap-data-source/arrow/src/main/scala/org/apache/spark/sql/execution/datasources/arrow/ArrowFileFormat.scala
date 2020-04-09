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

package org.apache.spark.sql.execution.datasources.arrow;

import scala.collection.JavaConverters._

import org.apache.arrow.dataset.Dataset
import org.apache.arrow.dataset.jni.{NativeDataSource, NativeScanner}
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.arrow.ArrowFileFormat.UnsafeItr
import org.apache.spark.sql.execution.datasources.v2.arrow.{ArrowFilters, ArrowOptions, ArrowUtils}
import org.apache.spark.sql.execution.vectorized.{ArrowWritableColumnVector, ColumnVectorUtils, OnHeapColumnVector}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector};

class ArrowFileFormat extends FileFormat with DataSourceRegister with Serializable {

  def convert(files: Seq[FileStatus], options: Map[String, String]): Option[StructType] = {
    ArrowUtils.readSchema(files, new CaseInsensitiveStringMap(options.asJava))
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    convert(files, options)
  }

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported for Arrow source")
  }

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = true

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    (file: PartitionedFile) => {

      val sqlConf = sparkSession.sessionState.conf;
      val enableFilterPushDown: Boolean = sqlConf
        .getConfString("spark.sql.arrow.filterPushdown", "true").toBoolean
      val path = file.filePath
      val discovery = ArrowUtils.makeArrowDiscovery(
        path, new ArrowOptions(
          new CaseInsensitiveStringMap(
            options.asJava).asScala.toMap))
      val source = discovery.finish()

      // todo predicate validation / pushdown
      val dataset = new Dataset[NativeDataSource](List(source).asJava,
        discovery.inspect())

      val filter = if (enableFilterPushDown) {
        ArrowFilters.translateFilters(filters)
      } else {
        org.apache.arrow.dataset.filter.Filter.EMPTY
      }

      val scanner = new NativeScanner(
        dataset,
        new ScanOptions(requiredSchema.map(f => f.name).toArray,
          filter, 4096),
        org.apache.spark.sql.util.ArrowUtils.rootAllocator)
      val itrList = scanner
        .scan()
        .iterator()
        .asScala
        .map(task => task.scan())
        .toList

      val itr = itrList
        .toIterator
        .flatMap(itr => itr.asScala)
        .map(vsr => loadVsr(vsr, file.partitionValues, partitionSchema))
      new UnsafeItr(itr).asInstanceOf[Iterator[InternalRow]]
    }
  }

  // fixme code duplicating with ArrowPartitionReaderFactory
  def loadVsr(vsr: VectorSchemaRoot, partitionValues: InternalRow,
              partitionSchema: StructType): ColumnarBatch = {
    val fvs = vsr.getFieldVectors

    val rowCount = vsr.getRowCount
    val vectors = ArrowWritableColumnVector.loadColumns(rowCount, fvs)
    val partitionColumns = OnHeapColumnVector.allocateColumns(rowCount, partitionSchema)
    (0 until partitionColumns.length).foreach(i => {
      ColumnVectorUtils.populate(partitionColumns(i), partitionValues, i)
      partitionColumns(i).setIsConstant()
    })

    val batch = new ColumnarBatch(vectors ++ partitionColumns, rowCount, new Array[Long](5))
    batch.setNumRows(rowCount)
    batch
  }

  override def shortName(): String = "arrow"
}

object ArrowFileFormat {
  class UnsafeItr[T](delegate: Iterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = delegate.hasNext

    override def next(): T = delegate.next()
  }
}
