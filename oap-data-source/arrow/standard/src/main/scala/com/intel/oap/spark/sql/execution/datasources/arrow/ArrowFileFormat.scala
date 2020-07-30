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

package com.intel.oap.spark.sql.execution.datasources.arrow

import java.net.URLDecoder

import scala.collection.JavaConverters._

import com.intel.oap.spark.sql.execution.datasources.arrow.ArrowFileFormat.UnsafeItr
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.{ArrowFilters, ArrowOptions, ExecutionMemoryAllocationListener}
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowSQLConf._
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.arrow.ArrowUtils
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch;

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
    val sqlConf = sparkSession.sessionState.conf;
    val batchSize = sqlConf.parquetVectorizedReaderBatchSize
    val enableFilterPushDown = sqlConf.arrowFilterPushDown

    (file: PartitionedFile) => {
      val taskMemoryManager = ArrowUtils.getTaskMemoryManager()
      val listener = new ExecutionMemoryAllocationListener(taskMemoryManager)
      val factory = ArrowUtils.makeArrowDiscovery(
        URLDecoder.decode(file.filePath, "UTF-8"), new ArrowOptions(
          new CaseInsensitiveStringMap(
            options.asJava).asScala.toMap),
        listener)

      // todo predicate validation / pushdown
      val dataset = factory.finish();

      val filter = if (enableFilterPushDown) {
        ArrowFilters.translateFilters(filters)
      } else {
        org.apache.arrow.dataset.filter.Filter.EMPTY
      }

      val scanOptions = new ScanOptions(requiredSchema.map(f => f.name).toArray,
        filter, batchSize)
      val scanner = dataset.newScan(scanOptions)

      val taskList = scanner
          .scan()
          .iterator()
          .asScala
          .toList
      val itrList = taskList
        .map(task => task.scan())

      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => {
        itrList.foreach(_.close())
        taskList.foreach(_.close())
        scanner.close()
        dataset.close()
        factory.close()
      }))

      val itr = itrList
        .toIterator
        .flatMap(itr => itr.asScala)
        .map(vsr => ArrowUtils.loadVectors(vsr, file.partitionValues, partitionSchema,
          requiredSchema))
      new UnsafeItr(itr).asInstanceOf[Iterator[InternalRow]]
    }
  }

  override def shortName(): String = "arrow"
}

object ArrowFileFormat {
  class UnsafeItr[T](delegate: Iterator[ColumnarBatch])
    extends Iterator[ColumnarBatch] {
    val holder = new ColumnarBatchRetainer()

    override def hasNext: Boolean = {
      holder.release()
      val hasNext = delegate.hasNext
      hasNext
    }

    override def next(): ColumnarBatch = {
      val b = delegate.next()
      holder.retain(b)
      b
    }
  }

  class ColumnarBatchRetainer {
    private var retained: Option[ColumnarBatch] = None

    def retain(batch: ColumnarBatch): Unit = {
      if (retained.isDefined) {
        throw new IllegalStateException
      }
      retained = Some(batch)
    }

    def release(): Unit = {
      retained.foreach(b => b.close())
      retained = None
    }
  }
}
