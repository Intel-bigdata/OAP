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

package org.apache.spark.sql.execution.datasources.oap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.StringUtils
import org.apache.orc.OrcFile
import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.oap.io.{DataFileContext, OapDataReaderV1, OrcDataFileContext}
import org.apache.spark.sql.execution.datasources.orc.{OrcFiltersAdapter, OrcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.util.SerializableConfiguration

private[sql] class OptimizedOrcFileFormat extends OapFileFormat {

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory =
    throw new UnsupportedOperationException("OptimizedOrcFileFormat " +
      "only support read operation")

  /**
   * Returns whether the reader will return the rows as batch or not.
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    conf.getConf(SQLConf.ORC_VECTORIZED_READER_ENABLED) &&
      conf.wholeStageEnabled &&
      schema.length <= conf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def buildReaderWithPartitionValues(
     sparkSession: SparkSession,
     dataSchema: StructType,
     partitionSchema: StructType,
     requiredSchema: StructType,
     filters: Seq[Filter],
     options: Map[String, String],
     hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    // TODO we need to pass the extra data source meta information via the func parameter
    val (filterScanners, m) = meta match {
      case Some(x) =>
        (indexScanners(x, filters), x)
      case _ =>
        // TODO Now we need use a meta with ORC_DATA_FILE_CLASSNAME & dataSchema to init
        // OrcDataFile, try to remove this condition.
        val emptyMeta = new DataSourceMetaBuilder()
          .withNewDataReaderClassName(OapFileFormat.ORC_DATA_FILE_CLASSNAME)
          .withNewSchema(dataSchema).build()
        (None, emptyMeta)
    }
    // TODO refactor this.
    hitIndexColumns = filterScanners match {
      case Some(s) =>
        s.scanners.flatMap { scanner =>
          scanner.keyNames.map(n => n -> scanner.meta.indexType)
        }.toMap
      case _ => Map.empty
    }

    val requiredIds = requiredSchema.map(dataSchema.fields.indexOf(_)).toArray

    val enableOffHeapColumnVector =
      sparkSession.sessionState.conf.getConf(SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED)
    val copyToSpark =
      sparkSession.sessionState.conf.getConf(SQLConf.ORC_COPY_BATCH_TO_SPARK)
    val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

    // Push down the filters to the orc record reader.
    if (sparkSession.sessionState.conf.orcFilterPushDown) {
      OrcFiltersAdapter.createFilter(dataSchema,
        filters).foreach { f =>
        OrcInputFormat.setSearchArgument(hadoopConf, f, dataSchema.fieldNames)
      }
    }

    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val returningBatch = supportBatch(sparkSession, resultSchema)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)
      val conf = broadcastedHadoopConf.value.value

      val path = new Path(StringUtils.unEscapeString(file.filePath))
      val fs = path.getFileSystem(broadcastedHadoopConf.value.value)

      var orcWithEmptyColIds = false
      // For Orc, the context is used by both vectorized readers and map reduce readers.
      // See the comments in DataFile.scala.
      val context: Option[DataFileContext] = {
        val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
        val reader = OrcFile.createReader(path, readerOptions)
        val requestedColIdsOrEmptyFile = OrcUtils.requestedColumnIds(
          isCaseSensitive, dataSchema, requiredSchema, reader, conf)
        if (requestedColIdsOrEmptyFile.isDefined) {
          val requestedColIds = requestedColIdsOrEmptyFile.get
          assert(requestedColIds.length == requiredSchema.length,
            "[BUG] requested column IDs do not match required schema")
          Some(OrcDataFileContext(partitionSchema, file.partitionValues,
            returningBatch, requiredSchema, dataSchema, enableOffHeapColumnVector,
            copyToSpark, requestedColIds))
        } else {
          orcWithEmptyColIds = true
          None
        }
      }

      if (orcWithEmptyColIds) {
        // For the case of Orc with empty required column Ids.
        Iterator.empty
      } else {
        val reader = new OapDataReaderV1(file.filePath, m, partitionSchema, requiredSchema,
          filterScanners, requiredIds, None, oapMetrics, conf, returningBatch, options,
          filters, context)
        reader.read(file)
      }
    }
  }
}
