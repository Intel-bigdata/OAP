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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.filter2.predicate.FilterPredicate

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{OapException, PartitionedFile}
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache.DataFileMetaCacheManager
import org.apache.spark.sql.execution.datasources.oap.index.{IndexContext, IndexScanners, ScannerBuilder}
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileProperties.DataFileVersion
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileProperties.DataFileVersion.DataFileVersion
import org.apache.spark.sql.execution.datasources.oap.io.meta.OapDataFileMetaV1
import org.apache.spark.sql.execution.datasources.oap.utils.FilterHelper
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

abstract class OapDataReader {
  def read: PartitionedFile => Iterator[InternalRow]
}

class OapDataReaderV1(
    m: DataSourceMeta,
    filterScanners: Option[IndexScanners],
    requiredIds: Array[Int],
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    broadcastedHadoopConf: Broadcast[SerializableConfiguration],
    pushed: Option[FilterPredicate],
    enableVectorizedReader: Boolean,
    returningBatch: Boolean,
    parquetDataCacheEnable: Boolean,
    oapMetrics: OapMetricsManager) extends OapDataReader {

  override def read: (PartitionedFile) => Iterator[InternalRow] = {

    (file: PartitionedFile) => {

      assert(file.partitionValues.numFields == partitionSchema.size)
      val conf = broadcastedHadoopConf.value.value

      def isSkippedByFile: Boolean = {
        if (m.dataReaderClassName == OapFileFormat.OAP_DATA_FILE_V1_CLASSNAME) {
          val dataFile = DataFile(file.filePath, m.schema, m.dataReaderClassName, conf)
          val dataFileMeta = DataFileMetaCacheManager(dataFile)
              .asInstanceOf[OapDataFileMetaV1]
          if (filters.exists(filter => isSkippedByStatistics(
            dataFileMeta.columnsMeta.map(_.fileStatistics).toArray, filter, m.schema))) {
            val totalRows = dataFileMeta.totalRowCount()
            oapMetrics.updateTotalRows(totalRows)
            oapMetrics.skipForStatistic(totalRows)
            return true
          }
        }
        false
      }

      if (isSkippedByFile) {
        Iterator.empty
      } else {
        OapIndexInfo.partitionOapIndex.put(file.filePath, false)
        FilterHelper.setFilterIfExist(conf, pushed)
        // if enableVectorizedReader == true, init VectorizedContext,
        // else context is None.
        val context = if (enableVectorizedReader) {
          Some(VectorizedContext(partitionSchema, file.partitionValues, returningBatch))
        } else {
          None
        }
        val reader = new OapDataScannerV1(new Path(
          new URI(file.filePath)), m, filterScanners, requiredIds, context)
        val iter = reader.initialize(conf, options, filters)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))
        val totalRows = reader.totalRows()
        oapMetrics.updateTotalRows(totalRows)
        oapMetrics.updateIndexAndRowRead(reader, totalRows)
        // if enableVectorizedReader == true and parquetDataCacheEnable = false,
        // return iter directly because of partitionValues
        // already filled by VectorizedReader, else use original branch.
        if (enableVectorizedReader && !parquetDataCacheEnable) {
          iter
        } else {
          val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
          val joinedRow = new JoinedRow()
          val appendPartitionColumns =
            GenerateUnsafeProjection.generate(fullSchema, fullSchema)

          iter.map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
        }
      }
    }
  }
}

object OapDataReader extends Logging {

  def read(m: DataSourceMeta,
      sparkSession: SparkSession,
      filterScanners: Option[IndexScanners],
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      supportBatch: (SparkSession, StructType) => Boolean,
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val requiredIds = requiredSchema.map(dataSchema.fields.indexOf(_)).toArray
    val pushed = FilterHelper.tryToPushFilters(sparkSession, requiredSchema, filters)

    // Refer to ParquetFileFormat, use resultSchema to decide if this query support
    // Vectorized Read and returningBatch. Also it depends on WHOLE_STAGE_CODE_GEN,
    // as the essential unsafe projection is done by that.
    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val isParquet = m.dataReaderClassName.equals(OapFileFormat.PARQUET_DATA_FILE_CLASSNAME)
    val enableVectorizedReader: Boolean =
      isParquet && sparkSession.sessionState.conf.parquetVectorizedReaderEnabled &&
        sparkSession.sessionState.conf.wholeStageEnabled &&
          resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
    val returningBatch = supportBatch(sparkSession, resultSchema)
    val parquetDataCacheEnable = sparkSession.conf.get(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val oapMetrics = SparkEnv.get.oapManager.metricsManager

    (file: PartitionedFile) => {
      val path = new Path(StringUtils.unEscapeString(file.filePath))
      val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
      val version = if (isParquet) {
        DataFileVersion.OAP_DATAFILE_V1
      } else {
        readVersion(fs.open(path), fs.getFileStatus(path).getLen)
      }
      version match {
        case DataFileVersion.OAP_DATAFILE_V1 => new OapDataReaderV1(m, filterScanners, requiredIds,
          partitionSchema, requiredSchema, filters, options, broadcastedHadoopConf, pushed,
            enableVectorizedReader, returningBatch, parquetDataCacheEnable, oapMetrics).read(file)
        // Actually it won't get to this line, because unsupported version will cause exception
        // thrown in readVersion call
        case _ => Iterator.empty
      }
    }
  }

  private def readVersion(is: FSDataInputStream, fileLen: Long): DataFileVersion = {
    val MAGIC_VERSION_LENGTH = 4
    val metaEnd = fileLen - 4

    // seek to the position of data file meta length
    is.seek(metaEnd)
    val metaLength = is.readInt()
    // read all bytes of data file meta
    val magicBuffer = new Array[Byte](MAGIC_VERSION_LENGTH)
    is.readFully(metaEnd - metaLength, magicBuffer)

    val magic = UTF8String.fromBytes(magicBuffer).toString
    if (! magic.contains("OAP")) {
      throw new OapException("Not a valid Oap Data File")
    } else if (magic == "OAP1") {
      DataFileVersion.OAP_DATAFILE_V1
    } else {
      throw new OapException("Not a supported Oap Data File version")
    }
  }
}
