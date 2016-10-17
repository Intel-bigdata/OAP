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

package org.apache.spark.sql.execution.datasources.spinach

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.hadoop.util.{ContextUtil, SerializationUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

private[sql] class SpinachFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  override def initialize(
    sparkSession: SparkSession,
    options: Map[String, String],
    fileCatalog: FileCatalog): FileFormat = {
    super.initialize(sparkSession, options, fileCatalog)

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    // TODO
    // 1. Make the scanning etc. as lazy loading, as inferSchema probably not be called
    // 2. We need to pass down the spinach meta file and its associated partition path
    val partition2Meta = catalog.allFiles().map(_.getPath.getParent).map { parent =>
      (parent, new Path(parent, SpinachFileFormat.SPINACH_META_FILE))
    }
      .filter(pair => pair._2.getFileSystem(hadoopConf).exists(pair._2))
      .toMap

    // TODO we dont support partition for now
    val meta = partition2Meta.values.headOption.map {
      DataSourceMeta.initialize(_, hadoopConf)
    }
    SpinachFileFormat.serializeDataSourceMeta(hadoopConf, meta)
    inferSchema = meta.map(_.schema)

    this
  }

  // TODO inferSchema could be lazy computed
  var inferSchema: Option[StructType] = _

  override def prepareWrite(
    sparkSession: SparkSession,
    job: Job, options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = {
    // TODO pass down something via job conf
    val conf = job.getConfiguration

    new SpinachOutputWriterFactory(sparkSession.sqlContext.conf,
      dataSchema,
      job,
      options)
  }

  override def shortName(): String = "spn"

  /**
    * Returns whether the reader will return the rows as batch or not.
    */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    // TODO we should naturelly support batch
    false
  }

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = false

  override private[sql] def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    // For Parquet data source, `buildReader` already handles partition values appending. Here we
    // simply delegate to `buildReader`.
    buildReader(
      sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    // TODO we need to pass the extra data source meta information via the func parameter
    SpinachFileFormat.deserializeDataSourceMeta(hadoopConf) match {
      case Some(meta) =>
        val ic = new IndexContext(meta)
        BPlusTreeSearch.build(filters.toArray, ic)
        val filterScanner = ic.getScannerBuilder.map(_.build)
        val requiredIds = requiredSchema.map(dataSchema.fields.indexOf(_)).toArray

        val broadcastedHadoopConf =
          sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

        (file: PartitionedFile) => {
          assert(file.partitionValues.numFields == partitionSchema.size)

          val iter = new SpinachDataReader2(
            new Path(new URI(file.filePath)), dataSchema, filterScanner, requiredIds
          ).initialize(broadcastedHadoopConf.value.value)

          val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
          val joinedRow = new JoinedRow()
          val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

          iter.map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
        }
      case None => (_: PartitionedFile) => {
        // TODO need to think about when there is no spinach.meta file at all
        Iterator.empty
      }
    }
  }
}

private[spinach] class SpinachOutputWriterFactory(
    sqlConf: SQLConf,
    dataSchema: StructType,
    @transient job: Job,
    options: Map[String, String]) extends OutputWriterFactory {
  private val serializableConf: SerializableConfiguration = {
    val conf = ContextUtil.getConfiguration(job)

    new SerializableConfiguration(conf)
  }

  override def newInstance(
                            path: String, bucketId: Option[Int],
                            dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
    // TODO we don't support bucket yet
    assert(bucketId.isDefined == false, "Spinach doesn't support bucket yet.")
    new SpinachOutputWriter(path, dataSchema, context, serializableConf)
  }

  // this is called from driver side
  override def commitJob(taskResults: Array[WriteResult]): Unit = {
    // TODO supposedly, we put one single meta file for each partition, however,
    // we need to thinking about how to read data from partitions
    val outputRoot = FileOutputFormat.getOutputPath(job)
    val path = new Path(outputRoot, SpinachFileFormat.SPINACH_META_FILE)

    val builder = DataSourceMeta.newBuilder()
    taskResults.foreach {
      // The file fingerprint is not used at the moment.
      case s: SpinachWriteResult => builder.addFileMeta(FileMeta("", s.rowsWritten, s.fileName))
      case _ => throw new SpinachException("Unexpected Spinach write result.")
    }

    val spinachMeta = builder.withNewSchema(dataSchema).build()
    DataSourceMeta.write(path, job.getConfiguration, spinachMeta)

    super.commitJob(taskResults)
  }
}


private[spinach] case class SpinachWriteResult(fileName: String, rowsWritten: Int)

private[spinach] class SpinachOutputWriter(
                                            path: String,
                                            dataSchema: StructType,
                                            context: TaskAttemptContext,
                                            sc: SerializableConfiguration) extends OutputWriter {
  private var rowCount = 0
  private val writer: SpinachDataWriter2 = {
    val isCompressed: Boolean = FileOutputFormat.getCompressOutput(context)
    val file: Path = new Path(path, getFileName(SpinachFileFormat.SPINACH_DATA_EXTENSION))
    val fs: FileSystem = file.getFileSystem(sc.value)
    val fileOut: FSDataOutputStream = fs.create(file, false)

    new SpinachDataWriter2(isCompressed, fileOut, dataSchema)
  }

  override def write(row: Row): Unit = throw new NotImplementedError("write(row: Row)")
  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    rowCount += 1
    writer.write(row)
  }

  override def close(): WriteResult = {
    writer.close()
    SpinachWriteResult(dataFileName, rowCount)
  }

  private def getFileName(extension: String): String = {
    val configuration = sc.value
    // this is the way how we pass down the uuid
    val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = context.getTaskAttemptID
    val split = taskAttemptId.getTaskID.getId
    f"part-r-$split%05d-${uniqueWriteJobId}$extension"
  }

  def dataFileName: String = getFileName(SpinachFileFormat.SPINACH_DATA_EXTENSION)
}

private[sql] object SpinachFileFormat {
  val SPINACH_DATA_EXTENSION = ".data"
  val SPINACH_INDEX_EXTENSION = ".index"
  val SPINACH_META_EXTENSION = ".meta"
  val SPINACH_META_FILE = ".spinach.meta"
  val SPINACH_META_SCHEMA = "spinach.schema"
  val SPINACH_DATA_SOURCE_META = "spinach.meta.datasource"

  def serializeDataSourceMeta(conf: Configuration, meta: Option[DataSourceMeta]): Unit = {
    SerializationUtil.writeObjectToConfAsBase64(SPINACH_DATA_SOURCE_META, meta, conf)
  }

  def deserializeDataSourceMeta(conf: Configuration): Option[DataSourceMeta] = {
    SerializationUtil.readObjectFromConfAsBase64(SPINACH_DATA_SOURCE_META, conf)
  }
}
