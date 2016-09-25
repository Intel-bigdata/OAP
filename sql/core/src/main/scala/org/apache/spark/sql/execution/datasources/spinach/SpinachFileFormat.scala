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
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

private[sql] class SpinachFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  override def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]): Option[StructType] = {
    val metaPaths = files.filter { status =>
      status.getPath.getName.endsWith(SpinachFileFormat.SPINACH_META_FILE)
    }.toArray

    val meta: Option[DataSourceMeta] =
      SpinachFileFormat.inferDataSourceMeta(sparkSession.sparkContext.hadoopConfiguration, files)
    SpinachFileFormat.serializeDataSourceMeta(sparkSession.sparkContext.hadoopConfiguration, meta)
    meta.map(_.schema)
  }

  override def prepareWrite(
    sparkSession: SparkSession,
    job: Job, options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = {
    // TODO pass down something via job conf
    val conf = sparkSession.sqlContext.sessionState.newHadoopConf()

    new SpinachOutputWriterFactory(sparkSession.sqlContext.conf,
      dataSchema,
      conf,
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
          ).initialize(SpinachFileFormat.dummyTaskAttemptContext(broadcastedHadoopConf))

          val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
          val joinedRow = new JoinedRow()
          val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

          iter.asInstanceOf[Iterator[InternalRow]]
            .map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
        }
      case None => (_: PartitionedFile) => { Iterator.empty }
    }
  }
}

private[spinach] class SpinachOutputWriterFactory(
    sqlConf: SQLConf,
    dataSchema: StructType,
    hadoopConf: Configuration,
    options: Map[String, String]) extends OutputWriterFactory {
  private val serializableConf: SerializableConfiguration = {
    val job = Job.getInstance(hadoopConf)
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
}

private[spinach] class SpinachOutputWriter(
                                            path: String,
                                            dataSchema: StructType,
                                            context: TaskAttemptContext,
                                            sc: SerializableConfiguration) extends OutputWriter {
  private val writer = new FileOutputFormat[NullWritable, InternalRow] {
    override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
      new Path(path, getFileName(extension))
    }

    override def getRecordWriter(context: TaskAttemptContext)
    : RecordWriter[NullWritable, InternalRow] = {

      val isCompressed: Boolean = FileOutputFormat.getCompressOutput(context)

      val file: Path = getDefaultWorkFile(context, SpinachFileFormat.SPINACH_DATA_EXTENSION)
      val fs: FileSystem = file.getFileSystem(sc.value)
      val fileOut: FSDataOutputStream = fs.create(file, false)
      new SpinachDataWriter2(isCompressed, fileOut, dataSchema)
    }
  }.getRecordWriter(context)

  override def write(row: Row): Unit = throw new NotImplementedError("write(row: Row)")
  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    writer.write(NullWritable.get(), row)
  }
  override def close(): Unit = writer.close(context)

  def getFileName(extension: String): String = {
    val configuration = sc.value
    // this is the way how we pass down the uuid
    val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = context.getTaskAttemptID
    val split = taskAttemptId.getTaskID.getId
    f"part-r-$split%05d-${uniqueWriteJobId}$extension"
  }

  def getFileName(): String = getFileName(SpinachFileFormat.SPINACH_DATA_EXTENSION)
}

private[sql] object SpinachFileFormat {
  val SPINACH_DATA_EXTENSION = ".data"
  val SPINACH_INDEX_EXTENSION = ".index"
  val SPINACH_META_EXTENSION = ".meta"
  val SPINACH_META_FILE = "spinach.meta"
  val SPINACH_META_SCHEMA = "spinach.schema"
  val SPINACH_DATA_SOURCE_META = "spinach.meta.datasource"

  def serializeDataSourceMeta(conf: Configuration, meta: Option[DataSourceMeta]): Unit = {
    SerializationUtil.writeObjectToConfAsBase64(SPINACH_DATA_SOURCE_META, meta, conf)
  }

  def deserializeDataSourceMeta(conf: Configuration): Option[DataSourceMeta] = {
    Option(SerializationUtil.readObjectFromConfAsBase64(SPINACH_DATA_SOURCE_META, conf))
  }

  def inferDataSourceMeta(conf: Configuration, files: Seq[FileStatus]): Option[DataSourceMeta] = {
    val metaPaths = files.filter { status =>
      status.getPath.getName.endsWith(SpinachFileFormat.SPINACH_META_FILE)
    }.toArray

    if (metaPaths.isEmpty) {
      None
    } else {
      // TODO verify all of the schema from the meta data
      Some(DataSourceMeta.initialize(metaPaths(0).getPath, conf))
    }
  }

  def dummyTaskAttemptContext(hadoopConf: Broadcast[SerializableConfiguration])
  : TaskAttemptContext = {
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    new TaskAttemptContextImpl(hadoopConf.value.value, attemptId)
  }
}
