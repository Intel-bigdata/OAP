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

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.util.{ContextUtil, SerializationUtil}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.spinach.index.{IndexContext, ScannerBuilder}
import org.apache.spark.sql.execution.datasources.spinach.io._
import org.apache.spark.sql.execution.datasources.spinach.utils.SpinachUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
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

    // TODO we support partitions, but this only read meta from one of the partitions
    val partition2Meta = fileCatalog.allFiles().map(_.getPath.getParent).distinct.map { parent =>
      (parent, new Path(parent, SpinachFileFormat.SPINACH_META_FILE))
    }
      .find(pair => pair._2.getFileSystem(hadoopConf).exists(pair._2))
      .toMap
    meta = partition2Meta.values.headOption.map {
      DataSourceMeta.initialize(_, hadoopConf)
    }
    // SpinachFileFormat.serializeDataSourceMeta(hadoopConf, meta)
    inferSchema = meta.map(_.schema)

    this
  }

  // TODO inferSchema could be lazy computed
  var inferSchema: Option[StructType] = _
  var meta: Option[DataSourceMeta] = _

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
    // SpinachFileFormat.deserializeDataSourceMeta(hadoopConf) match {
    meta match {
      case Some(m) =>
        logDebug("Building SpinachDataReader with "
          + m.dataReaderClassName.substring(m.dataReaderClassName.lastIndexOf(".") + 1)
          + " ...")

        def canTriggerIndex(filter: Filter): Boolean = {
          var attr: String = null
          def checkAttribute(filter: Filter): Boolean = filter match {
              case Or(left, right) =>
                checkAttribute(left) && checkAttribute(right)
              case And(left, right) =>
                checkAttribute(left) && checkAttribute(right)
              case EqualTo(attribute, _) =>
                if (attr ==  null || attr == attribute) {attr = attribute; true} else false
              case LessThan(attribute, _) =>
                if (attr ==  null || attr == attribute) {attr = attribute; true} else false
              case LessThanOrEqual(attribute, _) =>
                if (attr ==  null || attr == attribute) {attr = attribute; true} else false
              case GreaterThan(attribute, _) =>
                if (attr ==  null || attr == attribute) {attr = attribute; true} else false
              case GreaterThanOrEqual(attribute, _) =>
                if (attr ==  null || attr == attribute) {attr = attribute; true} else false
              case _ => true
            }

          checkAttribute(filter)
        }

        val ic = new IndexContext(m)

        if (m.indexMetas.nonEmpty) { // check and use index
          logDebug("Supported Filters by Spinach:")
          // filter out the "filters" on which we can use the B+ tree index
          val supportFilters = filters.toArray.filter(canTriggerIndex)
          // After filtered, supportFilter only contains:
          // 1. Or predicate that contains only one attribute internally;
          // 2. Some atomic predicates, such as LessThan, EqualTo, etc.
          if (supportFilters.nonEmpty) {
            // determine whether we can use B+ tree index
            supportFilters.foreach(filter => logDebug("\t" + filter.toString))
            ScannerBuilder.build(supportFilters, ic)
          }
        }
//        val filterScanner = ic.getScannerBuilder.map(_.build)
        val filterScanner = ic.getScanner

        val requiredIds = requiredSchema.map(dataSchema.fields.indexOf(_)).toArray

        hadoopConf.setDouble(SQLConf.SPINACH_FULL_SCAN_THRESHOLD.key,
          sparkSession.conf.get(SQLConf.SPINACH_FULL_SCAN_THRESHOLD))
        val broadcastedHadoopConf =
          sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

        (file: PartitionedFile) => {
          assert(file.partitionValues.numFields == partitionSchema.size)

          val iter = new SpinachDataReader(
            new Path(new URI(file.filePath)), m, filterScanner, requiredIds)
            .initialize(broadcastedHadoopConf.value.value)

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

  def hasAvailableIndex(expressions: Seq[Expression]): Boolean = {
    meta match {
      case Some(m) =>
        val bTreeIndexAttrSet = new mutable.HashSet[String]()
        val bloomIndexAttrSet = new mutable.HashSet[String]()
        val bitmapIndexAttrSet = new mutable.HashSet[String]()
        var idx = 0
        while(idx < m.indexMetas.length) {
          m.indexMetas(idx).indexType match {
            case BTreeIndex(entries) =>
              bTreeIndexAttrSet.add(m.schema(entries(0).ordinal).name)
            case BloomFilterIndex(entries) =>
              entries.map(ordinal => m.schema(ordinal).name).foreach(bloomIndexAttrSet.add)
            case BitMapIndex(entries) =>
              entries.map(ordinal => m.schema(ordinal).name).foreach(bitmapIndexAttrSet.add)
            case _ => // we don't support other types of index
          }
          idx += 1
        }
        val hashSetList = new mutable.ListBuffer[mutable.HashSet[String]]()
        hashSetList.append(bTreeIndexAttrSet)
        hashSetList.append(bloomIndexAttrSet)
        hashSetList.append(bitmapIndexAttrSet)
        expressions.exists(m.isSupportedByIndex(_, hashSetList))
      case None => false
    }
  }
}

/**
 * Spinach Output Writer Factory
 * @param sqlConf
 * @param dataSchema
 * @param job
 * @param options
 */
private[spinach] class SpinachOutputWriterFactory(
    sqlConf: SQLConf,
    dataSchema: StructType,
    @transient protected val job: Job,
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

  def spnMetaFileExists(path: Path): Boolean = {
    val fs = path.getFileSystem(job.getConfiguration)
    fs.exists(new Path(path, SpinachFileFormat.SPINACH_META_FILE))
  }

  def addOldMetaToBuilder(path: Path, builder: DataSourceMetaBuilder): Unit = {
    if (spnMetaFileExists(path)) {
      val m = SpinachUtils.getMeta(job.getConfiguration, path)
      assert(m.nonEmpty)
      val oldMeta = m.get
      val existsIndexes = oldMeta.indexMetas
      val existsData = oldMeta.fileMetas
      if (existsData != null) existsData.foreach(builder.addFileMeta(_))
      if (existsIndexes != null) {
        existsIndexes.foreach(builder.addIndexMeta(_))
      }
      builder.withNewSchema(oldMeta.schema)
    } else {
      builder.withNewSchema(dataSchema)
    }
  }

  // this is called from driver side
  override def commitJob(taskResults: Array[WriteResult]): Unit = {
    // TODO supposedly, we put one single meta file for each partition, however,
    // we need to thinking about how to read data from partitions
    val outputRoot = FileOutputFormat.getOutputPath(job)
    val path = new Path(outputRoot, SpinachFileFormat.SPINACH_META_FILE)

    val builder = DataSourceMeta.newBuilder()
      .withNewDataReaderClassName(SpinachFileFormat.SPINACH_DATA_FILE_CLASSNAME)
    val conf = job.getConfiguration
    val partitionMeta = taskResults.map {
      // The file fingerprint is not used at the moment.
      case s: SpinachWriteResult =>
        builder.addFileMeta(FileMeta("", s.rowsWritten, s.fileName))
        (s.partitionString, (s.fileName, s.rowsWritten))
      case _ => throw new SpinachException("Unexpected Spinach write result.")
    }.groupBy(_._1)

    if (partitionMeta.nonEmpty && partitionMeta.head._1 != "") {
      partitionMeta.foreach(p => {
        // we should judge if exists old meta files
        // if exists we should load old meta info
        // and write that to new mete files
        val parent = new Path(outputRoot, p._1)
        val partBuilder = DataSourceMeta.newBuilder()

        addOldMetaToBuilder(parent, partBuilder)

        p._2.foreach(m => partBuilder.addFileMeta(FileMeta("", m._2._2, m._2._1)))
        val partMetaPath = new Path(parent, SpinachFileFormat.SPINACH_META_FILE)
        DataSourceMeta.write(partMetaPath, conf, partBuilder.build())
      })
    } else if (partitionMeta.nonEmpty) { // normal table file without partitions
      addOldMetaToBuilder(outputRoot, builder)
      DataSourceMeta.write(path, conf, builder.build())
    }

    super.commitJob(taskResults)
  }
}


private[spinach] case class SpinachWriteResult(
    fileName: String, rowsWritten: Int, partitionString: String)

private[spinach] class SpinachOutputWriter(
                                            path: String,
                                            dataSchema: StructType,
                                            context: TaskAttemptContext,
                                            sc: SerializableConfiguration) extends OutputWriter {
  private var rowCount = 0
  private var partitionString: String = ""
  override def setPartitionString(ps: String): Unit = {
    partitionString = ps
  }
  private val writer: SpinachDataWriter = {
    val isCompressed: Boolean = FileOutputFormat.getCompressOutput(context)
    val file: Path = new Path(path, getFileName(SpinachFileFormat.SPINACH_DATA_EXTENSION))
    val fs: FileSystem = file.getFileSystem(sc.value)
    val fileOut: FSDataOutputStream = fs.create(file, false)

    new SpinachDataWriter(isCompressed, fileOut, dataSchema, sc.value)
  }

  override def write(row: Row): Unit = throw new NotImplementedError("write(row: Row)")
  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    rowCount += 1
    writer.write(row)
  }

  override def close(): WriteResult = {
    writer.close()
    SpinachWriteResult(dataFileName, rowCount, partitionString)
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
  val SPINACH_DATA_FILE_CLASSNAME = classOf[SpinachDataFile].getCanonicalName
  val PARQUET_DATA_FILE_CLASSNAME = classOf[ParquetDataFile].getCanonicalName

  def serializeDataSourceMeta(conf: Configuration, meta: Option[DataSourceMeta]): Unit = {
    SerializationUtil.writeObjectToConfAsBase64(SPINACH_DATA_SOURCE_META, meta, conf)
  }

  def deserializeDataSourceMeta(conf: Configuration): Option[DataSourceMeta] = {
    SerializationUtil.readObjectFromConfAsBase64(SPINACH_DATA_SOURCE_META, conf)
  }
}
