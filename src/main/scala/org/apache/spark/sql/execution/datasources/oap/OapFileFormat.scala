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
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.StringUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.oap.index.{IndexContext, ScannerBuilder}
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileProperties.DataFileVersion
import org.apache.spark.sql.execution.datasources.oap.utils.{FilterHelper, OapUtils}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.util.SerializableConfiguration

private[sql] class OapFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  // exposed for test
  private[oap] lazy val oapMetrics = OapRuntime.getOrCreate.oapMetricsManager

  private var initialized = false
  @transient protected var options: Map[String, String] = _
  @transient protected var sparkSession: SparkSession = _
  @transient protected var files: Seq[FileStatus] = _

  def init(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): FileFormat = {
    this.sparkSession = sparkSession
    this.options = options
    this.files = files

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    // TODO
    // 1. Make the scanning etc. as lazy loading, as inferSchema probably not be called
    // 2. We need to pass down the oap meta file and its associated partition path

    val parents = files.map(file => file.getPath.getParent)

    // TODO we support partitions, but this only read meta from one of the partitions
    val partition2Meta = parents.distinct.reverse.map { parent =>
      new Path(parent, OapFileFormat.OAP_META_FILE)
    }.find(metaPath => metaPath.getFileSystem(hadoopConf).exists(metaPath))

    meta = partition2Meta.map {
      DataSourceMeta.initialize(_, hadoopConf)
    }

    // OapFileFormat.serializeDataSourceMeta(hadoopConf, meta)
    inferSchema = meta.map(_.schema)
    initialized = true

    this
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    if (!initialized) {
      init(sparkSession, options, files)
    }
    inferSchema
  }

  // TODO inferSchema could be lazy computed
  var inferSchema: Option[StructType] = _
  var meta: Option[DataSourceMeta] = _
  // map of columns->IndexType
  private var hitIndexColumns: Map[String, IndexType] = _

  def initMetrics(metrics: Map[String, SQLMetric]): Unit = oapMetrics.initMetrics(metrics)

  def getHitIndexColumns: Map[String, IndexType] = {
    if (this.hitIndexColumns == null) {
      logWarning("Trigger buildReaderWithPartitionValues before getHitIndexColumns")
      Map.empty
    } else {
      this.hitIndexColumns
    }
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job, options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration

    // First use table option, if not, use SqlConf, else, use default value.
    conf.set(OapFileFormat.COMPRESSION, options.getOrElse("compression",
      sparkSession.conf.get(OapConf.OAP_COMPRESSION.key, OapFileFormat.DEFAULT_COMPRESSION)))

    conf.set(OapFileFormat.ROW_GROUP_SIZE, options.getOrElse("rowgroup",
      sparkSession.conf.get(OapConf.OAP_ROW_GROUP_SIZE.key, OapFileFormat.DEFAULT_ROW_GROUP_SIZE)))

    new OapOutputWriterFactory(
      dataSchema,
      job,
      options)
  }

  override def shortName(): String = "oap"

  /**
   * Returns whether the reader will return the rows as batch or not.
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    // TODO remove readerClassName after oap support batch return
    val readerClassName = meta match {
      case Some(m) =>
        m.dataReaderClassName
      case _ => ""
    }
    val conf = sparkSession.sessionState.conf
    // TODO modify conditions after oap support batch return
    readerClassName.equals(OapFileFormat.PARQUET_DATA_FILE_CLASSNAME) &&
      conf.parquetVectorizedReaderEnabled &&
      conf.wholeStageEnabled &&
      schema.length <= conf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType]) &&
      !sparkSession.conf.get(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED)
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    // TODO we need to pass the extra data source meta information via the func parameter
    meta match {
      case Some(m) =>
        logDebug("Building OapDataReader with "
          + m.dataReaderClassName.substring(m.dataReaderClassName.lastIndexOf(".") + 1)
          + " ...")

        // Check whether this filter conforms to certain patterns that could benefit from index
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
            case In(attribute, _) =>
              if (attr ==  null || attr == attribute) {attr = attribute; true} else false
            case IsNull(attribute) =>
              if (attr ==  null || attr == attribute) {attr = attribute; true} else false
            case IsNotNull(attribute) =>
              if (attr ==  null || attr == attribute) {attr = attribute; true} else false
            case StringStartsWith(attribute, _) =>
              if (attr ==  null || attr == attribute) {attr = attribute; true} else false
            case _ => false
          }

          checkAttribute(filter)
        }

        val ic = new IndexContext(m)

        if (m.indexMetas.nonEmpty) { // check and use index
          logDebug("Supported Filters by Oap:")
          // filter out the "filters" on which we can use index
          val supportFilters = filters.toArray.filter(canTriggerIndex)
          // After filtered, supportFilter only contains:
          // 1. Or predicate that contains only one attribute internally;
          // 2. Some atomic predicates, such as LessThan, EqualTo, etc.
          if (supportFilters.nonEmpty) {
            // determine whether we can use index
            supportFilters.foreach(filter => logDebug("\t" + filter.toString))
            // get index options such as limit, order, etc.
            val indexOptions = options.filterKeys(OapFileFormat.oapOptimizationKeySeq.contains(_))
            val maxChooseSize = sparkSession.conf.get(OapConf.OAP_INDEXER_CHOICE_MAX_SIZE)
            val indexDisableList = sparkSession.conf.get(OapConf.OAP_INDEX_DISABLE_LIST)
            ScannerBuilder.build(supportFilters, ic, indexOptions, maxChooseSize, indexDisableList)
          }
        }

        val filterScanners = ic.getScanners
        hitIndexColumns = filterScanners match {
          case Some(s) =>
            s.scanners.flatMap { scanner =>
              scanner.keyNames.map( n => n -> scanner.meta.indexType)
            }.toMap
          case _ => Map.empty
        }

        val requiredIds = requiredSchema.map(dataSchema.fields.indexOf(_)).toArray
        val pushed = FilterHelper.tryToPushFilters(sparkSession, requiredSchema, filters)

        // Refer to ParquetFileFormat, use resultSchema to decide if this query support
        // Vectorized Read and returningBatch. Also it depends on WHOLE_STAGE_CODE_GEN,
        // as the essential unsafe projection is done by that.
        val isParquet = m.dataReaderClassName.equals(OapFileFormat.PARQUET_DATA_FILE_CLASSNAME)
        val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
        val enableVectorizedReader: Boolean =
          m.dataReaderClassName.equals(OapFileFormat.PARQUET_DATA_FILE_CLASSNAME) &&
          sparkSession.sessionState.conf.parquetVectorizedReaderEnabled &&
          sparkSession.sessionState.conf.wholeStageEnabled &&
          resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
        val returningBatch = supportBatch(sparkSession, resultSchema)
        val parquetDataCacheEnable = sparkSession.conf.get(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED)
        val broadcastedHadoopConf =
          sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

        (file: PartitionedFile) => {
          assert(file.partitionValues.numFields == partitionSchema.size)
          val conf = broadcastedHadoopConf.value.value

          // if enableVectorizedReader == true, init VectorizedContext,
          // else context is None.
          val context = if (enableVectorizedReader) {
            Some(VectorizedContext(partitionSchema, file.partitionValues, returningBatch))
          } else {
            None
          }

          val path = new Path(StringUtils.unEscapeString(file.filePath))
          val fs = path.getFileSystem(broadcastedHadoopConf.value.value)

          val version = if (isParquet) {
            // Currently Parquet is using OapDataReaderV1
            DataFileVersion.OAP_DATAFILE_V1
          } else {
            OapDataReader.readVersion(fs.open(path), fs.getFileStatus(path).getLen)
          }

          version match {
            case DataFileVersion.OAP_DATAFILE_V1 =>
              val reader = new OapDataReaderV1(file.filePath, m, partitionSchema, requiredSchema,
                filterScanners, requiredIds, pushed, oapMetrics, conf, enableVectorizedReader,
                parquetDataCacheEnable, options, filters, context = context)
              reader.read(file)
            // Actually it shouldn't get to this line, because unsupported version will cause
            // exception thrown in readVersion call
            case _ =>
              throw new OapException("Unexpected data file version")
              Iterator.empty
          }

        }
      case None => (_: PartitionedFile) => {
        // TODO need to think about when there is no oap.meta file at all
        Iterator.empty
      }
    }
  }

  /**
   * Check if index satisfies strategies' requirements.
   *
   * @param expressions: Index expressions.
   * @param requiredTypes: Required index metrics by optimization strategies.
   * @return
   */
  def hasAvailableIndex(
      expressions: Seq[Expression],
      requiredTypes: Seq[IndexType] = Nil): Boolean = {
    if (expressions.nonEmpty && sparkSession.conf.get(OapConf.OAP_ENABLE_OINDEX)) {
      meta match {
        case Some(m) if requiredTypes.isEmpty =>
          expressions.exists(m.isSupportedByIndex(_, None))
        case Some(m) if requiredTypes.length == expressions.length =>
          expressions.zip(requiredTypes).exists{ x =>
            val expression = x._1
            val requirement = Some(x._2)
            m.isSupportedByIndex(expression, requirement)
          }
        case _ => false
      }
    } else {
      false
    }
  }
}

private[oap] object INDEX_STAT extends Enumeration {
  type INDEX_STAT = Value
  val MISS_INDEX, HIT_INDEX, IGNORE_INDEX = Value
}

/**
 * Oap Output Writer Factory
 * @param dataSchema
 * @param job
 * @param options
 */
private[oap] class OapOutputWriterFactory(
    dataSchema: StructType,
    @transient protected val job: Job,
    options: Map[String, String]) extends OutputWriterFactory {

  override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    new OapOutputWriter(path, dataSchema, context)
  }

  override def getFileExtension(context: TaskAttemptContext): String = {

    val extensionMap = Map(
      "UNCOMPRESSED" -> "",
      "SNAPPY" -> ".snappy",
      "GZIP" -> ".gzip",
      "LZO" -> ".lzo")

    val compressionType =
      context.getConfiguration.get(
        OapFileFormat.COMPRESSION, OapFileFormat.DEFAULT_COMPRESSION).trim.toUpperCase()

    extensionMap(compressionType) + OapFileFormat.OAP_DATA_EXTENSION
  }

  private def oapMetaFileExists(path: Path): Boolean = {
    val fs = path.getFileSystem(job.getConfiguration)
    fs.exists(new Path(path, OapFileFormat.OAP_META_FILE))
  }

  def addOldMetaToBuilder(path: Path, builder: DataSourceMetaBuilder): Unit = {
    if (oapMetaFileExists(path)) {
      val m = OapUtils.getMeta(job.getConfiguration, path)
      assert(m.nonEmpty)
      val oldMeta = m.get
      val existsIndexes = oldMeta.indexMetas
      val existsData = oldMeta.fileMetas
      if (existsData != null) {
        existsData.foreach(builder.addFileMeta(_))
      }
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
    val path = new Path(outputRoot, OapFileFormat.OAP_META_FILE)

    val builder = DataSourceMeta.newBuilder()
      .withNewDataReaderClassName(OapFileFormat.OAP_DATA_FILE_CLASSNAME)
    val conf = job.getConfiguration
    val partitionMeta = taskResults.map {
      // The file fingerprint is not used at the moment.
      case s: OapWriteResult =>
        builder.addFileMeta(FileMeta("", s.rowsWritten, s.fileName))
        (s.partitionString, (s.fileName, s.rowsWritten))
      case _ => throw new OapException("Unexpected Oap write result.")
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
        val partMetaPath = new Path(parent, OapFileFormat.OAP_META_FILE)
        DataSourceMeta.write(partMetaPath, conf, partBuilder.build())
      })
    } else if (partitionMeta.nonEmpty) { // normal table file without partitions
      addOldMetaToBuilder(outputRoot, builder)
      DataSourceMeta.write(path, conf, builder.build())
    }

    super.commitJob(taskResults)
  }
}


private[oap] case class OapWriteResult(
    fileName: String,
    rowsWritten: Int,
    partitionString: String)

private[oap] class OapOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext) extends OutputWriter {
  private var rowCount = 0
  private var partitionString: String = ""

  override def setPartitionString(ps: String): Unit = {
    partitionString = ps
  }

  private val writer: OapDataWriter = {
    val isCompressed = FileOutputFormat.getCompressOutput(context)
    val conf = context.getConfiguration
    val file: Path = new Path(path)
    val fs = file.getFileSystem(conf)
    val fileOut = fs.create(file, false)

    new OapDataWriter(isCompressed, fileOut, dataSchema, conf)
  }

  override def write(row: Row): Unit = throw new NotImplementedError("write(row: Row)")

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    rowCount += 1
    writer.write(row)
  }

  override def close(): Unit = {
    writer.close()
  }

  override def writeStatus(): WriteResult = {
    OapWriteResult(dataFileName, rowCount, partitionString)
  }

  def dataFileName: String = new Path(path).getName
}

private[sql] object OapFileFormat {
  val OAP_DATA_EXTENSION = ".data"
  val OAP_INDEX_EXTENSION = ".index"
  val OAP_META_FILE = ".oap.meta"
  // This is used in DataSourceMeta file to indicate Parquet/OAP file format
  // For OAP data files, the version info is written in data file header, hence various versions of
  // OAP data file(i.e. one partition uses V1 while another uses V2) are supported in different
  // distributed tasks
  val OAP_DATA_FILE_CLASSNAME = classOf[OapDataFile].getCanonicalName
  // This is used while actually reading a Parquet/OAP data file
  val OAP_DATA_FILE_V1_CLASSNAME = classOf[OapDataFileV1].getCanonicalName

  val PARQUET_DATA_FILE_CLASSNAME = classOf[ParquetDataFile].getCanonicalName

  val COMPRESSION = "oap.compression"
  val DEFAULT_COMPRESSION = OapConf.OAP_COMPRESSION.defaultValueString
  val ROW_GROUP_SIZE = "oap.rowgroup.size"
  val DEFAULT_ROW_GROUP_SIZE = OapConf.OAP_ROW_GROUP_SIZE.defaultValueString

  /**
   * Oap Optimization Options.
   */
  val OAP_QUERY_ORDER_OPTION_KEY = "oap.scan.file.order"
  val OAP_QUERY_LIMIT_OPTION_KEY = "oap.scan.file.limit"
  val OAP_INDEX_SCAN_NUM_OPTION_KEY = "oap.scan.index.limit"
  val OAP_INDEX_GROUP_BY_OPTION_KEY = "oap.scan.index.group"

  val oapOptimizationKeySeq : Seq[String] = {
    OAP_QUERY_ORDER_OPTION_KEY ::
    OAP_QUERY_LIMIT_OPTION_KEY ::
    OAP_INDEX_SCAN_NUM_OPTION_KEY ::
    OAP_INDEX_GROUP_BY_OPTION_KEY :: Nil
  }
}
