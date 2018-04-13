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

import java.net.URI

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.util.SerializationUtil

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateOrdering, GenerateUnsafeProjection}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.oap.filecache.DataFileHandleCacheManager
import org.apache.spark.sql.execution.datasources.oap.index.{IndexContext, ScannerBuilder}
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.{FilterHelper, OapUtils}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{AtomicType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

private[sql] class OapFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  val oapMetrics = new OapMetrics

  override def initialize(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): FileFormat = {
    super.initialize(sparkSession, options, files)

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

    this
  }

  // TODO inferSchema could be lazy computed
  var inferSchema: Option[StructType] = _
  var meta: Option[DataSourceMeta] = _
  // map of columns->IndexType
  private var hitIndexColumns: Map[String, IndexType] = _

  def initMetrics(metrics: Map[String, SQLMetric]): Unit =
    oapMetrics.initMetrics(metrics)

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

    // TODO: Should we have our own config util instead of SqlConf?
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

        def order(sf: StructField): Ordering[Key] = GenerateOrdering.create(StructType(Array(sf)))
        def canSkipFile(
            columnStats: ArrayBuffer[ColumnStatistics],
            filter: Filter,
            schema: StructType): Boolean = filter match {
          case Or(left, right) =>
            canSkipFile(columnStats, left, schema) && canSkipFile(columnStats, right, schema)
          case And(left, right) =>
            canSkipFile(columnStats, left, schema) || canSkipFile(columnStats, right, schema)
          case IsNotNull(attribute) =>
            val idx = schema.fieldIndex(attribute)
            val stat = columnStats(idx)
            !stat.hasNonNullValue
          case EqualTo(attribute, handle) =>
            val key = OapUtils.keyFromAny(handle)
            val idx = schema.fieldIndex(attribute)
            val stat = columnStats(idx)
            val comp = order(schema(idx))
            (OapUtils.keyFromBytes(stat.min, schema(idx).dataType), OapUtils.keyFromBytes(
              stat.max, schema(idx).dataType)) match {
              case (Some(v1), Some(v2)) => comp.gt(v1, key) || comp.lt(v2, key)
              case _ => false
            }
          case LessThan(attribute, handle) =>
            val key = OapUtils.keyFromAny(handle)
            val idx = schema.fieldIndex(attribute)
            val stat = columnStats(idx)
            val comp = order(schema(idx))
            OapUtils.keyFromBytes(stat.min, schema(idx).dataType) match {
              case Some(v) => comp.gteq(v, key)
              case None => false
            }
          case LessThanOrEqual(attribute, handle) =>
            val key = OapUtils.keyFromAny(handle)
            val idx = schema.fieldIndex(attribute)
            val stat = columnStats(idx)
            val comp = order(schema(idx))
            OapUtils.keyFromBytes(stat.min, schema(idx).dataType) match {
              case Some(v) => comp.gt(v, key)
              case None => false
            }
          case GreaterThan(attribute, handle) =>
            val key = OapUtils.keyFromAny(handle)
            val idx = schema.fieldIndex(attribute)
            val stat = columnStats(idx)
            val comp = order(schema(idx))
            OapUtils.keyFromBytes(stat.max, schema(idx).dataType) match {
              case Some(v) => comp.lteq(v, key)
              case None => false
            }
          case GreaterThanOrEqual(attribute, handle) =>
            val key = OapUtils.keyFromAny(handle)
            val idx = schema.fieldIndex(attribute)
            val stat = columnStats(idx)
            val comp = order(schema(idx))
            OapUtils.keyFromBytes(stat.max, schema(idx).dataType) match {
              case Some(v) => comp.lt(v, key)
              case None => false
            }
          case _ => false
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

        // refer to ParquetFileFormat, use resultSchema to decide if this query support
        // Vectorized Read and returningBatch.
        val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
        val enableVectorizedReader: Boolean =
          m.dataReaderClassName.equals(OapFileFormat.PARQUET_DATA_FILE_CLASSNAME) &&
          sparkSession.sessionState.conf.parquetVectorizedReaderEnabled &&
          resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
        val returningBatch = supportBatch(sparkSession, resultSchema)
        val parquetDataCacheEnable = sparkSession.conf.get(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED)
        val broadcastedHadoopConf =
          sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

        (file: PartitionedFile) => {
          assert(file.partitionValues.numFields == partitionSchema.size)
          val conf = broadcastedHadoopConf.value.value

          def canSkipByDataFileStatistics: Boolean = {
            if (m.dataReaderClassName == OapFileFormat.OAP_DATA_FILE_CLASSNAME) {
              val dataFile = DataFile(file.filePath, m.schema, m.dataReaderClassName, conf)
              val dataFileHandle: OapDataFileHandle = DataFileHandleCacheManager(dataFile)
              if (filters.exists(filter =>
                canSkipFile(dataFileHandle.columnsMeta.map(_.statistics), filter, m.schema))) {
                val totalRows = dataFileHandle.totalRowCount()
                oapMetrics.updateTotalRows(totalRows)
                oapMetrics.skipForStatistic(totalRows)
                return true
              }
            }
            false
          }

          if (canSkipByDataFileStatistics) {
            Iterator.empty
          } else {
            OapIndexInfo.partitionOapIndex.put(file.filePath, false)
            FilterHelper.setFilterIfExist(conf, pushed)
            // if enableVectorizedReader == true, init VectorizedContext,
            // else context is None.
            val context = if (enableVectorizedReader) {
              Some(VectorizedContext(partitionSchema,
                file.partitionValues, returningBatch))
            } else {
              None
            }
            val reader = new OapDataReader(
              new Path(new URI(file.filePath)), m, filterScanners, requiredIds, context)
            val iter = reader.initialize(conf, options)
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
      context.getConfiguration
          .get(OapFileFormat.COMPRESSION, OapFileFormat.DEFAULT_COMPRESSION).trim

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

  override def close(): WriteResult = {
    writer.close()
    OapWriteResult(dataFileName, rowCount, partitionString)
  }

  def dataFileName: String = new Path(path).getName
}

private[sql] object OapFileFormat {
  val OAP_DATA_EXTENSION = ".data"
  val OAP_INDEX_EXTENSION = ".index"
  val OAP_META_EXTENSION = ".meta"
  val OAP_META_FILE = ".oap.meta"
  val OAP_META_SCHEMA = "oap.schema"
  val OAP_DATA_SOURCE_META = "oap.meta.datasource"
  val OAP_DATA_FILE_CLASSNAME = classOf[OapDataFile].getCanonicalName
  val PARQUET_DATA_FILE_CLASSNAME = classOf[ParquetDataFile].getCanonicalName

  val COMPRESSION = "oap.compression"
  val DEFAULT_COMPRESSION = OapConf.OAP_COMPRESSION.defaultValueString
  val ROW_GROUP_SIZE = "oap.rowgroup.size"
  val DEFAULT_ROW_GROUP_SIZE = OapConf.OAP_ROW_GROUP_SIZE.defaultValueString

  def serializeDataSourceMeta(conf: Configuration, meta: Option[DataSourceMeta]): Unit = {
    SerializationUtil.writeObjectToConfAsBase64(OAP_DATA_SOURCE_META, meta, conf)
  }

  def deserializeDataSourceMeta(conf: Configuration): Option[DataSourceMeta] = {
    SerializationUtil.readObjectFromConfAsBase64(OAP_DATA_SOURCE_META, conf)
  }

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
