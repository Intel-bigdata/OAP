package org.apache.spark.sql.execution.datasources.parquet

import com.intel.oap.datasource.VectorizedParquetArrowReader

import java.io.IOException
import java.net.URI

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.schema.MessageType

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils}

object ColumnarParquetFileFormatHandler extends ParquetFileFormat{
    
    def buildColumnarReaderWithPartitionValues(
      @transient relation: HadoopFsRelation,
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      dataFilters: Seq[Expression],
      options: Map[String, String],
      hadoopConf: Configuration,
      tmp_dir: String): (PartitionedFile) => Iterator[ColumnarBatch] = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    ParquetWriteSupport.setSchema(requiredSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // TODO: if you move this into the closure it reverts to the default values.
    // If true, enable using the custom RecordReader for parquet. This only works for
    // a subset of the types (no complex types).
    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val enableVectorizedReader: Boolean =
      sqlConf.parquetVectorizedReaderEnabled &&
      resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
    val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
    val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
    val capacity = sqlConf.parquetVectorizedReaderBatchSize
    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
    // Whole stage codegen (PhysicalRDD) is able to deal with batches directly
    val returningBatch = supportBatch(sparkSession, resultSchema)
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      val filePath = new Path(new URI(file.filePath))
      val split =
        new org.apache.parquet.hadoop.ParquetInputSplit(
          filePath,
          file.start,
          file.start + file.length,
          file.length,
          Array.empty,
          null)

      val sharedConf = broadcastedHadoopConf.value.value

      lazy val filters = {
        val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
        dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
      }
      lazy val footerFileMetaData =
        ParquetFileReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData
      // Try to push down filters when filter push-down is enabled.
      val pushed = if (enableParquetFilterPushDown) {
        val parquetSchema = footerFileMetaData.getSchema
        val parquetFilters = new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
          pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
        filters
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
          .flatMap(parquetFilters.createFilter(_))
          .reduceOption(FilterApi.and)
      } else {
        None
      }

      // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
      // *only* if the file was created by something other than "parquet-mr", so check the actual
      // writer here for this file.  We have to do this per-file, as each file in the table may
      // have different writers.
      // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
      def isCreatedByParquetMr: Boolean =
        footerFileMetaData.getCreatedBy().startsWith("parquet-mr")

      val convertTz =
        if (timestampConversion && !isCreatedByParquetMr) {
          Some(DateTimeUtils.getTimeZone(sharedConf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
        } else {
          None
        }

      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext =
        new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

      // Try to push down filters when filter push-down is enabled.
      // Notice: This push-down is RowGroups level, not individual records.
      if (pushed.isDefined) {
        ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
      }
      if (!enableVectorizedReader) {
        logDebug(s"Vectorized reader is disabled.")
      }
      val taskContext = Option(TaskContext.get())
      val requiredNameSet =
        requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet
      val partitionNameSet =
        partitionSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet
      val fields = dataSchema.fields.filter { field =>
        val colName = PartitioningUtils.getColName(field, isCaseSensitive)
        requiredNameSet.contains(colName) && !partitionNameSet.contains(colName)
      }
      val readDataSchema = StructType(fields)
      val vectorizedReader = new VectorizedParquetArrowReader(
          split.getPath().toString(),
          null,
          false,
          capacity,
          dataSchema,
          readDataSchema,
          tmp_dir)
      val iter = new RecordReaderIterator(vectorizedReader)
      // SPARK-23457 Register a task completion lister before `initialization`.
      taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
      vectorizedReader.initialize(split, hadoopAttemptContext)
      logDebug(s"Appending $partitionSchema ${file.partitionValues}")
      vectorizedReader.initBatch(partitionSchema, file.partitionValues)
      if (returningBatch) {
        vectorizedReader.enableReturningBatches()
      }

      // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
      iter.asInstanceOf[Iterator[ColumnarBatch]]
    }
  }
}
