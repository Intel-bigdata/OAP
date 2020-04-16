package org.apache.spark.sql.execution.datasources.v2

import com.intel.sparkColumnarPlugin.datasource.VectorizedParquetArrowReader

import java.net.URI
import java.time.ZoneId

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, PartitionReader}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.PartitionedFileReader
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetPartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReader
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object VectorizedFilePartitionReaderHandler {
  def get(
    inputPartition: InputPartition,
    parquetReaderFactory: ParquetPartitionReaderFactory) 
  : FilePartitionReader[ColumnarBatch] = {
    val iter: Iterator[PartitionedFileReader[ColumnarBatch]] 
      = inputPartition.asInstanceOf[FilePartition].files.toIterator.map { file =>
      val filePath = new Path(new URI(file.filePath))
      val split =
      new org.apache.parquet.hadoop.ParquetInputSplit(
        filePath,
        file.start,
        file.start + file.length,
        file.length,
        Array.empty,
        null)
      //val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
      /*val convertTz =
        if (timestampConversion && !isCreatedByParquetMr) {
          Some(DateTimeUtils.getZoneId(conf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
        } else {
          None
        }*/
      val capacity = 4096
      //partitionReaderFactory.createColumnarReader(inputPartition)
      val dataSchema = parquetReaderFactory.dataSchema
      val readDataSchema = parquetReaderFactory.readDataSchema
      
      val conf = parquetReaderFactory.broadcastedConf.value.value
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

      val vectorizedReader = new VectorizedParquetArrowReader(split.getPath().toString(), null, false, capacity, dataSchema, readDataSchema)
      vectorizedReader.initialize(split, hadoopAttemptContext)
      val partitionReader = new PartitionReader[ColumnarBatch] {
        override def next(): Boolean = vectorizedReader.nextKeyValue()
        override def get(): ColumnarBatch =
          vectorizedReader.getCurrentValue.asInstanceOf[ColumnarBatch]
        override def close(): Unit = vectorizedReader.close()
      }

      PartitionedFileReader(file, partitionReader)
    }
    new FilePartitionReader[ColumnarBatch](iter)
  }
}

