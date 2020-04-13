package com.intel.sparkColumnarPlugin.execution

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class ColumnarBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan) extends BatchScanExec(output, scan) {
  override def supportsColumnar(): Boolean = true
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "BatchScan elapse time"))
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    val inputColumnarRDD = new ColumnarDataSourceRDD(sparkContext, partitions, readerFactory, true, scanTime)
    inputColumnarRDD.map { r =>
      numOutputRows += r.numRows()
      r
    }
  }
}
