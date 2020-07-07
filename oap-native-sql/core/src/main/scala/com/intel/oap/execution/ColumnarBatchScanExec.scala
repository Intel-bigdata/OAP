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

package com.intel.oap.execution

import com.intel.oap.ColumnarPluginConfig
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class ColumnarBatchScanExec(output: Seq[AttributeReference], @transient scan: Scan)
    extends BatchScanExec(output, scan) {
  val tmpDir = ColumnarPluginConfig.getConf(sparkContext.getConf).tmpFile
  override def supportsColumnar(): Boolean = true
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "BatchScan elapse time"))
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    val inputColumnarRDD =
      new ColumnarDataSourceRDD(sparkContext, partitions, readerFactory, true, scanTime, tmpDir)
    inputColumnarRDD.map { r =>
      numOutputRows += r.numRows()
      r
    }
  }
}
