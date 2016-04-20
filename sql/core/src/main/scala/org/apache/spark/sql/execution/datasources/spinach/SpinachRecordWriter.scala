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

import java.io.DataOutputStream

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

private[spinach] class SpinachRecordWriter(
    isCompressed: Boolean,
    out: DataOutputStream,
    schema: StructType) extends RecordWriter[NullWritable, InternalRow] {
  // TODO: make the fiber size configurable
  private final val DEFAULT_ROW_GROUP_SIZE = 1024
  private var rowCount: Int = 0
  private var fileLen: Long = 0L

  private val rowGroup: Array[FiberBuilder] =
    FiberBuilder.initializeFromSchema(schema, DEFAULT_ROW_GROUP_SIZE)
  private val metaBuilder = new SpinachSplitMetaBuilder()

  metaBuilder.setColumnNumber(rowGroup.length)
  metaBuilder.setDefaultRowGroupSize(DEFAULT_ROW_GROUP_SIZE)

  override def write(ignore: NullWritable, row: InternalRow) {
    // TODO compressed
//    var codec: CompressionCodec = null
//    if (isCompressed) {
//      val codecClass: Class[_ <: CompressionCodec] =
//         FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
//      codec = ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[CompressionCodec]
//    }
    var idx = 0
    while (idx < rowGroup.length) {
      rowGroup(idx).append(row, idx)
      idx += 1
    }
    rowCount += 1
    if (rowGroup(0).records == DEFAULT_ROW_GROUP_SIZE) {
      writeRowGroup()
    }
  }

  private def writeRowGroup(): Unit = {
    val startPos: Long = fileLen
    val fiberLen = new Array[Int](rowGroup.length)
    var idx: Int = 0
    while (idx < rowGroup.length) {
      val fiberData = rowGroup(idx).build()
      for (bitVal <- rowGroup(idx).bitStream.toLongArray()) {
        out.writeLong(bitVal)
        fileLen += 8
      }
      var len: Int = 0
      for (data <- fiberData) {
        out.write(data._1, 0, data._2)
        fileLen += data._2
        len += data._2
      }
      fiberLen(idx) = len
      rowGroup(idx).clear()
      idx += 1
    }
    metaBuilder.addRowGroupMeta((startPos, fiberLen))
  }

  override def close(context: TaskAttemptContext) {

    metaBuilder.setLastRowGroupSize(rowGroup(0).records)
    writeRowGroup()
    metaBuilder.build().write(out)
    out.close
  }
}
