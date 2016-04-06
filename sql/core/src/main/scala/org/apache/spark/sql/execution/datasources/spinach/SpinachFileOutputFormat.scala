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
  private val writers: Array[DataReaderWriter] =
    DataReaderWriter.initialDataReaderWriterFromSchema(schema)

  private var count = 0

  override def write(ignore: NullWritable, row: InternalRow) {
    // TODO compressed
//    var codec: CompressionCodec = null
//    if (isCompressed) {
//      val codecClass: Class[_ <: CompressionCodec] =
//         FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
//      codec = ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[CompressionCodec]
//    }
    var i = 0
    while (i < writers.length) {
      writers(i).write(out, row)
      i += 1
    }
    count += 1
  }

  override def close(context: TaskAttemptContext) {
    out.writeInt(count) // write the record count in this partition
    out.close
  }
}
