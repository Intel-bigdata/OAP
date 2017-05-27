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

package org.apache.spark.sql.execution.datasources.spinach.index

import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources._

private[spinach] class IndexOutputWriter(
    bucketId: Option[Int],
    context: TaskAttemptContext)
  extends OutputWriter {
  protected lazy val writer: RecordWriter[Void, Any] = {
    val outputFormat = new SpinachIndexOutputFormat[Any]()
    outputFormat.getRecordWriter(context)
  }

  def write(b: Array[Byte]): Unit = write(b, 0, b.length)
  def write(b: Array[Byte], off: Int, len: Int): Unit = writer.write(null, b)

  def write(i: Int): Unit = writer.write(null, i)

  override def close(): WriteResult = writer.close(context)

  override def write(row: Row): Unit = throw new UnsupportedOperationException("don't use this")
  // TODO block writeInternal
}
