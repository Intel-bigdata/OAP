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

package org.apache.spark.sql.execution.datasources.oap.index

import java.io.DataOutputStream
import java.nio.charset.Charset
import java.util.Comparator

import com.google.common.collect.ArrayListMultimap
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.expressions.{Ascending, BoundReference, Descending, FromUnsafeProjection, SortOrder}
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.utils.BTreeUtils
import org.apache.spark.sql.types.StructType

/*
 * Created by linhongl on 25/09/2017.
 */
private[index] class BTreeIndexRecordWriter(
    fileWriter: OapIndexFileWriter,
    keySchema: StructType) extends RecordWriter[Void, InternalRow] {

  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)

  private val multiHashMap = ArrayListMultimap.create[InternalRow, Long]()
  private var recordCount: Long = 0

  override def write(key: Void, value: InternalRow): Unit = {
    val v = genericProjector(value).copy()
    multiHashMap.put(v, recordCount)
    recordCount += 1
  }

  override def close(context: TaskAttemptContext): Unit = {
    fileWriter.flushToFile(multiHashMap)
    fileWriter.close()
  }
}
