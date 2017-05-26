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

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.spinach.index.{BloomFilter, IndexOutputWriter, IndexUtils, RangeInterval}
import org.apache.spark.sql.execution.datasources.spinach.statistics.{BloomFilterStatistics, BloomFilterStatisticsType, StaticsAnalysisResult}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

class TestIndexOutputWriter extends IndexOutputWriter(bucketId = None, context = null) {
  val buf = new ByteArrayOutputStream(8000)
  override protected lazy val writer: RecordWriter[Void, Any] =
    new RecordWriter[Void, Any] {
      override def close(context: TaskAttemptContext) = buf.close()
      override def write(key: Void, value: Any) = value match {
        case bytes: Array[Byte] => buf.write(bytes)
        case i: Int => buf.write(i) // this will only write a byte
      }
    }
}

class BloomFilterStatisticsSuite extends SparkFunSuite {
  def rowGen(i: Int): InternalRow = InternalRow(i, UTF8String.fromString(s"test#$i"))

  test("write function test") {
  }

  test("read function test") {
  }

  test("read AND write test") {
  }
}
