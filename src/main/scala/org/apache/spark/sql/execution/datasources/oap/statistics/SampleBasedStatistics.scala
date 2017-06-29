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
package org.apache.spark.sql.execution.datasources.oap.statistics

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.unsafe.Platform


private[oap] class SampleBasedStatistics extends Statistics {
  override val id: Int = SampleBasedStatisticsType.id

  lazy val sampleRate: Double = StatisticsManager.sampleRate
  @transient private lazy val converter = UnsafeProjection.create(schema)
  @transient private lazy val ordering = GenerateOrdering.create(schema)


  protected var sampleArray: Array[Key] = _

  // SampleBasedStatistics file structure
  // statistics_id        4 Bytes, Int, specify the [[Statistic]] type
  // sample_size          4 Bytes, Int, number of UnsafeRow
  //
  // | unsafeRow-1 sizeInBytes | unsafeRow-1 content |   (4 + u1_sizeInBytes) Bytes, unsafeRow-1
  // | unsafeRow-2 sizeInBytes | unsafeRow-2 content |   (4 + u2_sizeInBytes) Bytes, unsafeRow-2
  // | unsafeRow-3 sizeInBytes | unsafeRow-3 content |   (4 + u3_sizeInBytes) Bytes, unsafeRow-3
  // ...
  // | unsafeRow-(sample_size) sizeInBytes | unsafeRow-(sample_size) content |
  override def write(writer: IndexOutputWriter, sortedKeys: ArrayBuffer[Key]): Long = {
    var offset = super.write(writer, sortedKeys)
    val size = (sortedKeys.size * sampleRate).toInt
    sampleArray = takeSample(sortedKeys, size)

    IndexUtils.writeInt(writer, size)
    offset += 4
    sampleArray.foreach(key => {
      offset += Statistics.writeInternalRow(converter, key, writer)
    })
    offset
  }

  override def read(bytes: Array[Byte], baseOffset: Long): Long = {
    var offset = super.read(bytes, baseOffset) + baseOffset

    val size = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    offset += 4

    // TODO is it ok to store all sample array in memory?
    sampleArray = new Array[Key](size)

    for (i <- 0 until size) {
      val rowSize = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      sampleArray(i) = Statistics.getUnsafeRow(schema.length, bytes, offset, rowSize).copy()
      offset += (4 + rowSize)
    }
    offset - baseOffset
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double = {
    if (sampleArray == null || sampleArray.length <= 0) {
      StaticsAnalysisResult.USE_INDEX
    } else {
      var hitCnt = 0
      for (row <- sampleArray) {
        if (Statistics.rowInIntervalArray(row, intervalArray, ordering)) hitCnt += 1
      }
      hitCnt * 1.0 / sampleArray.length
    }
  }

  protected def takeSample(keys: ArrayBuffer[InternalRow], size: Int): Array[InternalRow] =
    Random.shuffle(keys.indices.toList).take(size).map(keys(_)).toArray
}
