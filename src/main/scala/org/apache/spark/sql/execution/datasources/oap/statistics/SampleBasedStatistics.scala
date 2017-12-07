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

import java.io.{ByteArrayOutputStream, OutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.types.StructType


private[oap] class SampleBasedStatistics extends Statistics {
  override val id: Int = SampleBasedStatisticsType.id

  lazy val sampleRate: Double = StatisticsManager.sampleRate
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
  override def write(writer: OutputStream, sortedKeys: ArrayBuffer[Key]): Int = {
    var offset = super.write(writer, sortedKeys)
    val size = (sortedKeys.size * sampleRate).toInt
    sampleArray = takeSample(sortedKeys, size)

    IndexUtils.writeInt(writer, size)
    offset += IndexUtils.INT_SIZE
    val tempWriter = new ByteArrayOutputStream()
    sampleArray.foreach(key => {
      IndexUtils.writeBasedOnSchema(tempWriter, key, schema)
      IndexUtils.writeInt(writer, tempWriter.size())
      offset += IndexUtils.INT_SIZE
    })
    offset += tempWriter.size()
    writer.write(tempWriter.toByteArray)
    offset
  }

  override def read(fiberCache: FiberCache, offset: Int): Int = {
    var readOffset = super.read(fiberCache, offset) + offset

    val size = fiberCache.getInt(readOffset)
    readOffset += 4

    // TODO use unsafe way to interact with sample array
    sampleArray = new Array[Key](size)

    var rowOffset = 0
    for (i <- 0 until size) {
      sampleArray(i) = IndexUtils.readBasedOnSchema(
        fiberCache, readOffset + size * IndexUtils.INT_SIZE + rowOffset, schema)
      rowOffset = fiberCache.getInt(readOffset + i * IndexUtils.INT_SIZE)
    }
    readOffset += (rowOffset + size * IndexUtils.INT_SIZE)
    readOffset - offset
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double = {
    if (sampleArray == null || sampleArray.isEmpty) {
      StaticsAnalysisResult.USE_INDEX
    } else {
      var hitCnt = 0
      val partialOrder = GenerateOrdering.create(StructType(schema.dropRight(1)))
      for (row <- sampleArray) {
        if (Statistics.rowInIntervalArray(row, intervalArray, ordering, partialOrder)) hitCnt += 1
      }
      hitCnt * 1.0 / sampleArray.length
    }
  }

  protected def takeSample(keys: ArrayBuffer[InternalRow], size: Int): Array[InternalRow] =
    Random.shuffle(keys.indices.toList).take(size).map(keys(_)).toArray
}
