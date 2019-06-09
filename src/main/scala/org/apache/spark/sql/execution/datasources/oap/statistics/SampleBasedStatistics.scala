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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{Aggregator, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.OapExternalSorter

private[oap] class SampleBasedStatisticsReader(
    schema: StructType) extends StatisticsReader(schema) with Logging{
  override val id: Int = StatisticsType.TYPE_SAMPLE_BASE

  @transient private lazy val ordering = GenerateOrdering.create(schema)

  protected var sampleArray: Array[Key] = _

  override def read(fiberCache: FiberCache, offset: Int): Int = {
    logInfo(s"start to read sample")
    var readOffset = super.read(fiberCache, offset) + offset

    val size = fiberCache.getInt(readOffset)
    readOffset += 4

    // TODO use unsafe way to interact with sample array
    sampleArray = new Array[Key](size)
    logInfo(s"read sampleArray size: ${sampleArray.size}")
    var rowOffset = 0
    for (i <- 0 until size) {
      sampleArray(i) = nnkr.readKey(
        fiberCache, readOffset + size * IndexUtils.INT_SIZE + rowOffset)._1
      rowOffset = fiberCache.getInt(readOffset + i * IndexUtils.INT_SIZE)
    }
    logInfo(s"read rowOffset: ${rowOffset}")
    readOffset += (rowOffset + size * IndexUtils.INT_SIZE)
    readOffset - offset
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): StatsAnalysisResult = {
    if (sampleArray == null || sampleArray.isEmpty) {
      logInfo("Use Index")
      StatsAnalysisResult.USE_INDEX
    } else {
      var hitCnt = 0
      val partialOrder = GenerateOrdering.create(StructType(schema.dropRight(1)))
      for (row <- sampleArray) {
        if (Statistics.rowInIntervalArray(row, intervalArray, ordering, partialOrder)) {
          hitCnt += 1
        }
      }
      logInfo((hitCnt * 1.0 / sampleArray.length).toString)
      StatsAnalysisResult(hitCnt * 1.0 / sampleArray.length)
    }
  }
}

private[oap] class SampleBasedStatisticsWriter(schema: StructType, conf: Configuration)
  extends StatisticsWriter(schema, conf) with Logging{
  override val id: Int = StatisticsType.TYPE_SAMPLE_BASE

  lazy val sampleRate: Double = conf.getDouble(
    OapConf.OAP_STATISTICS_SAMPLE_RATE.key, OapConf.OAP_STATISTICS_SAMPLE_RATE.defaultValue.get)

  private val minSampleSize = conf.getInt(
    OapConf.OAP_STATISTICS_SAMPLE_MIN_SIZE.key,
    OapConf.OAP_STATISTICS_SAMPLE_MIN_SIZE.defaultValue.get)

  protected var sampleArray: Array[Key] = _

  protected var isExternalSorterEnable =
    conf.getBoolean(OapConf.OAP_INDEX_STATISTIC_EXTERNALSORTER_ENABLE.key,
    OapConf.OAP_INDEX_STATISTIC_EXTERNALSORTER_ENABLE.defaultValue.get)

  @transient private lazy val ordering = GenerateOrdering.create(schema)
  protected val combiner: Int => Seq[Int] = Seq(_)
  protected val merger: (Seq[Int], Int) => Seq[Int] = _ :+ _
  protected val mergeCombiner: (Seq[Int], Seq[Int]) => Seq[Int] = _ ++ _
  protected val aggregator =
    new Aggregator[InternalRow, Int, Seq[Int]](combiner, merger, mergeCombiner)
  protected var externalSorter = {
      if (isExternalSorterEnable) {
      val taskContext = TaskContext.get()
      val sorter = new OapExternalSorter[InternalRow, Int, Seq[Int]](
        taskContext, Some(aggregator), Some(ordering))
      taskContext.addTaskCompletionListener(_ => sorter.stop())
      sorter
    } else {
      null
    }
  }
  private var recordCount: Int = 0

  override def addOapKey(key: Key): Unit = {
    if (isExternalSorterEnable) {
      externalSorter.insert(key, recordCount)
      recordCount += 1
    }
  }

  private def internalWrite(writer: OutputStream, offsetP: Int, sizeP: Int): Int = {
    var offset = offsetP
    val size = sizeP
    IndexUtils.writeInt(writer, size)
    offset += IndexUtils.INT_SIZE
    val tempWriter = new ByteArrayOutputStream()
    sampleArray.foreach(key => {
      nnkw.writeKey(tempWriter, key)
      IndexUtils.writeInt(writer, tempWriter.size())
      offset += IndexUtils.INT_SIZE
    })
    offset += tempWriter.size()
    writer.write(tempWriter.toByteArray)
    offset
  }

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
    val offset = super.write(writer, sortedKeys)
    val size = math.max(
      (sortedKeys.size * sampleRate).toInt, math.min(minSampleSize, sortedKeys.size))
    sampleArray = takeSample(sortedKeys, size)

    internalWrite(writer, offset, size)
  }

  protected def takeSample(keys: ArrayBuffer[InternalRow], size: Int): Array[InternalRow] =
    Random.shuffle(keys.indices.toList).take(size).map(keys(_)).toArray

  /*
  override def customWrite(writer: OutputStream): Int = {
    val offset = super.customWrite(writer)
    val sortedIter = externalSorter.iterator
    val keySize = recordCount
    val size = math.max(
      (keySize * sampleRate).toInt, math.min(minSampleSize, keySize))
    sampleArray = takeSample(sortedIter, size, keySize)
    logInfo(s"write sampleArray size: ${sampleArray.size}")
    internalWrite(writer, offset, sampleArray.size)
  }
  */

  /*
  protected def takeSample(
    sortedIter: Iterator[Product2[Key, Seq[Int]]],
    size: Int,
    keySize: Int): Array[InternalRow] = {
    // use Hashset only when sample size is much smaller than keySize
    if (size == (keySize * sampleRate).toInt) {
      val hashset: mutable.HashSet[Int] = mutable.HashSet.empty[Int]
      val keyBuffer: mutable.Buffer[Key] = mutable.Buffer.empty[Key]
      while (hashset.size < size) {
        hashset.add(Random.nextInt((keySize - 1)))
      }
      logInfo(s"use hashset: ${hashset.size}")

      for(value <- sortedIter) {
        value._2.foreach(
          idx => {
            if (hashset.contains(idx)) keyBuffer += value._1
          }
        )
      }
      keyBuffer.toArray

      // sortedIter.zipWithIndex.filter(v => hashset.contains(v._2)).map(_._1._1).toArray
    } else {
      val dataList = sortedIter.toList
      logInfo(s"use dataList: ${dataList.size}")
      Random.shuffle(dataList.indices.toList)
        .take(size).map(dataList(_)._1).toArray
    }
  }
  */

  private var randomIdxArray: Array[Int] = _
  private var sampleArrayBuffer: ArrayBuffer[Key] = _
  private var keyCount: Int = 0

  override def customWrite(writer: OutputStream): Int = {
    val offset = super.customWrite(writer)
    sampleArray = sampleArrayBuffer.toArray
    sampleArrayBuffer = null
    logInfo(s"write sampleArray size: ${sampleArray.size}")
    internalWrite(writer, offset, sampleArray.size)
  }

  def initParams(totalSorterRecordSize: Int): Unit = {
    val size = math.max(
      (totalSorterRecordSize * sampleRate).toInt, math.min(minSampleSize, totalSorterRecordSize))
    if (size == (totalSorterRecordSize * sampleRate).toInt) {
      val hashset: mutable.HashSet[Int] = mutable.HashSet.empty[Int]
      while (hashset.size < size) {
        hashset.add(Random.nextInt((totalSorterRecordSize - 1)))
      }
      randomIdxArray = hashset.toArray
      logInfo(s"use randomIdxArray: ${randomIdxArray.size}")
    } else {
      randomIdxArray =
        Random.shuffle((0 to totalSorterRecordSize).indices.toList).take(size).toArray
    }
    sampleArrayBuffer = ArrayBuffer.empty[Key]
  }

  def buildSampleArray(keyArray: Array[Product2[Key, Seq[Int]]], isLast: Boolean): Unit = {
    keyArray.foreach(
      value => {
        value._2.foreach(
          _ => {
            if (randomIdxArray.contains(keyCount)) {
              sampleArrayBuffer += value._1
            }
            keyCount += 1
          }
        )
      }
    )
  }
}
