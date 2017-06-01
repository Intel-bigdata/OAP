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

package org.apache.spark.sql.execution.datasources.spinach.statistics

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.Key
import org.apache.spark.sql.execution.datasources.spinach.index._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

/**
 * Manange all statistics info, use case:
 * Statistics build:
 * {{{
 * val statisticsManager = new StatisticsManager
 * statisticsManager.initialize(BTreeIndexType, schema)
 * for (key <- keys) statisticsManager.addSpinachKey(key)
 * statisticsManager.write(out)
 * }}}
 *
 * Statistics read:
 * {{{
 * val statisticsManager = new StatisticsManager
 * statisticsManager.read(file)
 * }}}
 */
class StatisticsManager {
  protected var stats: Array[Statistics] = _
  protected var schema: StructType = _

  // share key store for all statistics
  // for MinMax and BloomFilter, this is not necessary
  // but for SampleBase and PartByValue, this is needed
  protected var content: ArrayBuffer[Key] = _

  private var sortFlag: Boolean = _
  @transient private lazy val ordering = GenerateOrdering.create(schema)

  // for read with incorrect mask, the statistics is invalid
  private var invalidStatistics: Boolean = false

  def initialize(indexType: AnyIndexType, s: StructType): Unit = {
    val statsTypes = StatisticsManager.statisticsTypeMap(indexType)
    stats = statsTypes.map(t => t match {
      case MinMaxStatisticsType => new MinMaxStatistics
      case SampleBasedStatisticsType => new SampleBasedStatistics
      case PartByValueStatisticsType => new PartByValueStatistics
      case BloomFilterStatisticsType => new BloomFilterStatistics
      case _ => throw new UnsupportedOperationException(s"non-supported statistic type $t")
    })
    schema = s
    sortFlag = false
    content = new ArrayBuffer[Key]()
    stats.foreach(stat => stat.initialize(schema))
  }

  def addSpinachKey(key: Key): Unit = {
    content.append(key)
    stats.foreach(_.addSpinachKey(key))
    sortFlag = false
  }

  def write(out: IndexOutputWriter): Long = {
    if (!sortFlag) sortKeys()
    var offset = 0L

    IndexUtils.writeLong(out, StatisticsManager.STATISTICSMASK)
    offset += 8

    IndexUtils.writeInt(out, stats.length)
    offset += 4
    for (stat <- stats) {
      IndexUtils.writeInt(out, stat.id)
      offset += 4
    }

    stats.foreach(stat => {
      val off = stat.write(out, content)
      assert(off >= 0)
      offset += off
    })
    offset
  }

  private def sortKeys(): Unit = {
    content.sortWith((l, r) => ordering.compare(l, r) < 0)
  }


  def read(bytes: Array[Byte], s: StructType): Unit = {
    var offset = 0L
    val mask = Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    offset += 8
    if (mask != StatisticsManager.STATISTICSMASK) {
      invalidStatistics = true
    } else {
      val numOfStats = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      offset += 4
      stats = new Array[Statistics](numOfStats)

      for (i <- 0 until numOfStats) {
        Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset) match {
          case MinMaxStatisticsType.id => stats(i) = new MinMaxStatistics
          case SampleBasedStatisticsType.id => stats(i) = new SampleBasedStatistics
          case PartByValueStatisticsType.id => stats(i) = new PartByValueStatistics
          case BloomFilterStatisticsType.id => stats(i) = new BloomFilterStatistics
          case _ => throw new UnsupportedOperationException("unsupport statistics id")
        }
        offset += 4
      }
      for (stat <- stats) {
        stat.initialize(s)
        offset += stat.read(bytes, offset)
      }
    }
  }

  def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double = {
    var resSum: Double = 0.0
    var resNum: Int = 0

    if (invalidStatistics) StaticsAnalysisResult.USE_INDEX // use index if no statistics
    else {
      for (stat <- stats) {
        val res = stat.analyse(intervalArray)

        if (res == StaticsAnalysisResult.SKIP_INDEX) {
          resSum = StaticsAnalysisResult.SKIP_INDEX
        } else {
          resSum += res
          resNum += 1
        }
      }

      if (resSum == StaticsAnalysisResult.SKIP_INDEX) {
        StaticsAnalysisResult.SKIP_INDEX
      } else if (resNum == 0 || resSum / resNum <= StatisticsManager.FULLSCANTHRESHOLD) {
        StaticsAnalysisResult.USE_INDEX
      } else {
        StaticsAnalysisResult.FULL_SCAN
      }
    }
  }
}

object StatisticsManager {
  val STATISTICSMASK: Long = 0x20170524abcdefabL // a random mask for statistics begin

  val statisticsTypeMap: scala.collection.mutable.Map[AnyIndexType, Array[StatisticsType]] =
    scala.collection.mutable.Map(
      BTreeIndexType -> Array(MinMaxStatisticsType, SampleBasedStatisticsType,
        BloomFilterStatisticsType, PartByValueStatisticsType),
      BitMapIndexType -> Array(MinMaxStatisticsType, SampleBasedStatisticsType,
        BloomFilterStatisticsType, PartByValueStatisticsType))

  var sampleRate: Double = 0.1
  var partNumber: Int = 5
  var bloomFilterMaxBits: Int = 1 << 20
  var bloomFilterHashFuncs: Int = 3

  var FULLSCANTHRESHOLD: Double = 0.8

  // TODO we need to find better ways to configure these parameters
  def setStatisticsType(indexType: AnyIndexType, statisticsType: Array[StatisticsType]): Unit =
    statisticsTypeMap.update(indexType, statisticsType)
  def setSampleRate(rate: Double): Unit = this.sampleRate = rate
  def setPartNumber(num: Int): Unit = this.partNumber = num
  def setFullScanThreshold(rate: Double): Unit = this.FULLSCANTHRESHOLD = rate
}
