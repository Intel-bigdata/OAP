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

import java.io.OutputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Manange all statistics info, use case:
 * Statistics build:
 * {{{
 * val statisticsManager = new StatisticsManager
 * statisticsManager.initialize(BTreeIndexType, schema)
 * for (key <- keys) statisticsManager.addOapKey(key)
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

  @transient private lazy val ordering = GenerateOrdering.create(schema)

  // for read with incorrect mask, the statistics is invalid
  private var invalidStatistics: Boolean = false

  def initialize(indexType: AnyIndexType, s: StructType, conf: Configuration): Unit = {

    StatisticsManager.setPartNumber(
      conf.getInt(SQLConf.OAP_STATISTICS_PART_NUM.key,
        SQLConf.OAP_STATISTICS_PART_NUM.defaultValue.get)
    )

    StatisticsManager.setSampleRate(
      conf.getDouble(SQLConf.OAP_STATISTICS_SAMPLE_RATE.key,
        SQLConf.OAP_STATISTICS_SAMPLE_RATE.defaultValue.get)
    )

    StatisticsManager.setBloomFilterMaxBits(
      conf.getInt(SQLConf.OAP_BLOOMFILTER_MAXBITS.key,
        SQLConf.OAP_BLOOMFILTER_MAXBITS.defaultValue.get)
    )

    StatisticsManager.setBloomFilterHashFuncs(
      conf.getInt(SQLConf.OAP_BLOOMFILTER_NUMHASHFUNC.key,
        SQLConf.OAP_BLOOMFILTER_NUMHASHFUNC.defaultValue.get)
    )

    val statsTypes = StatisticsManager.statisticsTypeMap(indexType).filter { statType =>
      val typeFromConfig = conf.get(SQLConf.OAP_STATISTICS_TYPES.key,
        SQLConf.OAP_STATISTICS_TYPES.defaultValueString).split(",").map(_.trim)
      typeFromConfig.contains(statType.name)
    }
    schema = s
    stats = statsTypes.map {
      case MinMaxStatisticsType => new MinMaxStatistics(s)
      case SampleBasedStatisticsType => new SampleBasedStatistics(s)
      case PartByValueStatisticsType => new PartByValueStatistics(s)
      case BloomFilterStatisticsType => new BloomFilterStatistics(s)
      case t => throw new UnsupportedOperationException(s"non-supported statistic type $t")
    }
    content = new ArrayBuffer[Key]()
  }

  def addOapKey(key: Key): Unit = {
    if (key.anyNull) return // stats info does not collect null keys
    content.append(key)
    stats.foreach(_.addOapKey(key))
  }

  def write(out: OutputStream): Int = {
    var offset = 0

    IndexUtils.writeLong(out, StatisticsManager.STATISTICSMASK)
    offset += 8

    IndexUtils.writeInt(out, stats.length)
    offset += 4
    for (stat <- stats) {
      IndexUtils.writeInt(out, stat.id)
      offset += 4
    }

    val sortedKeys = sortKeys
    stats.foreach(stat => {
      val off = stat.write(out, sortedKeys)
      assert(off >= 0)
      offset += off
    })
    offset
  }

  private def sortKeys = content.sortWith((l, r) => ordering.compare(l, r) < 0)

  def read(fiberCache: FiberCache, offset: Int, s: StructType): Unit = {
    var readOffset = 0
    val mask = fiberCache.getLong(offset + readOffset)
    readOffset += 8
    if (mask != StatisticsManager.STATISTICSMASK) {
      invalidStatistics = true
    } else {
      val numOfStats = fiberCache.getInt(offset + readOffset)
      readOffset += 4
      stats = new Array[Statistics](numOfStats)

      for (i <- 0 until numOfStats) {
        fiberCache.getInt(offset + readOffset) match {
          case MinMaxStatisticsType.id => stats(i) = new MinMaxStatistics(s)
          case SampleBasedStatisticsType.id => stats(i) = new SampleBasedStatistics(s)
          case PartByValueStatisticsType.id => stats(i) = new PartByValueStatistics(s)
          case BloomFilterStatisticsType.id => stats(i) = new BloomFilterStatistics(s)
          case _ => throw new UnsupportedOperationException("unsupport statistics id")
        }
        readOffset += 4
      }
      for (stat <- stats) {
        readOffset += stat.read(fiberCache, offset + readOffset)
      }
    }
  }

  def analyse(intervalArray: ArrayBuffer[RangeInterval], conf: Configuration): Double = {
    var resSum: Double = 0.0
    var resNum: Int = 0

    StatisticsManager.setFullScanThreshold(
      conf.getDouble(SQLConf.OAP_FULL_SCAN_THRESHOLD.key,
        SQLConf.OAP_FULL_SCAN_THRESHOLD.defaultValue.get))

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
      BTreeIndexType -> Array(
        MinMaxStatisticsType, SampleBasedStatisticsType,
        BloomFilterStatisticsType, PartByValueStatisticsType),
      BitMapIndexType -> Array.empty)

  /**
   * Using a static object to store parameter is not a good idea, some reasons:
   * 1. In local mode, driver and worker will use one same object, but in cluster they are different
   * 2. object.setXXX() then val a = object.XXX can be realized by Configuration.set and get
   * But
   * 1. current Statistics has no interface to set parameters by Configuration and many test suites
   * depend on this static object.
   * 2. the `initialize()` in each Statistics is depended by both `read` and `write` while `read`
   * should get parameter from disk and doesn't need Configuration.
   * 3. Some test suites only init specific Statistics and not init StatisticsManager, so remove
   * this object will need interface change in every Statistics sub classes, this means more changes
   * So, TODO: Let's address all of this latter.
   */
  var sampleRate: Double = SQLConf.OAP_STATISTICS_SAMPLE_RATE.defaultValue.get
  var partNumber: Int = SQLConf.OAP_STATISTICS_PART_NUM.defaultValue.get
  var bloomFilterMaxBits: Int = SQLConf.OAP_BLOOMFILTER_MAXBITS.defaultValue.get
  var bloomFilterHashFuncs: Int = SQLConf.OAP_BLOOMFILTER_NUMHASHFUNC.defaultValue.get

  var FULLSCANTHRESHOLD: Double = SQLConf.OAP_FULL_SCAN_THRESHOLD.defaultValue.get

  // TODO we need to find better ways to configure these parameters
  def setStatisticsType(indexType: AnyIndexType, statisticsType: Array[StatisticsType]): Unit =
    statisticsTypeMap.update(indexType, statisticsType)
  def setSampleRate(rate: Double): Unit = this.sampleRate = rate
  def setPartNumber(num: Int): Unit = this.partNumber = num
  def setFullScanThreshold(rate: Double): Unit = this.FULLSCANTHRESHOLD = rate
  def setBloomFilterMaxBits(maxBits: Int): Unit = this.bloomFilterMaxBits = maxBits
  def setBloomFilterHashFuncs(num: Int): Unit = this.bloomFilterHashFuncs = num
}
