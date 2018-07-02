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

import org.apache.spark.{Aggregator, TaskContext}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OapExternalSorter

private[statistics] class SortedKeys(sortedKeysWithOccurTimes: Iterator[(Key, Int)])
    extends Iterator[Key] {

  var times: Int = _
  var current: Key = _

  override def hasNext: Boolean = sortedKeysWithOccurTimes.hasNext || times != 0

  override def next(): Key = {
    if (times == 0) {
      val tuple = sortedKeysWithOccurTimes.next()
      current = tuple._1
      times = tuple._2
    }
    times -= 1
    current
  }
}

/**
 * Statistics write:
 * {{{
 * val statisticsManager = new StatisticsWriteManager
 * statisticsManager.initialize(BTreeIndexType, schema)
 * for (key <- keys) statisticsManager.addOapKey(key)
 * statisticsManager.write(out)
 * }}}
 */
class StatisticsWriteManager {
  protected var stats: Array[StatisticsWriter] = _
  protected var schema: StructType = _

  private val combiner = (_: Int) => 1
  private val merger = (c: Int, _: Int) => c + 1
  private val mergeCombiner = (c1: Int, c2: Int) => c1 + c2
  private val aggregator =
    new Aggregator[Key, Int, Int](combiner, merger, mergeCombiner)

  // share key store for all statistics
  // for MinMax and BloomFilter, this is not necessary
  // but for SampleBase and PartByValue, this is needed
  private lazy val keys = {
    val taskContext = TaskContext.get()
    val sorter = new OapExternalSorter[Key, Int, Int](taskContext, Some(aggregator), Some(ordering))
    taskContext.addTaskCompletionListener(_ => sorter.stop())
    sorter
  }

  private lazy val keys2 = {
    val taskContext = TaskContext.get()
    val sorter = new OapExternalSorter[Key, Int, Int](taskContext, Some(aggregator), Some(ordering))
    taskContext.addTaskCompletionListener(_ => sorter.stop())
    sorter
  }

  @transient private lazy val ordering = GenerateOrdering.create(schema)

  // When a task initialize statisticsWriteManager, we read all config from `conf`,
  // which is created from `SparkUtils`, hence containing all spark config values.
  def initialize(indexType: OapIndexType, s: StructType, conf: Configuration): Unit = {
    val statsTypes = StatisticsManager.statisticsTypeMap(indexType).filter { statType =>
      val typeFromConfig = conf.get(OapConf.OAP_STATISTICS_TYPES.key,
        OapConf.OAP_STATISTICS_TYPES.defaultValueString).split(",").map(_.trim)
      typeFromConfig.contains(statType)
    }
    schema = s
    stats = statsTypes.map {
      case StatisticsType(st) => st(s, conf)
      case t => throw new UnsupportedOperationException(s"non-supported statistic type $t")
    }
  }

  def addOapKey(key: Key): Unit = {
    if (key.anyNull || stats.isEmpty) {
      // stats info does not collect null keys
      return
    }
    val whatever: Int = 0
    keys.insert(key, whatever)
    keys2.insert(key, whatever)
    stats.foreach(_.addOapKey(key))
  }

  def write(out: OutputStream): Int = {
    var offset = 0

    IndexUtils.writeLong(out, StatisticsManager.STATISTICSMASK)
    offset += 8

    IndexUtils.writeInt(out, stats.length)
    offset += 4
    stats.foreach { stat =>
      IndexUtils.writeInt(out, stat.id)
      offset += 4
    }

    val sortedKeys = new SortedKeys(keys.iterator.asInstanceOf[Iterator[(Key, Int)]])
    val sortedKeys2 = new SortedKeys(keys2.iterator.asInstanceOf[Iterator[(Key, Int)]])

    stats.foreach { stat =>
      var off = 0
      if (stat.isInstanceOf[SampleBasedStatisticsWriter]) {
        off = stat.write(out, sortedKeys)
      } else if (stat.isInstanceOf[PartByValueStatisticsWriter]) {
        off = stat.write(out, sortedKeys2)
      } else {
        stat.write(out, null)
      }
      assert(off >= 0)
      offset += off
    }
    offset
  }

}

object StatisticsManager {
  val STATISTICSMASK: Long = 0x20170524abcdefabL // a random mask for statistics begin

  val statisticsTypeMap: scala.collection.mutable.Map[OapIndexType, Array[String]] =
    scala.collection.mutable.Map(
      BTreeIndexType -> Array("MINMAX", "SAMPLE", "BLOOM", "PARTBYVALUE"),
      BitMapIndexType -> Array.empty)

  def read(fiberCache: FiberCache, offset: Int, s: StructType): Array[StatisticsReader] = {
    var readOffset = 0
    val mask = fiberCache.getLong(offset + readOffset)
    readOffset += 8
    if (mask != StatisticsManager.STATISTICSMASK) {
      Array.empty[StatisticsReader]
    } else {
      val numOfStats = fiberCache.getInt(offset + readOffset)
      readOffset += 4
      val statsArray = new Array[StatisticsReader](numOfStats)
      for (i <- 0 until numOfStats) {
        statsArray(i) = fiberCache.getInt(offset + readOffset) match {
          case StatisticsType(stat) => stat(s)
          case _ => throw new UnsupportedOperationException("unsupport statistics id")
        }
        readOffset += 4
      }
      statsArray
    }.map { stat =>
      readOffset += stat.read(fiberCache, offset + readOffset)
      stat
    }
  }

  def analyse(
      stats: Array[StatisticsReader],
      intervalArray: ArrayBuffer[RangeInterval],
      conf: Configuration): StatsAnalysisResult = {
    val fullScanThreshold = conf.getDouble(
      OapConf.OAP_FULL_SCAN_THRESHOLD.key, OapConf.OAP_FULL_SCAN_THRESHOLD.defaultValue.get)
    val analysisResults = stats.map(_.analyse(intervalArray))

    if (analysisResults.exists(_ == StatsAnalysisResult.SKIP_INDEX)) {
      StatsAnalysisResult.SKIP_INDEX
    } else if (analysisResults.isEmpty ||
      analysisResults.map(_.coverage).sum / analysisResults.length <= fullScanThreshold) {
      StatsAnalysisResult.USE_INDEX
    } else {
      StatsAnalysisResult.FULL_SCAN
    }
  }
}
