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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index.{IndexUtils, RangeInterval}
import org.apache.spark.sql.types.StructType

private[oap] case class Sample(key: Key, nEq: Int, nLt: Int, nDlt: Int, isPeriodic: Boolean) {

}

private[oap] class SampleStatisticsReader(schema: StructType) extends StatisticsReader(schema)  {

  require(schema.length == 1)

  override val id: Int = StatisticsType.TYPE_SAMPLE
  protected var sampleArray: Array[Sample] = _
  protected var rowCount: Int = 0
  protected var avgEq: Int = 0
  override def read(fiberCache: FiberCache, offset: Int): Int = {
    var readOffset = super.read(fiberCache, offset) + offset

    rowCount = fiberCache.getInt(readOffset)
    val size = fiberCache.getInt(readOffset + 4)

    sampleArray = new Array[Sample](size)
    readOffset += 8
    var rowOffset = 0
    for (i <- 0 until size) {
      val start = readOffset + size * IndexUtils.INT_SIZE + rowOffset
      val (key, length) = nnkr.readKey(
        fiberCache, start)
      val nEq = fiberCache.getInt(start + length)
      val nLt = fiberCache.getInt(start + length + IndexUtils.INT_SIZE)
      val nDLt = fiberCache.getInt(start + length + IndexUtils.INT_SIZE * 2)
      sampleArray(i) = Sample(key, nEq, nLt, nDLt, isPeriodic = false)

      rowOffset = fiberCache.getInt(readOffset + i * IndexUtils.INT_SIZE)
    }
    avgEq = sampleArray.map(_.nEq).sum / sampleArray.length

    readOffset += (rowOffset + size * IndexUtils.INT_SIZE)
    readOffset - offset
  }

  private val ordering = GenerateOrdering.create(schema)

  // Return first greater or equal sample index, nLt, nEq for the key
  private def estimateKey(key: Key): (Int, Int, Int) = {
    var iSample = sampleArray.length
    var iMin = 0
    var iLower = 0
    var iUpper = 0
    var res = 0
    do {
      val iTest = (iMin + iSample) / 2
      res = ordering.compare(sampleArray(iTest).key, key)
      if (res < 0) {
        iLower = sampleArray(iTest).nLt + sampleArray(iTest).nEq
        iMin = iTest + 1
      } else {
        iSample = iTest
      }
    } while (res != 0 && iMin < iSample)

    if (res == 0) {
      (iSample, sampleArray(iSample).nLt, sampleArray(iSample).nEq)
    } else {
      if (iSample >= sampleArray.length) {
        iUpper = rowCount
      } else {
        iUpper = sampleArray(iSample).nLt
      }
      val iGap = if (iLower >= iUpper) 0 else iUpper - iLower
      (iSample, iLower + iGap / 3, avgEq)
    }
  }

  private def estimateLines(key: Key): Int = {
    val sampleOption = sampleArray.find(sample => ordering.compare(sample.key, key) >= 0)
    val index = sampleOption.map(sample => sampleArray.indexOf(sample)).getOrElse(-1)
    if (index == -1) {
      // Not find sample greater or equal `key`, so all records are smaller than key
      return sampleArray.last.nLt + sampleArray.last.nEq
    }

    val upperSample = sampleArray(index)
    val upperLines = upperSample.nLt

    val lowerSample = if (index == 0) upperSample else sampleArray(index - 1)
    val lowerLines = lowerSample.nLt

    lowerLines + (if (upperLines > lowerLines) (upperLines - lowerLines) / 3 else 0)
  }

  private def analyseInterval(interval: RangeInterval): Int = {
    if (ordering.compare(interval.start, interval.end) == 0
        && interval.startInclude && interval.endInclude) {
      // Equal
      val nEq = estimateKey(interval.start)._3
      nEq
    } else {
      // Range
      assert(ordering.compare(interval.start, interval.end) < 0)
      var iLower = 0
      var iUpper = rowCount
      val (iLowerIdx, iLowerLt, iLowerEq) = estimateKey(interval.start)
      val (iUpperIdx, iUpperLt, iUpperEq) = estimateKey(interval.end)
      if (iLower < iLowerLt + iLowerEq) iLower = iLowerLt + iLowerEq
      if (iUpper > iUpperLt + iLowerEq) iUpper = iUpperLt + iUpperEq
      if (iUpper > iLower) {
        iUpper - iLower
      } else {
        2
      }
    }
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): StatsAnalysisResult = {
    val hitCount = intervalArray.map(interval => analyseInterval(interval)).sum
    print("\t%4d".format(hitCount))
    val hitRate = hitCount.toDouble / rowCount
    if (hitRate < 0.01) {
      StatsAnalysisResult.USE_INDEX
    } else {
      StatsAnalysisResult.FULL_SCAN
    }
  }
}

private[oap] class SampleStatisticsWriter(
    schema: StructType,
    conf: Configuration) extends StatisticsWriter(schema, conf) {

  require(schema.length == 1)
  private val MIN_SAMPLE = 24

  override val id: Int = StatisticsType.TYPE_SAMPLE

  override def write(writer: OutputStream, sortedKeys: ArrayBuffer[Key]): Int = {

    // Step 1: row count
    val rowCount = sortedKeys.length

    // Step 2: Sample array length, least 24, or rowCount * 1%
    val mxSample = math.max(MIN_SAMPLE, rowCount / 100 + 1)

    // Step 3: periodic number
    val nPSample = rowCount / (mxSample / 3 + 1) + 1

    // Step 4: go through all keys, calculate nEq, nLt, nDlt, construct sample, insert
    implicit val ord: Ordering[Sample] = Ordering[(Boolean, Int)].on(s => (!s.isPeriodic, -s.nEq))
    val samples = new mutable.PriorityQueue[Sample]()
    var nEq = 0
    var nLt = 0
    var nDLt = 0
    var isPeriodic = false
    var prevKey: Key = null
    sortedKeys.indices.foreach { i =>
      if (i == 0) {
        nEq = 1
        nLt = 0
        nDLt = 0
        prevKey = sortedKeys.head
      } else {
        if (i % nPSample == 0) isPeriodic = true
        if (prevKey == sortedKeys(i)) {
          nEq += 1
        } else {
          samples.enqueue(Sample(prevKey, nEq, nLt, nDLt, isPeriodic = isPeriodic))
          if (samples.length > mxSample) samples.dequeue()
          nLt += nEq
          nEq = 1
          nDLt += 1
          isPeriodic = false
          prevKey = sortedKeys(i)
        }
      }
    }
    samples.enqueue(Sample(prevKey, nEq, nLt, nDLt, isPeriodic = isPeriodic))
    if (samples.length > mxSample) samples.dequeue()
    val ordering = GenerateOrdering.create(schema)
    val finalSamples = samples.dequeueAll.sortWith((l, r) => ordering.compare(l.key, r.key) < 0)

    var offset = super.write(writer, sortedKeys)
    IndexUtils.writeInt(writer, rowCount)
    IndexUtils.writeInt(writer, finalSamples.length)
    val tempWriter = new ByteArrayOutputStream()
    finalSamples.foreach { s =>
      nnkw.writeKey(tempWriter, s.key)
      IndexUtils.writeInt(tempWriter, s.nEq)
      IndexUtils.writeInt(tempWriter, s.nLt)
      IndexUtils.writeInt(tempWriter, s.nDlt)
      IndexUtils.writeInt(writer, tempWriter.size())
    }
    offset += samples.length * IndexUtils.INT_SIZE
    writer.write(tempWriter.toByteArray)
    offset += tempWriter.size()
    offset
  }
}
