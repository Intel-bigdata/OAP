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
import scala.util.Random
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{BaseOrdering, GenerateOrdering}
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform


abstract class Statistics extends Serializable {
  // there is no need to need actually data file path when reading from metafile.
  def read(in: FSDataInputStream, schema: StructType, fullSize: Int): Unit

  def write(out: FSDataOutputStream, schema: StructType): Int

  def analyze(fileIndex: Int, ids: Array[Int], schema: StructType,
              intervalArray: Array[RangeInterval]): Double
}

class MinMaxStatistics(var content: Array[InternalRow] = null,
                       var pathName: Array[String] = null) extends Statistics {
  private var schema: StructType = _
  @transient private lazy val converter = UnsafeProjection.create(schema)

  override def read(in: FSDataInputStream, schema: StructType, fullSize: Int): Unit = {
    if (content == null) return

    println("min max read")
    this.schema = schema

    val contentSize = in.readInt()
    assert(contentSize == content.length, "wrong content array.")

    val bytesArray = new Array[Byte](fullSize - 4)

    val startPos = in.getPos
    in.readFully(startPos, bytesArray) // seek needed after readFully
    in.seek(startPos + bytesArray.length)
    var offset = 0

    for (i <- content.indices) {
      val internalRow = Statistics.getUnsafeRow(schema.length, bytesArray, offset).copy()
      offset += internalRow.getSizeInBytes + 4
      content(i) = internalRow
    }
  }

  override def write(out: FSDataOutputStream, schema: StructType): Int = {
    if (content == null) return 0
    println("min max write")
    this.schema = schema

    out.writeInt(content.length)
    out.flush()

    var fullSize = 0

    content.foreach{
      fullSize += Statistics.writeInternalRow(converter, _, out)
    }

    fullSize + 4
  }

  override def analyze(fileIndex: Int, ids: Array[Int], keySchema: StructType,
                       intervalArray: Array[RangeInterval]): Double = {
    val start = intervalArray.head
    val end = intervalArray.last

    val order = ids.zipWithIndex.map {
      case (index, i) =>
        SortOrder(BoundReference(index, schema(index).dataType, nullable = true), Ascending)
    }

    val ordering = GenerateOrdering.generate(order, keySchema.toAttributes)

    var result = false
    val min = content(2 * fileIndex)
    val max = content(2 * fileIndex + 1)

    if (start.start != RangeScanner.DUMMY_KEY_START) { // > or >= start
      if (start.startInclude) {
        result |= ordering.gt(start.start, max)
      } else {
        result |= ordering.gteq(start.start, max)
      }
    }

    if (end.end != RangeScanner.DUMMY_KEY_END) {
      if (end.endInclude) {
        result |= ordering.lt(end.end, min)
      } else {
        result |= ordering.lteq(end.end, min)
      }
    }

    if (result) StaticsAnalysisResult.SKIP_INDEX
    else StaticsAnalysisResult.UNSURE
  }
}

class SampleBasedStatistics(var content: Array[Array[InternalRow]] = null,
                            var pathName: Array[String] = null) extends Statistics {
  private var schema: StructType = _
  @transient private lazy val converter = UnsafeProjection.create(schema)

  override def read(in: FSDataInputStream, schema: StructType, fullSize: Int): Unit = {
    this.schema = schema
    println("sample based read")

    val content_len = in.readInt()
    if (content == null) content = new Array[Array[InternalRow]](content_len)
    val contentSizes = new Array[Int](content_len)

    for (i <- 0 until content_len) {
      contentSizes(i) = in.readInt()
    }

    val bytesArray = new Array[Byte](fullSize - 4 - 4 * content_len)
    val start_offset = in.getPos
    in.readFully(start_offset, bytesArray)
    in.seek(start_offset + bytesArray.size) // seek needed after readFully
    var offset = 0L

    for (i <- content.indices) {
      val temp_array = new ArrayBuffer[InternalRow]

      for (_ <- 0 until contentSizes(i)) {
        val internalRow = Statistics.getUnsafeRow(schema.length, bytesArray, offset).copy()
        offset += internalRow.getSizeInBytes + 4
        temp_array += internalRow
      }
      content(i) = temp_array.toArray
    }

  }

  override def write(out: FSDataOutputStream, schema: StructType): Int = {
    if (content == null) return 0
    this.schema = schema
    var fullSize = 0
    println("sample based write")

    out.writeInt(content.length)
    // write out all content length
    content.foreach(rows => out.writeInt(rows.length))
    out.flush()
    fullSize += 4 + content.length * 4

    content.foreach(rows => {
//      out.writeInt(rows.length)
//      fullSize += 4
      rows.foreach{
        fullSize += Statistics.writeInternalRow(converter, _, out)
      }
    })
    fullSize
  }

  override def analyze(fileIndex: Int, ids: Array[Int], schema: StructType,
                       intervalArray: Array[RangeInterval]): Double = {
    assert(fileIndex < content.length, "fileIndex out of bound")
    val ordering = GenerateOrdering.create(schema)
    content(fileIndex).count(row =>
      Statistics.rowInIntervalArray(row, intervalArray, ordering)) / content(fileIndex).length
  }
}

object MinMaxStatistics {
  def apply(): MinMaxStatistics = new MinMaxStatistics()

  def apply(fileCount: Long, pathName: Array[String] = null): MinMaxStatistics = {
    val minMaxStatistics = new MinMaxStatistics()

    minMaxStatistics.content = new Array[InternalRow](fileCount.toInt * 2)
    minMaxStatistics.pathName = pathName

    minMaxStatistics
  }

  def fromLocalResult1(localResults: Array[StatisticsLocalResult],
                       fileNames: Array[String]): MinMaxStatistics = {
    val collectResults: Array[InternalRow] = new Array(2 * localResults.length)
    for (i <- localResults.indices) {
      collectResults(i * 2) = localResults(i).rows.head.copy()
      collectResults(i * 2 + 1) = localResults(i).rows.last.copy()
    }
    new MinMaxStatistics(collectResults, fileNames)
  }

  def buildLocalStatistics1(schema: StructType,
                            internalRows: Array[InternalRow]): StatisticsLocalResult = {
    // TODO here can be optimized
    val minAB = new ArrayBuffer[Any]()
    val maxAB = new ArrayBuffer[Any]()

    for (i <- 0 until schema.length) {
      val field = schema(i)
      var min = internalRows(0)
      var max = internalRows(0)

      val order = SortOrder(BoundReference(i, field.dataType, nullable = true), Ascending)

      val ordering = GenerateOrdering.generate(order :: Nil,
        StructType(field :: Nil).toAttributes)

      for (row <- internalRows) {
        min = if (ordering.compare(row, min) < 0) row else min
        max = if (ordering.compare(row, max) > 0) row else max
      }
      minAB += min.get(i, field.dataType)
      maxAB += max.get(i, field.dataType)
    }

    val minRow = InternalRow.fromSeq(minAB)
    val maxRow = InternalRow.fromSeq(maxAB)

    StatisticsLocalResult(Array(minRow, maxRow))
  }
}

object SampleBasedStatistics {
  def fromLocalResult2(localResults: Array[StatisticsLocalResult],
                       fileNames: Array[String]): SampleBasedStatistics = {
    new SampleBasedStatistics(localResults.map(_.rows), fileNames)
  }
  def buildLocalStatistics2(internalRows: Array[InternalRow],
                            sampleRate: Double): StatisticsLocalResult = {
    StatisticsLocalResult(
      Random.shuffle(internalRows.indices.toList)
        .take((internalRows.length * sampleRate).toInt)
        .map(internalRows(_)).toArray)
  }
}

case class StatisticsLocalResult(rows: Array[InternalRow])

object Statistics {
  val thresName = "spn_fsthreshold"
  val sampleRate = "spn_sampleRate"

  def getUnsafeRow(schemaLen: Int, array: Array[Byte], offset: Long): UnsafeRow = {
    val size = Platform.getInt(array, Platform.BYTE_ARRAY_OFFSET + offset)
    val row = UnsafeRangeNode.row.get
    row.setNumFields(schemaLen)
    row.pointTo(array, Platform.BYTE_ARRAY_OFFSET + offset + 4, size)
    row
  }

  /**
   * This method help spinach convert InternalRow type to UnsafeRow type
   *  @param internalRow
   *  @param keyBuf
   *  @return unsafeRow
   */
  def convertHelper(converter: UnsafeProjection, internalRow: InternalRow,
                    keyBuf: ByteArrayOutputStream): UnsafeRow = {
    val writeRow = converter.apply(internalRow)
    IndexUtils.writeInt(keyBuf, writeRow.getSizeInBytes)
    writeRow
  }

  def writeInternalRow(converter: UnsafeProjection,
                       internalRow: InternalRow,
                       fileOut: FSDataOutputStream): Int = {
    val keyBuf = new ByteArrayOutputStream()
    val value = convertHelper(converter, internalRow, keyBuf)
    value.writeToStream(keyBuf, null)

    keyBuf.writeTo(fileOut)
    fileOut.flush()
    keyBuf.close()

    4 + value.getSizeInBytes
  }

  def buildLocalStatistics2(internalRows: Array[InternalRow],
                            sampleRate: Double): StatisticsLocalResult = {
    StatisticsLocalResult(
      Random.shuffle(internalRows.indices.toList)
      .take((internalRows.length * sampleRate).toInt)
      .map(internalRows(_)).toArray)
  }

  def fromLocalResult2(localResults: Array[StatisticsLocalResult],
                      fileNames: Array[String]): SampleBasedStatistics = {
    new SampleBasedStatistics(localResults.map(_.rows), fileNames)
  }

  def buildLocalStatstics(schema: StructType,
                          internalRows: Array[InternalRow],
                          stats_id: Int,
                          options: Map[String, String] = null): StatisticsLocalResult = {
    stats_id match {
      case StatsMeta.MINMAX =>
//        MinMaxStatistics.buildLocalStatistics(internalRows)
        MinMaxStatistics.buildLocalStatistics1(schema, internalRows)
//        Statistics.buildLocalStatistics1(schema, internalRows)
      case StatsMeta.SAMPLE =>
//        SampleBasedStatistics.buildLocalStatistics(internalRows)
        SampleBasedStatistics.buildLocalStatistics2(internalRows,
          options.getOrElse(Statistics.sampleRate, "0.8").toDouble)
      case _ =>
        throw new Exception("unsupported statistics type")
    }
  }

  def fromLocalResult(localresults: Array[StatisticsLocalResult],
      fileNames: Array[String], stats_type: Int): Statistics = {
    stats_type match {
      case StatsMeta.MINMAX =>
        MinMaxStatistics.fromLocalResult1(localresults, fileNames)
      case StatsMeta.SAMPLE =>
//        SampleBasedStatistics.fromLocalResult(localresults, fileNames)
        SampleBasedStatistics.fromLocalResult2(localresults, fileNames)
      case _ =>
        throw new Exception("unsupported statistics type")
    }
  }
  // logic is complex, needs to be refactored :(
  def rowInSingleInterval(row: InternalRow, interval: RangeInterval,
                          order: BaseOrdering): Boolean = {
    if (interval.start == RangeScanner.DUMMY_KEY_START) {
      if (interval.end == RangeScanner.DUMMY_KEY_END) true
      else {
        if (order.lt(row, interval.end)) true
        else if (order.equiv(row, interval.end) && interval.endInclude) true
        else false
      }
    } else {
      if (order.lt(row, interval.start)) false
      else if (order.equiv(row, interval.start)) {
        if (interval.startInclude) {
          if (interval.end != RangeScanner.DUMMY_KEY_END &&
            order.equiv(interval.start, interval.end) && !interval.endInclude) {false}
          else true
        } else interval.end == RangeScanner.DUMMY_KEY_END || order.gt(interval.end, row)
      }
      else if (interval.end != RangeScanner.DUMMY_KEY_END && (order.gt(row, interval.end) ||
        (order.equiv(row, interval.end) && !interval.endInclude))) {false}
      else true
    }
  }
  def rowInIntervalArray(row: InternalRow, intervalArray: Array[RangeInterval],
                         order: BaseOrdering): Boolean = {
    if (intervalArray == null || intervalArray.isEmpty) false
    else intervalArray.exists(interval => rowInSingleInterval(row, interval, order))
  }

}

object StaticsAnalysisResult {
  val FULL_SCAN = 1
  val SKIP_INDEX = -1
  val UNSURE = -2
  val USE_INDEX = 0
}