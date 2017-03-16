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

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

abstract class Statistics extends Serializable {
  // there is no need to need actually data file path when reading from metafile.
  def read(in: FSDataInputStream, schema: StructType, fullSize: Int): Unit

  def write(out: FSDataOutputStream, schema: StructType): Int
}

class MinMaxStatistics(var content: Array[InternalRow] = null,
                       var pathName: Array[String] = null) extends Statistics {
  //assert(content.length == 2 * pathName.length) // every data file has a min and a max

  private var schema: StructType = _
  @transient private lazy val converter = UnsafeProjection.create(schema)

  override def read(in: FSDataInputStream, schema: StructType, fullSize: Int): Unit = {
    if (content == null) return

    println("min max read")
    this.schema = schema

    val contentSize = in.readInt()
    assert(contentSize == content.length, "wrong content array.")

    val bytesArray = new Array[Byte](fullSize)

    val startPos = in.getPos
    in.readFully(startPos, bytesArray)
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

    fullSize
  }
}

class SampleBasedStatistics(var content: Array[Array[InternalRow]] = null,
                            var pathName: Array[String] = null) extends Statistics {
//  assert(content.length == pathName.length) // every data file reflects to a Array[InternalRow]

  private var schema: StructType = _

  override def read(in: FSDataInputStream, schema: StructType, fullSize: Int): Unit = {
    this.schema = schema

    val temp = in.readInt()
    assert(temp == 2)
    println("sample based read")
  }

  override def write(out: FSDataOutputStream, schema: StructType): Int = {
    out.writeInt(2)
    println("sample based write")
    2
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

  def buildLocalStatistics(internalRows: Array[InternalRow]): StatisticsLocalResult = {
    StatisticsLocalResult(internalRows.take(10).map(_.copy()))
  }
  def fromLocalResult(localResults: Array[StatisticsLocalResult],
          fileNames: Array[String]): MinMaxStatistics = {
    new MinMaxStatistics(localResults.head.rows, fileNames)
  }
}

object SampleBasedStatistics {
  def buildLocalStatistics(internalRows: Array[InternalRow]): StatisticsLocalResult = {
    StatisticsLocalResult(internalRows.take(10).map(_.copy()))
  }
  def fromLocalResult(localResults: Array[StatisticsLocalResult],
                      fileNames: Array[String]): SampleBasedStatistics = {
    new SampleBasedStatistics(localResults.map(_.rows), fileNames)
  }
}

case class StatisticsLocalResult(rows: Array[InternalRow])

object Statistics {
  val thresName = "spn_fsthreshold"

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

  def buildLocalStatistics1(internalRows: Array[InternalRow]): StatisticsLocalResult = {
    StatisticsLocalResult(internalRows.take(2).map(_.copy()))
  }
  def fromLocalResult2(localResults: Array[StatisticsLocalResult],
                      fileNames: Array[String]): SampleBasedStatistics = {
    new SampleBasedStatistics(localResults.map(_.rows), fileNames)
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

  def buildLocalStatstics(internalRows: Array[InternalRow],
                          stats_id: Int): StatisticsLocalResult = {
    stats_id match {
      case StatsMeta.MINMAX =>
//        MinMaxStatistics.buildLocalStatistics(internalRows)
        Statistics.buildLocalStatistics1(internalRows)
      case StatsMeta.SAMPLE =>
//        SampleBasedStatistics.buildLocalStatistics(internalRows)
        Statistics.buildLocalStatistics1(internalRows)
      case _ =>
        throw new Exception("unsupported statistics type")
    }
  }

  def fromLocalResult(localresults: Array[StatisticsLocalResult],
      fileNames: Array[String], stats_type: Int): Statistics = {
    stats_type match {
      case StatsMeta.MINMAX =>
//        MinMaxStatistics.fromLocalResult(localresults, fileNames)
        Statistics.fromLocalResult1(localresults, fileNames)
      case StatsMeta.SAMPLE =>
//        SampleBasedStatistics.fromLocalResult(localresults, fileNames)
        Statistics.fromLocalResult2(localresults, fileNames)
      case _ =>
        throw new Exception("unsupported statistics type")
    }

  }
}
