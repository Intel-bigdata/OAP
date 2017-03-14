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

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}

import org.apache.spark.sql.catalyst.InternalRow

abstract class Statistics extends Serializable {
  def read(in: FSDataInputStream): Unit
  def write(out: FSDataOutputStream): Unit
}

class MinMaxStatistics(var content: Array[InternalRow] = null,
                       var pathName: Array[String] = null) extends Statistics {
  //assert(content.length == 2 * pathName.length) // every data file has a min and a max

  override def read(in: FSDataInputStream): Unit = {
    val temp = in.readInt()
    assert(temp == 1)
    println("min max read")
  }

  override def write(out: FSDataOutputStream): Unit = {
    out.writeInt(1)
    println("min max write")
  }
}

class SampleBasedStatistics(var content: Array[Array[InternalRow]] = null,
                            var pathName: Array[String] = null) extends Statistics {
//  assert(content.length == pathName.length) // every data file reflects to a Array[InternalRow]

  override def read(in: FSDataInputStream): Unit = {
    val temp = in.readInt()
    assert(temp == 2)
    println("sample based read")
  }

  override def write(out: FSDataOutputStream): Unit = {
    out.writeInt(2)
    println("sample based write")
  }
}

object MinMaxStatistics {
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

  def buildLocalStatistics1(internalRows: Array[InternalRow]): StatisticsLocalResult = {
    StatisticsLocalResult(internalRows.take(10).map(_.copy()))
  }
  def fromLocalResult2(localResults: Array[StatisticsLocalResult],
                      fileNames: Array[String]): SampleBasedStatistics = {
    new SampleBasedStatistics(localResults.map(_.rows), fileNames)
  }
  def fromLocalResult1(localResults: Array[StatisticsLocalResult],
                       fileNames: Array[String]): MinMaxStatistics = {
    new MinMaxStatistics(localResults.head.rows, fileNames)
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
