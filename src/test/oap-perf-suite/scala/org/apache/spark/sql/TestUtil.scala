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
package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}

object TestUtil {
  val conf = new Configuration()
  val oapDataFilter = new PathFilter {
    override def accept(path: Path) = path.getName.endsWith(".data")
  }
  val parquetDataFilter = new PathFilter {
    override def accept(path: Path) = path.getName.endsWith(".parquet")
  }


  def convertFileSize(size: Long): String = {
    val kb: Long = 1024
    val mb: Long = kb * 1024
    val gb: Long = mb * 1024
    val tb: Long = gb * 1024

    if (size >= tb) "%.1f TB".format(size.toFloat / tb)
    else if (size >= gb) {
      val f = size.toFloat / gb
      (if (f > 100) "%.0f GB" else "%.1f GB").format(f)
    } else if (size >= mb) {
      val f = size.toFloat / mb
      (if (f > 100) "%.0f MB" else "%.1f MB").format(f)
    } else if (size >= kb) {
      val f = size.toFloat / kb
      (if (f > 100) "%.0f KB" else "%.1f KB").format(f)
    } else "%d B".format(size)
  }

  def calculateIndexSize(tableName: String, tablePath: String, attr: String): String = {
    val path = new Path(tablePath + tableName)
    val indexFilter = new PathFilter {
      override def accept(path: Path) = path.getName.endsWith(s"${attr}_index.index")
    }
    val size = path.getFileSystem(conf).listStatus(path, indexFilter).map(_.getLen).sum
    convertFileSize(size)
  }
  def calculateFileSize(tableName: String, tablePath: String, format: String) : String = {
    val path = new Path(tablePath + tableName)
    val size = path.getFileSystem(conf).listStatus(path,
      if (format == "oap") oapDataFilter else parquetDataFilter).map(_.getLen).sum
    convertFileSize(size)
  }

  def calculateFileSize(path: Path, filter: PathFilter): String = {
    val size = path.getFileSystem(conf).listStatus(path, filter).map(_.getLen).sum
    convertFileSize(size)
  }

  def time[T](code: => T, action: String): Unit = {
    val t0 = System.nanoTime
    code
    val t1 = System.nanoTime
    println(action + ((t1 - t0) / 1000000) + "ms")
  }

  def queryTime[T](code: => T): Int = {
    val t0 = System.nanoTime
    code
    val t1 = System.nanoTime
    ((t1 - t0) / 1000000).toInt
  }

  def median(s: Seq[Int]): Int = {
    val sortSeq = s.sortWith(_ < _)
    if (sortSeq.length % 2 == 0) (sortSeq(sortSeq.length / 2 - 1) + sortSeq(sortSeq.length / 2)) / 2
    else sortSeq(sortSeq.length / 2)
  }

  def formatResults(resultSet: Seq[(String, Seq[(String, Array[Int])])]): Unit = {
    assert(resultSet.nonEmpty)

    resultSet.foreach{ result =>
      val header =
        Seq(("%" + Tabulator.MAX_WIDTH + "s").format(Tabulator.truncate(result._1))) ++
        (1 to result._2(0)._2.length).map("T" + _ +"/ms") ++
        Seq("Median/ms")
      val content = result._2.map(x =>
        Seq(Tabulator.truncate(x._1)) ++
          x._2.map(_.toString) ++
          Seq(median(x._2).toString)
      )
      println(Tabulator.format(Seq(header) ++ content))
    }
  }
}

// TODO: use DataSet.show()??
object Tabulator {
  val MAX_WIDTH = 64

  def truncate(value: String, length: Int = MAX_WIDTH): String = {
    assert(length > 3)
    if (value != null && value.length > length) value.substring(0, length - 3) + "..."
    else value
  }

  def format(table: Seq[Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield (for (cell <- row) yield
        if (cell == null) 0 else cell.toString.length)
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator ::
      rows.head ::
      rowSeparator ::
      rows.tail.toList :::
      rowSeparator ::
      List()).mkString("\n")

  def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = {
    val cells = for ((item, size) <- row.zip(colSizes)) yield
      if (size == 0) "" else ("%" + size + "s").format(item)
    cells.mkString("|", "|", "|")
  }

  private def rowSeparator(colSizes: Seq[Int]) = colSizes map { "-" * _ } mkString("+", "+", "+")
}
