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

package org.apache.spark.sql.execution.datasources.oap.index

import java.io.OutputStream

import org.apache.hadoop.fs.{FSDataInputStream, Path}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile


/**
 * Utils for Index read/write
 */
private[oap] object IndexUtils {

  def serializeVersion(versionNum: Int): Array[Byte] = {
    assert(versionNum <= 65535)
    IndexFile.VERSION_PREFIX.getBytes("UTF-8") ++
      Array((versionNum >> 8).toByte, (versionNum & 0xFF).toByte)
  }

  def deserializeVersion(bytes: Array[Byte]): Option[Int] = {
    val prefix = IndexFile.VERSION_PREFIX.getBytes("UTF-8")
    val versionPos = bytes.length - 2
    assert(bytes.length == prefix.length + 2)
    if (bytes.slice(0, prefix.length) sameElements prefix) {
      val version = bytes(versionPos) << 8 | bytes(versionPos + 1)
      Some(version)
    } else {
      None
    }
  }

  def writeHead(writer: OutputStream, versionNum: Int): Int = {
    val versionData = serializeVersion(versionNum)
    assert(versionData.length == IndexFile.VERSION_LENGTH)
    writer.write(versionData)
    IndexFile.VERSION_LENGTH
  }

  def indexFileFromDataFile(dataFile: Path, name: String, time: String): Path = {
    import OapFileFormat._
    val dataFileName = dataFile.getName
    val pos = dataFileName.lastIndexOf(".")
    val indexFileName = if (pos > 0) {
      dataFileName.substring(0, pos)
    } else {
      dataFileName
    }
    new Path(
      dataFile.getParent, "." + indexFileName + "." + time + "." +  name + OAP_INDEX_EXTENSION)
  }

  def writeFloat(out: OutputStream, v: Float): Unit =
    writeInt(out, java.lang.Float.floatToIntBits(v))

  def writeDouble(out: OutputStream, v: Double): Unit =
    writeLong(out, java.lang.Double.doubleToLongBits(v))

  def writeBoolean(out: OutputStream, v: Boolean): Unit = out.write(if (v) 1 else 0)

  def writeByte(out: OutputStream, v: Int): Unit = out.write(v)

  def writeBytes(out: OutputStream, b: Array[Byte]): Unit = out.write(b)

  def writeShort(out: OutputStream, v: Int): Unit = {
    out.write(v >>> 0 & 0XFF)
    out.write(v >>> 8 & 0xFF)
  }

  def toBytes(v: Int): Array[Byte] = {
    Array(0, 8, 16, 24).map(shift => ((v >>> shift) & 0XFF).toByte)
  }

  def writeInt(out: OutputStream, v: Int): Unit = {
    out.write((v >>>  0) & 0xFF)
    out.write((v >>>  8) & 0xFF)
    out.write((v >>> 16) & 0xFF)
    out.write((v >>> 24) & 0xFF)
  }

  def writeLong(out: OutputStream, v: Long): Unit = {
    out.write((v >>>  0).toInt & 0xFF)
    out.write((v >>>  8).toInt & 0xFF)
    out.write((v >>> 16).toInt & 0xFF)
    out.write((v >>> 24).toInt & 0xFF)
    out.write((v >>> 32).toInt & 0xFF)
    out.write((v >>> 40).toInt & 0xFF)
    out.write((v >>> 48).toInt & 0xFF)
    out.write((v >>> 56).toInt & 0xFF)
  }

  /**
   * Note: outputPath comes from `FileOutputFormat.getOutputPath`, which is made by Data source
   * API, so `outputPath` should be simple enough, without scheme and authority.
   */
  def getIndexWorkPath(
      inputFile: Path, outputPath: Path, attemptPath: Path, indexFile: String): Path = {
    new Path(inputFile.getParent.toString.replace(
      outputPath.toString, attemptPath.toString), indexFile)
  }

  val INT_SIZE = 4
  val LONG_SIZE = 8

  /**
   * Constrain: keys.last >= candidate must be true. This is guaranteed
   * by [[BTreeIndexRecordReader.findNodeIdx]]
   * @return the first key >= candidate. (keys.last >= candidate makes this always possible)
   */
  def binarySearch(
      start: Int, length: Int,
      keys: Int => InternalRow, candidate: InternalRow,
      compare: (InternalRow, InternalRow) => Int): (Int, Boolean) = {
    var s = start
    var e = length - 1
    var found = false
    var m = s
    while (s <= e && !found) {
      assert(s + e >= 0, "too large array size caused overflow")
      m = (s + e) / 2
      val cmp = compare(candidate, keys(m))
      if (cmp == 0) {
        found = true
      } else if (cmp > 0) {
        s = m + 1
      } else {
        e = m - 1
      }
      if (!found) {
        m = s
      }
    }
    (m, found)
  }

  def binarySearchForStart(
      start: Int, length: Int,
      keys: Int => InternalRow, candidate: InternalRow,
      compare: (InternalRow, InternalRow) => Int): (Int, Boolean) = {
    var s = start + 1
    var e = length - 1
    lazy val initCmp = compare(candidate, keys(0))
    if (length <= 0 || initCmp <= 0) {
      return (0, length > 0 && initCmp == 0)
    }
    var found = false
    var m = s
    while (s <= e && !found) {
      assert(s + e >= 0, "too large array size caused overflow")
      m = (s + e) / 2
      val cmp = compare(candidate, keys(m))
      val marginCmp = compare(candidate, keys(m - 1))
      if (cmp == 0 && marginCmp > 0) found = true
      else if (cmp > 0) s = m + 1
      else e = m - 1
    }
    if (!found) m = s
    (m, found)
  }

  def binarySearchForEnd(
      start: Int, length: Int,
      keys: Int => InternalRow, candidate: InternalRow,
      compare: (InternalRow, InternalRow) => Int): (Int, Boolean) = {
    var s = start
    var e = length - 2
    lazy val initCmp = compare(candidate, keys(length - 1))
    if (length <= 0 || compare(candidate, keys(0)) < 0) {
      (-1, false)
    } else if (initCmp > 0) {
      (length, false)
    } else if (initCmp == 0) {
      (length - 1, true)
    } else {
      var (m, found) = (s, false)
      while (s <= e && !found) {
        assert(s + e >= 0, "too large array size caused overflow")
        m = (s + e) / 2
        val cmp = compare(candidate, keys(m))
        val marginCmp = compare(candidate, keys(m + 1))
        if (cmp == 0 && marginCmp < 0) found = true
        else if (cmp < 0) e = m - 1
        else s = m + 1
      }
      if (!found) m = s
      (m, found)
    }
  }
}
