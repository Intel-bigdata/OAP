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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.IndexMeta
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.index.impl.IndexFileReaderImpl
import org.apache.spark.sql.execution.datasources.oap.statistics.StatsAnalysisResult

private[oap] case class BitMapScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {

  private var _totalRows: Long = 0
  @transient private var bmRowIdIterator: Iterator[Int] = _
  override def hasNext: Boolean = bmRowIdIterator.hasNext
  override def next(): Int = bmRowIdIterator.next
  override def totalRows(): Long = _totalRows

  // TODO: If the index file is not changed, bypass the repetitive initialization for queries.
  override def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    // Currently OAP index type supports the column with one single field.
    assert(keySchema.fields.length == 1)
    val idxPath = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    val fileReader = IndexFileReaderImpl(conf, idxPath)

    val bitmapReader = IndexUtils.readVersion(fileReader) match {
      case Some(version) =>
        IndexVersion(version) match {
          case IndexVersion.OAP_INDEX_V1 =>
            val reader = new BitmapReaderV1(
              fileReader, intervalArray, internalLimit, keySchema, conf)
            reader.initRowIdIterator
            bmRowIdIterator = reader
            reader
          case IndexVersion.OAP_INDEX_V2 =>
            val reader = new BitmapReaderV2(
              fileReader, intervalArray, internalLimit, keySchema, conf)
            reader.initRowIdIterator
            bmRowIdIterator = reader
            reader
        }
      case None =>
        throw new OapException("not a valid index file")
    }
    _totalRows = bitmapReader.totalRows
    fileReader.close()
    this
  }

  override protected def analyzeStatistics(
      idxPath: Path,
      conf: Configuration): StatsAnalysisResult = {
    val fileReader = IndexFileReaderImpl(conf, idxPath)
    val reader = BitmapReader(fileReader, intervalArray, keySchema, conf)
    _totalRows = reader.totalRows
    try {
      reader.analyzeStatistics()
    } finally {
      fileReader.close()
    }
  }

  override def toString: String = "BitMapScanner"

}
