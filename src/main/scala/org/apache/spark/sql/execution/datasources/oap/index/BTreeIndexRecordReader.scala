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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.index.impl.IndexFileReaderImpl
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatsAnalysisResult
import org.apache.spark.sql.types._

private[index] trait BTreeIndexRecordReader extends Iterator[Int] {
  def initialize(path: Path, intervalArray: ArrayBuffer[RangeInterval]): Unit
  def analyzeStatistics(
      keySchema: StructType,
      intervalArray: ArrayBuffer[RangeInterval]): StatsAnalysisResult
  def totalRows(): Long
  def close(): Unit
}

private[index] object BTreeIndexRecordReader {
  def apply(
      configuration: Configuration,
      schema: StructType,
      indexPath: Path): BTreeIndexRecordReader = {

    val fileReader = IndexFileReaderImpl(configuration, indexPath)

    readVersion(fileReader) match {
      case Some(version) =>
        IndexVersion(version) match {
          case IndexVersion.OAP_INDEX_V1 =>
            BTreeIndexRecordReaderV1(configuration, schema, fileReader)
        }
      case None =>
        throw new OapException("not a valid index file")
    }
  }

  private def readVersion(fileReader: IndexFileReader): Option[Int] = {
    val magicBytes = fileReader.read(0, IndexFile.VERSION_LENGTH)
    IndexUtils.deserializeVersion(magicBytes)
  }
}
