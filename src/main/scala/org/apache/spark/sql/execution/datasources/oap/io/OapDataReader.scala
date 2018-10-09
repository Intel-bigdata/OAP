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

package org.apache.spark.sql.execution.datasources.oap.io

import org.apache.hadoop.fs.FSDataInputStream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OapException, PartitionedFile}
import org.apache.spark.sql.execution.datasources.oap.INDEX_STAT._
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileProperties.DataFileVersion
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileProperties.DataFileVersion.DataFileVersion
import org.apache.spark.unsafe.types.UTF8String

abstract class OapDataReader {
  def read(file: PartitionedFile): Iterator[InternalRow]

  // The two following fields have to be defined by certain versions of OapDataReader for use in
  // [[OapMetricsManager]]
  def rowsReadByIndex: Option[Long]
  def indexStat: INDEX_STAT
}

object OapDataReader extends Logging {

  def readVersion(is: FSDataInputStream, fileLen: Long): DataFileVersion = {
    val MAGIC_VERSION_LENGTH = 4
    val metaEnd = fileLen - 4

    // seek to the position of data file meta length
    is.seek(metaEnd)
    val metaLength = is.readInt()
    // read all bytes of data file meta
    val magicBuffer = new Array[Byte](MAGIC_VERSION_LENGTH)
    is.readFully(metaEnd - metaLength, magicBuffer)

    val magic = UTF8String.fromBytes(magicBuffer).toString
    magic match {
      case m if ! m.contains("OAP") => throw new OapException("Not a valid Oap Data File")
      case m if m == "OAP1" => DataFileVersion.OAP_DATAFILE_V1
      case _ => throw new OapException("Not a supported Oap Data File version")
    }
  }

  def getDataFileClassFor(dataReaderClassFromDataSourceMeta: String, reader: OapDataReader): String
    = {
    dataReaderClassFromDataSourceMeta match {
      case c if c == OapFileFormat.PARQUET_DATA_FILE_CLASSNAME => c
      case c if c == OapFileFormat.ORC_DATA_FILE_CLASSNAME => c
      case c if c == OapFileFormat.OAP_DATA_FILE_CLASSNAME =>
        reader match {
          case r: OapDataReaderV1 => OapFileFormat.OAP_DATA_FILE_V1_CLASSNAME
          case _ => throw new OapException(s"Undefined connection for $reader")
        }
      case _ => throw new OapException(
        s"Undefined data reader class name $dataReaderClassFromDataSourceMeta")
    }
  }
}
