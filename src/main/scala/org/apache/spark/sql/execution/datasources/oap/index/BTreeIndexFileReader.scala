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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile


trait BTreeIndexFileReader extends Logging
object BTreeIndexFileReader {

  def apply(configuration: Configuration, file: Path): BTreeIndexFileReader = {
    val fs = file.getFileSystem(configuration)
    val reader = fs.open(file)
    val fileLen = fs.getFileStatus(file).getLen

    val magic = new Array[Byte](IndexFile.VERSION_LENGTH)
    reader.readFully(0, magic)

    IndexUtils.deserializeVersion(magic) match {
      case Some(version) =>
        IndexVersion.fromId(version) match {
          case IndexVersion.OAP_INDEX_V1 =>
            BTreeIndexFileReaderV1(configuration, reader, file, fileLen)
        }
      case None =>
        throw new OapException("not a valid index file")
    }
  }
}
