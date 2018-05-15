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

package org.apache.spark.sql.execution.datasources.oap.index.impl

import java.io.OutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.oap.index.IndexFileWriter

private[index] case class IndexFileWriterImpl(
    configuration: Configuration,
    indexPath: Path) extends IndexFileWriter {

  protected override val os: OutputStream =
    indexPath.getFileSystem(configuration).create(indexPath, true)

  // Give RecordWriter a chance which file it's writing to.
  override def getName: String = indexPath.toString

  override def tempRowIdWriter: IndexFileWriter = {
    val tempFileName = new Path(indexPath.getParent, indexPath.getName + ".id")
    IndexFileWriterImpl(configuration, tempFileName)
  }

  override def writeRowId(tempWriter: IndexFileWriter): Unit = {
    val path = new Path(tempWriter.getName)
    val is = path.getFileSystem(configuration).open(path)
    val length = path.getFileSystem(configuration).getFileStatus(path).getLen
    val bufSize = configuration.getInt("io.file.buffer.size", 4096)
    val bytes = new Array[Byte](bufSize)
    var remaining = length
    while (remaining > 0) {
      val readSize = math.min(bufSize, remaining).toInt
      is.readFully(bytes, 0, readSize)
      os.write(bytes, 0, readSize)
      remaining -= readSize
    }
    is.close()
    path.getFileSystem(configuration).delete(path, false)
  }
}
