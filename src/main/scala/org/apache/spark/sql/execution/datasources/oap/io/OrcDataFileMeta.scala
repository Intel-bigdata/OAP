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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile
import org.apache.orc.Reader

private[oap] class OrcDataFileMeta(val path: Path, val configuration: Configuration)
    extends DataFileMeta {

  private val fs = path.getFileSystem(configuration)
  private val readerOptions = OrcFile.readerOptions(configuration).filesystem(fs)
  private val fileReader = OrcFile.createReader(path, readerOptions)

  def getOrcFileReader(): Reader = fileReader

  override def len: Long = fileReader.getContentLength()
  override def getGroupCount: Int = fileReader.getStripes().size()
  override def getFieldCount: Int = fileReader.getSchema().getFieldNames().size()
  // Not used by orc data file.
  override def fin: FSDataInputStream = null
}
