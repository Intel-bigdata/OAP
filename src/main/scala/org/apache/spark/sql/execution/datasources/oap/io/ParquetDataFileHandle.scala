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
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.format.converter.ParquetMetadataConverter._
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata

private[oap] class ParquetDataFileHandle(val footer: ParquetMetadata) extends DataFileHandle {

  require(footer != null, "footer of ParquetDataFileHandle should not be null.")

  def this(conf: Configuration, path: String) {
    this(ParquetFileReader.readFooter(conf, new Path(StringUtils.unEscapeString(path)), NO_FILTER))
  }

  override def fin: FSDataInputStream = null

  override def len: Long = 0

  override def getGroupCount: Int = footer.getBlocks.size()

  override def getFieldCount: Int =
    footer.getFileMetaData.getSchema.getColumns.size()

}
