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

import java.io.DataInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.util.StringUtils
import org.apache.spark.sql.types.StructType

case class SpinachMeta(schema: StructType) {
  def open(path: String): DataMeta = {
    throw new NotImplementedError("")
  }
}

private[spinach] object SpinachMeta {
  def initialize(path: Path, jobConf: Configuration): SpinachMeta = {
    // TODO
    // the meta file is simple as only save the schema in json string.
    val fileIn = new DataInputStream(path.getFileSystem(jobConf).open(path))
    new SpinachMeta(StructType.fromString(fileIn.readUTF()))
  }
}
