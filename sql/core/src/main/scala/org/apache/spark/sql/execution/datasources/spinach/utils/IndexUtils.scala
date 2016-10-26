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

package org.apache.spark.sql.execution.datasources.spinach.utils

import java.io.OutputStream

import org.apache.spark.sql.execution.datasources.spinach.SpinachFileFormat

/**
 * Utils for Index read/write
 */
object IndexUtils {
  def writeInt(out: OutputStream, v: Int): Unit = {
    out.write((v >>>  0) & 0xFF)
    out.write((v >>>  8) & 0xFF)
    out.write((v >>> 16) & 0xFF)
    out.write((v >>> 24) & 0xFF)
  }

  def indexFileNameFromDataFileName(dataFile: String, name: String): String = {
    import SpinachFileFormat._
    assert(dataFile.endsWith(SPINACH_DATA_EXTENSION))
    val simpleName = dataFile.substring(
      dataFile.lastIndexOf('/') + 1, dataFile.length - SPINACH_DATA_EXTENSION.length)
    val path = dataFile.substring(0, dataFile.lastIndexOf('/') + 1)

    path + "." + simpleName + "." + name + SPINACH_INDEX_EXTENSION
  }
}
