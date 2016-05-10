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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.util.SerializationUtil
import org.apache.spark.sql.types.StructType

private[spinach] object SpinachFileFormat {
  val SPINACH_DATA_EXTENSION = ".data"
  val SPINACH_META_EXTENSION = ".meta"
  val SPINACH_META_FILE = "spinach.meta"
  val SPINACH_META_SCHEMA = "spinach.schema"
  val SPINACH_REQUIRED_IDS = "spinach.required.columnids"
  val SPINACH_FILTER_SCANNER = "spinach.filter.scanner"

  def setRequiredColumnIds(
    conf: Configuration, schema: StructType, requiredColumns: Array[String]): Unit = {
    val requiredColumnIds = requiredColumns.map { name =>
      schema.fieldIndex(name)
    }.mkString(",")
    conf.set(SPINACH_REQUIRED_IDS, requiredColumnIds)
  }

  def getRequiredColumnIds(conf: Configuration): Array[Int] = {
    conf.get(SPINACH_REQUIRED_IDS).split(",").map(_.toInt)
  }

  def serializeFilterScanner(conf: Configuration, scanner: RangeScanner): Unit = {
    SerializationUtil.writeObjectToConfAsBase64(SPINACH_FILTER_SCANNER, scanner, conf)
  }

  def deserialzeFilterScanner(conf: Configuration): Option[RangeScanner] = {
    Option(SerializationUtil.readObjectFromConfAsBase64(SPINACH_FILTER_SCANNER, conf))
  }
}
