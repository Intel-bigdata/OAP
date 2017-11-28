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

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.parquet.column.Dictionary

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


abstract class DataFile {
  def path: String
  def schema: StructType
  def configuration: Configuration

  def createDataFileHandle(): DataFileHandle
  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): FiberCache
  def iterator(conf: Configuration, requiredIds: Array[Int]): Iterator[InternalRow]
  def iterator(conf: Configuration, requiredIds: Array[Int], rowIds: Array[Long])
  : Iterator[InternalRow]
  def getDictionary(fiberId: Int, conf: Configuration): Dictionary
}

private[oap] object DataFile {
  def apply(path: String, schema: StructType, dataFileClassName: String,
            configuration: Configuration): DataFile = {
    Try(Utils.classForName(dataFileClassName).getDeclaredConstructor(
      classOf[String], classOf[StructType], classOf[Configuration])).toOption match {
      case Some(ctor) =>
        Try (ctor.newInstance(path, schema, configuration).asInstanceOf[DataFile]) match {
          case Success(e) => e
          case Failure(e) =>
            throw new OapException(s"Cannot instantiate class $dataFileClassName", e)
        }
      case None => throw new OapException(
        s"Cannot find constructor of signature like:" +
          s" (String, StructType) for class $dataFileClassName")
    }
  }
}

/**
 * The data file handle, will be cached for performance purpose, as we don't want to open the
 * specified file again and again to get its data meta, the data file extension can have its own
 * implementation.
 */
abstract class DataFileHandle {
  def fin: FSDataInputStream
  def len: Long

  def close(): Unit = {
    if (fin != null) {
      fin.close()
    }
  }
}
