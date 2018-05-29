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

import java.io.Closeable
import java.lang.reflect.Constructor

import scala.util.{Failure, Success, Try}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

abstract class DataFile {
  def path: String
  def schema: StructType
  def configuration: Configuration

  def getDataFileMeta(): DataFileMeta
  def cache(groupId: Int, fiberId: Int): FiberCache
  def iterator(requiredIds: Array[Int], filters: Seq[Filter] = Nil): OapIterator[InternalRow]
  def iteratorWithRowIds(requiredIds: Array[Int], rowIds: Array[Int], filters: Seq[Filter] = Nil)
    : OapIterator[InternalRow]

  def totalRows(): Long
}

private[oap] class OapIterator[T](inner: Iterator[T]) extends Iterator[T] with Closeable {
  override def hasNext: Boolean = inner.hasNext
  override def next(): T = inner.next()
  override def close(): Unit = {}
}

private[oap] object DataFile {

  private val cache: LoadingCache[String, Constructor[_]] =
    CacheBuilder.newBuilder().build(new CacheLoader[String, Constructor[_]] {
      override def load(name: String): Constructor[_] =
        Utils.classForName(name).getDeclaredConstructor(
          classOf[String], classOf[StructType], classOf[Configuration])
  })

  def apply(
      path: String,
      schema: StructType,
      dataFileClassName: String,
      configuration: Configuration): DataFile = {
    Try(cache.get(dataFileClassName)).toOption match {
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

  private[oap] def cachedConstructorCount: Long = cache.size()
}

/**
 * VectorizedContext encapsulation infomation for Vectorized Read,
 * partitionColumns and partitionValues use by VectorizedOapRecordReader#initBatch
 * returningBatch use by VectorizedOapRecordReader#enableReturningBatches
 */
private[oap] case class VectorizedContext(
    partitionColumns: StructType,
    partitionValues: InternalRow,
    returningBatch: Boolean)

/**
 * The data file meta, will be cached for performance purpose, as we don't want to open the
 * specified file again and again to get its data meta, the data file extension can have its own
 * implementation.
 */
abstract class DataFileMeta {
  def fin: FSDataInputStream
  def len: Long

  def getGroupCount: Int
  def getFieldCount: Int

  def close(): Unit = {
    if (fin != null) {
      fin.close()
    }
  }
}
