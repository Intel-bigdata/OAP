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
import org.apache.spark.sql.execution.datasources.{OapException, RecordReader}
import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, FiberId}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

abstract class DataFile {
  def path: String
  def schema: StructType
  def configuration: Configuration

  def iterator(requiredIds: Array[Int], filters: Seq[Filter] = Nil)
    : OapCompletionIterator[Any]
  def iteratorWithRowIds(requiredIds: Array[Int], rowIds: Array[Int], filters: Seq[Filter] = Nil)
    : OapCompletionIterator[Any]

  def totalRows(): Long

  def getDataFileMeta(): DataFileMeta
  // FIXME: the first 'fiberId' should be renamed as 'columnId'
  def cache(groupId: Int, fiberId: Int, fiber: FiberId = null): FiberCache
  override def hashCode(): Int = path.hashCode
  override def equals(other: Any): Boolean = other match {
    case df: DataFile => path.equals(df.path)
    case _ => false
  }
}

private[oap] class FileRecordReaderIterator[V](private[this] var rowReader: RecordReader[V])
  extends Iterator[V] with Closeable {

  private[this] var havePair = false
  private[this] var finished = false

  override def hasNext: Boolean = {
    if (!finished && !havePair) {
      finished = !rowReader.nextKeyValue
      if (finished) {
        close()
      }
      havePair = !finished
    }
    !finished
  }

  override def next(): V = {
    if (!hasNext) {
      throw new java.util.NoSuchElementException("End of stream")
    }
    havePair = false
    rowReader.getCurrentValue
  }

  override def close(): Unit = {
    if (rowReader != null) {
      try {
        rowReader.close()
      } finally {
        rowReader = null
      }
    }
  }
}

// An OAP wrapped iterator calling completionFunction() automatically after iterations of all items
// Also, it contains a close interface to do cleaning(extra listener is needed) if tasks fail
// Note that completionFunction & close are slightly different, the latter one contains something
// you do not wish to clean immediately after the iteration
private[oap] class OapCompletionIterator[T](inner: Iterator[T], completionFunction: => Unit)
    extends Iterator[T] with Closeable {

  private[this] var completed = false
  override def hasNext: Boolean = {
    val r = inner.hasNext
    if (!r && !completed) {
      completed = true
      completionFunction
    }
    r
  }
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
 * The abstract DataFileContext is used by OapFileFormat and OapDataReaderWriter for
 * both parquet and orc.
 *
 * ParquetVectorizedContext encapsulats information for parquet vectorization read,
 * partitionColumns and partitionValues use by VectorizedOapRecordReader#initBatch
 * returningBatch use by VectorizedOapRecordReader#enableReturningBatches
 *
 * OrcDataFileContext encapsulats information for orc readers.
 */
private[oap] abstract class DataFileContext {}

// Below is only used by parquet vectorized reader with and without oap index.
private[oap] case class ParquetVectorizedContext(
    partitionColumns: StructType,
    partitionValues: InternalRow,
    returningBatch: Boolean) extends DataFileContext

// Below is used by both orc vectorized readers and the orc map reduce readers.
// The orc map reduce readers are using dataSchema and requestedColIds in read method
// of OapDataReaderWriter.
private[oap] case class OrcDataFileContext(
    partitionColumns: StructType,
    partitionValues: InternalRow,
    returningBatch: Boolean,
    requiredSchema: StructType,
    dataSchema: StructType,
    enableOffHeapColumnVector: Boolean,
    copyToSpark: Boolean,
    requestedColIds: Array[Int])
    extends DataFileContext
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
