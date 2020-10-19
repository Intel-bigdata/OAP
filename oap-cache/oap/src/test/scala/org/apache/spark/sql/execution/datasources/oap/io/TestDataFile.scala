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

import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, FiberId}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

private[oap] case class TestDataFile(path: String, schema: StructType, configuration: Configuration)
  extends DataFile {

  override def iterator(
      requiredIds: Array[Int],
      filters: Seq[Filter]): OapCompletionIterator[Any] =
    new OapCompletionIterator(Iterator.empty, {})

  override def iteratorWithRowIds(
      requiredIds: Array[Int],
      rowIds: Array[Int],
      filters: Seq[Filter]):
  OapCompletionIterator[Any] = new OapCompletionIterator(Iterator.empty, {})

  override def totalRows(): Long = 0

  override def getDataFileMeta(): DataFileMeta =
    throw new UnsupportedOperationException

  override def cache(groupId: Int, fiberId: Int, fiber: FiberId = null): FiberCache =
    throw new UnsupportedOperationException
}
