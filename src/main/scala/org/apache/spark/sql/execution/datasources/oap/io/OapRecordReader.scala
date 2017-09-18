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

import org.apache.spark.sql.catalyst.expressions.SortDirection
import org.apache.spark.sql.execution.datasources.oap.{ColumnValues, IndexMeta, Key}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructField


// OapRecordReader is the Interface for FileScan. Including:
// 1. Index selection based on cost
// 2. Get Row ID List from IndexRecordReader based on Filter
// 3. Check if IndexRecordReader is covered all required columns
// 4. Get row record from DataRecordReader based on RowID list if there are any
// 5. Use DataRecordReader to do full scan if there is no optimization.
class OapRecordReader(
  availableIndices: Seq[IndexMeta],
  dataName: String,
  filters: Seq[Filter],
  direction: SortDirection,
  scanNum: Int,
  required: Seq[StructField]) {

  private var indexRecordReader: OapIndexRecordReader = _
  private val dataRecordReader: OapDataRecordReader = _

  def initialize(): Unit = {
    indexRecordReader = getBestIndex
  }

  def nextKeyValue(): Boolean = {
    if (indexRecordReader != null) {
      indexRecordReader.nextRowId()
      if (!isCoveredByIndex) {
        dataRecordReader.nextKeyValue(indexRecordReader.getCurrentRowId._2)
      }
    }
    else dataRecordReader.nextKeyValue()
  }

  def getCurrentValue: Key = dataRecordReader.getCurrentValue

  def isCoveredByIndex: Boolean = false

  private def getBestIndex: OapIndexRecordReader = {

    availableIndices.map { meta =>
      new OapIndexRecordReader(meta.name, filters, direction, scanNum)
    }.sortBy(_.getCost).head
  }
}
