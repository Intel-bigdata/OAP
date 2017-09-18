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
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.sources.Filter


// OapIndexRecordReader is an iterator to get [Key, RowID] pair.
class OapIndexRecordReader(
  name: String,
  filters: Seq[Filter],
  direction: SortDirection,
  scanNum: Int) {

  // Get Disk Data through OapIndexFileReader
  private val indexFileReader = new OapIndexFileReader()
  private var indexHeader: IndexHeader = _
  private var indexNode: IndexNode = _

  // For example: Read Index Header
  def initialize(): Unit = {
    indexHeader = indexFileReader.readIndexHeader()
  }

  // Similar to hasNext()
  def nextRowId(): Boolean = {
    checkEndOfFiber()
    if (indexNode != null) {
      // Some code to advance to next Row ID
      true
    } else {
      false
    }
  }

  // Similar to next(). Read one [Key, RowId] pair from Index Data Fiber.
  def getCurrentRowId: (Key, Long) = {
    // Some Code to get current Row ID
    null
  }

  // Calculate current cast for this index. Used for index selection
  def getCost: Double = 0

  // If Index Data Fiber is used up. Get next one from IndexRecordReader
  def checkEndOfFiber(): Unit = {}

}
