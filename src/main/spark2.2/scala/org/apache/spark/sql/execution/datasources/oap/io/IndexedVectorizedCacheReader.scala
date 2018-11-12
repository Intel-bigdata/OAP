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

import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.{IndexedBlockMetaData, ParquetMetadata}
import org.apache.parquet.it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}

class IndexedVectorizedCacheReader(
    configuration: Configuration,
    footer: ParquetMetadata,
    dataFile: ParquetDataFile,
    requiredColumnIds: Array[Int]) extends VectorizedCacheReader(
  configuration,
  footer,
  dataFile,
  requiredColumnIds) {

  // pageNumber -> rowIdsList, use to decide：
  // 1. Is this pageNumber has data to read ?
  // 2. Use rowIdsList to mark which row need read.
  // TODO use scala map
  private val idsMap: JMap[Integer, IntList] = new JHashMap[Integer, IntList]()
  // Record current PageNumber
  private var currentPageNumber: Int = 0
  // for returnColumnarBatch is false branch,
  // secondary indexes to call columnarBatch.getRow
  private var batchIds: IntList = _

  private val IDS_MAP_STATE_ERROR_MSG: String =
    "The divideRowIdsIntoPages method should not be called when idsMap is not empty."
  private val IDS_ITER_STATE_ERROR_MSG: String =
    "The divideRowIdsIntoPages method should not be called when currentIndexList is Empty."

  override def getCurrentValue: AnyRef = {
    if (returnColumnarBatch) return columnarBatch
    assert(batchIds != null, "returnColumnarBatch = false, batchIds must not null.")
    assert(batchIdx <= numBatched, "batchIdx can not be more than numBatched")
    assert(batchIdx >= 1, "call nextKeyValue before getCurrentValue")
    // batchIds (IntArrayList) is random access.
    columnarBatch.getRow(batchIds.get(batchIdx - 1))
  }


  override def nextBatch: Boolean = {
    // if idsMap is Empty, needn't read remaining data in this row group
    // rowsReturned = totalCountLoadedSoFar to skip remaining data
    if (idsMap.isEmpty) {
      rowsReturned = totalCountLoadedSoFar
    }

    if (rowsReturned >= totalRowCount) {
      return false
    }

    checkEndOfRowGroup()

    var ids = idsMap.remove(currentPageNumber)
    currentPageNumber += 1

    while (ids == null || ids.isEmpty) {
      skipBatchInternal()
      ids = idsMap.remove(currentPageNumber)
      currentPageNumber += 1
    }

    nextBatchInternal()
    if (!returnColumnarBatch) {
      batchIds = ids
      numBatched = ids.size
    }
    true
  }

  override protected def readNextRowGroup(): Unit = {
    super.readNextRowGroup()
    val rowIds = currentRowGroup.asInstanceOf[IndexedBlockMetaData].getNeedRowIds
    // TODO add fine-grained fetch
    divideRowIdsIntoPages(rowIds)
  }

  private def divideRowIdsIntoPages(currentIndexList: IntList): Unit = {
    assert(idsMap.isEmpty, IDS_MAP_STATE_ERROR_MSG)
    assert(!currentIndexList.isEmpty, IDS_ITER_STATE_ERROR_MSG)
    this.currentPageNumber = 0
    val pageSize = columnarBatch.capacity
    val iterator = currentIndexList.iterator()
    while (iterator.hasNext) {
      val rowId = iterator.nextInt()
      val pageNumber = rowId / pageSize
      if (idsMap.containsKey(pageNumber)) {
        idsMap.get(pageNumber).add(rowId - pageNumber * pageSize)
      }
      else {
        val ids = new IntArrayList(pageSize / 2)
        ids.add(rowId - pageNumber * pageSize)
        idsMap.put(pageNumber, ids)
      }
    }
  }

  protected def skipBatchInternal(): Unit = {
    val num = columnarBatch.capacity
    rowsReturned += num
    numBatched = num
    batchIdx = 0
    currentRowGroupRowsReturned += num
  }
}
