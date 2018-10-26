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
package org.apache.parquet.hadoop;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetFooter;
import org.apache.parquet.hadoop.OapParquetFileReader.RowGroupDataAndRowIds;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

import org.apache.spark.sql.oap.adapter.CapacityAdapter;

public class IndexedVectorizedOapRecordReader extends VectorizedOapRecordReader {

  // pageNumber -> rowIdsList, use to decide：
  // 1. Is this pageNumber has data to read ?
  // 2. Use rowIdsList to mark which row need read.
  private Map<Integer, IntList> idsMap = Maps.newHashMap();
  // Record current PageNumber
  private int currentPageNumber;
  // Rowid list of file granularity
  private int[] globalRowIds;
  // for returnColumnarBatch is false branch,
  // secondary indexes to call columnarBatch.getRow
  private IntList batchIds;

  private static final String IDS_MAP_STATE_ERROR_MSG =
    "The divideRowIdsIntoPages method should not be called when idsMap is not empty.";
  private static final String IDS_ITER_STATE_ERROR_MSG =
    "The divideRowIdsIntoPages method should not be called when currentIndexList is Empty.";

  public IndexedVectorizedOapRecordReader(
      Path file,
      Configuration configuration,
      ParquetFooter footer,
      int[] globalRowIds) {
    super(file, configuration, footer);
    this.globalRowIds = globalRowIds;
  }

  public IndexedVectorizedOapRecordReader(
      Path file,
      Configuration configuration,
      int[] globalRowIds) throws IOException{
    super(file, configuration);
    this.globalRowIds = globalRowIds;
  }

  /**
   * Override initialize method, init footer if need,
   * then init indexedFooter and rowIdsIter,
   * then call super.initialize and initializeInternal
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void initialize() throws IOException, InterruptedException {
    // use indexedFooter read data, need't do filterRowGroups.
    initialize(footer.toParquetMetadata(globalRowIds), configuration, false);
    super.initializeInternal();
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    if (returnColumnarBatch) {
      return columnarBatch;
    }
    Preconditions.checkNotNull(batchIds,
      "returnColumnarBatch = false, batchIds must not null.");
    Preconditions.checkArgument(
      batchIdx <= numBatched, "batchIdx can not be more than numBatched");
    Preconditions.checkArgument(batchIdx >= 1, "call nextKeyValue before getCurrentValue");
    // batchIds (IntArrayList) is random access.
    return columnarBatch.getRow(batchIds.get(batchIdx - 1));
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   *
   * The RowGroup data divide into ColumnarBatch may as following:
   * ---------------------------
   * | batch(0)   no index hit |
   * ---------------------------
   * | batch(1)   index hit    |
   * ---------------------------
   * | batch(2)   no index hit |
   * ---------------------------
   * | batch(3)   index hit    |
   * ---------------------------
   * | batch(4)   no index hit |
   * ---------------------------
   *
   * The sample data above will be read through call nextBatch() method 3 times:
   *
   * 1. The first time:
   *    1.1 `idsMap.isEmpty` is true, assignment `totalCountLoadedSoFar` to `rowsReturned`,
   *    now `rowsReturned` and `totalCountLoadedSoFar` both 0.
   *    1.2 `rowsReturned < totalRowCount` is false because totalRowCount init by footer, with
   *    sample data totalRowCount may eq (columnarBatch capacity * 4 + n)
   *    1.3 Call `checkEndOfRowGroup` method, it will read next row group data and divide
   *    row group level rowIds into columnarBatch level (pageNumber-> rowIds) pairs and put
   *    them into idsMap.
   *    1.4 Call `skipBatchInternal` because `idsMap.remove(0)` is null.
   *    1.5 Call `nextBatchInternal` and `filterRowsWithIndex(ids)` because `idsMap.remove(1)`
   *    not empty.
   *    1.6 return true, and now columnarBatch has been filled by batch(1)
   *
   * 2. The second time:
   *    2.1 `idsMap.isEmpty` is false
   *    2.2 `rowsReturned >= totalRowCount` is false
   *    2.3 `checkEndOfRowGroup` will jump out because `rowsReturned != totalCountLoadedSoFar`
   *    2.4 Call `skipBatchInternal` because `idsMap.remove(2)` is null.
   *    2.5 Call `nextBatchInternal` and `filterRowsWithIndex(ids)` because `idsMap.remove(3)`
   *    not empty.
   *    2.6 return true, and now columnarBatch has been filled by batch(3)
   *
   * 3. The third time:
   *    3.1 `idsMap.isEmpty` is true, assignment `totalCountLoadedSoFar` to `rowsReturned`,
   *    now `rowsReturned` and `totalCountLoadedSoFar` both eq `totalRowCount`
   *    3.2 return false because `rowsReturned >= totalRowCount` is true, and the batch(4) will
   *    discard directly.
   */
  @Override
  public boolean nextBatch() throws IOException {
    // if idsMap is Empty, needn't read remaining data in this row group
    // rowsReturned = totalCountLoadedSoFar to skip remaining data
    if (idsMap.isEmpty()) {
      rowsReturned = totalCountLoadedSoFar;
    }

    if (rowsReturned >= totalRowCount) {
      return false;
    }

    checkEndOfRowGroup();

    IntList ids = idsMap.remove(currentPageNumber++);

    // when we do this while loop there must be remainder pageNumber->ids in idsMap，
    // it guarantee by rowsReturned value and checkEndOfRowGroup method.
    while (ids == null || ids.isEmpty()) {
      skipBatchInternal();
      ids = idsMap.remove(currentPageNumber++);
    }

    nextBatchInternal();
    if (!returnColumnarBatch) {
      batchIds = ids;
      numBatched = ids.size();
    }
    return true;
  }

  private void divideRowIdsIntoPages(IntList currentIndexList) {
    Preconditions.checkState(idsMap.isEmpty(), IDS_MAP_STATE_ERROR_MSG);
    Preconditions.checkState(!currentIndexList.isEmpty(), IDS_ITER_STATE_ERROR_MSG);
    this.currentPageNumber = 0;
    int pageSize = CapacityAdapter.getCapacity(columnarBatch);
    for (int rowId : currentIndexList) {
      int pageNumber = rowId / pageSize;
      if (idsMap.containsKey(pageNumber)) {
        idsMap.get(pageNumber).add(rowId - pageNumber * pageSize);
      } else {
        IntArrayList ids = new IntArrayList(pageSize / 2);
        ids.add(rowId - pageNumber * pageSize);
        idsMap.put(pageNumber, ids);
      }
    }
  }

  @Override
  protected void readNextRowGroup() throws IOException {
    RowGroupDataAndRowIds rowGroupDataAndRowIds = reader.readNextRowGroupAndRowIds();
    initColumnReaders(rowGroupDataAndRowIds.getPageReadStore());
    divideRowIdsIntoPages(rowGroupDataAndRowIds.getRowIds());
  }

  /**
   * Do skipBatch for every ColumnVector in ColumnarBatch, actually when we call this method, we
   * will skip the whole columnarBatch, so this num is columnarBatch.capacity().
   */
  protected void skipBatchInternal() throws IOException {
    for (int i = 0; i < columnReaders.length; ++i) {
      if (columnReaders[i] == null) continue;
      columnReaders[i].skipBatch(CAPACITY, columnarBatch.column(i).dataType());
    }
    rowsReturned += CAPACITY;
    numBatched = CAPACITY;
    batchIdx = 0;
  }
}
