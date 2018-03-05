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

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.IndexedParquetMetadata;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class IndexedVectorizedOapRecordReader extends VectorizedOapRecordReader {

    // pageNumber -> rowIdsList, use to decide：
    // 1. Is this pageNumber has data to read ?
    // 2. Use rowIdsList to mark which row need read.
    private Map<Integer, IntList> idsMap = Maps.newHashMap();
    // Record current PageNumber
    private int currentPageNumber;
    // Rowid list of file granularity
    private int[] globalRowIds;
    // Rowid Iter of RowGroup granularity
    private Iterator<IntList> rowIdsIter;
    // for returnColumnarBatch is false branch,
    // secondary indexes to call columnarBatch.getRow
    private IntList batchIds;

    private static final String IDS_MAP_STATE_ERROR_MSG =
      "The divideRowIdsIntoPages method should not be called when idsMap is not empty.";
    private static final String IDS_ITER_STATE_ERROR_MSG =
      "The divideRowIdsIntoPages method should not be called when rowIdsIter hasNext if false.";

    public IndexedVectorizedOapRecordReader(
        Path file,
        Configuration configuration,
        ParquetMetadata footer,
        int[] globalRowIds) {
      super(file, configuration, footer);
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
      if (this.footer == null) {
        footer = readFooter(configuration, file, NO_FILTER);
      }
      IndexedParquetMetadata indexedFooter = IndexedParquetMetadata.from(footer, globalRowIds);
      this.rowIdsIter = indexedFooter.getRowIdsList().iterator();

      // use indexedFooter read data, need't do filterRowGroups.
      initialize(footer, configuration, false);
      super.initializeInternal();
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
      if (returnColumnarBatch) {
        return columnarBatch;
      }
      Preconditions.checkNotNull(batchIds, "returnColumnarBatch = false, batchIds must not null.");
      Preconditions.checkArgument(
        batchIdx <= numBatched,
        "batchIdx can not be more than numBatched");
      Preconditions.checkArgument(batchIdx >= 1, "call nextKeyValue before getCurrentValue");
      // batchIds (IntArrayList) is random access.
      return columnarBatch.getRow(batchIds.get(batchIdx - 1));
    }

    /**
     * Advances to the next batch of rows. Returns false if there are no more.
     */
    @Override
    public boolean nextBatch() throws IOException {
      // if idsMap is Empty, needn't read remaining data in this row group
      // rowsReturned = totalCountLoadedSoFar to skip remaining data
      if (idsMap.isEmpty()) {
        rowsReturned = totalCountLoadedSoFar;
      }
      return super.nextBatch() && filterRowsWithIndex();
    }

    @Override
    protected void checkEndOfRowGroup() throws IOException {
      if (rowsReturned != totalCountLoadedSoFar) {
        return;
      }
      // if rowsReturned == totalCountLoadedSoFar
      // readNextRowGroup & divideRowIdsIntoPages
      super.readNextRowGroup();
      this.divideRowIdsIntoPages();
    }

    private boolean filterRowsWithIndex() throws IOException {
      IntList ids = idsMap.remove(currentPageNumber);
      if (ids == null || ids.isEmpty()) {
        currentPageNumber++;
        return this.nextBatch();
      } else {
        // if returnColumnarBatch, mark columnarBatch filtered status.
        // else assignment batchIdsIter.
        if (returnColumnarBatch) {
          // we can do same operation use markFiltered as follow code:
          // int current = 0;
          // for (Integer target : ids)
          // { while (current < target){
          // columnarBatch.markFiltered(current);
          // current++; }
          // current++; }
          // current++;
          // while (current < numBatched){
          // columnarBatch.markFiltered(current); current++; }
          // it a little complex and use current version,
          // we can revert use above code if need.
          columnarBatch.markAllFiltered();
            for (Integer rowId : ids) {
              columnarBatch.markValid(rowId);
            }
          } else {
            batchIds = ids;
            numBatched = ids.size();
          }
          currentPageNumber++;
          return true;
      }
    }

    private void divideRowIdsIntoPages() {
      Preconditions.checkState(idsMap.isEmpty(), IDS_MAP_STATE_ERROR_MSG);
      Preconditions.checkState(rowIdsIter.hasNext(), IDS_ITER_STATE_ERROR_MSG);
      this.currentPageNumber = 0;
      int pageSize = columnarBatch.capacity();
      IntList currentIndexList = rowIdsIter.next();
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
}
