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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.IndexedParquetMetadata;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

public class IndexedVectorizedOapRecordReader extends VectorizedOapRecordReader {

    private PageContext pageContext;
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
      initialize(indexedFooter, configuration, false);
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

    @Override
    public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
      super.initBatch(partitionColumns, partitionValues);
      this.pageContext = new PageContext(columnarBatch.capacity());
    }

    @Override
    public void enableReturningBatches() {
      super.enableReturningBatches();
      this.pageContext.enableReturnColumnarBatch();
    }

    /**
     * Advances to the next batch of rows. Returns false if there are no more.
     */
    @Override
    public boolean nextBatch() throws IOException {
      // if idsMap is Empty, needn't read remaining data in this row group
      // rowsReturned = totalCountLoadedSoFar to skip remaining data
      if (pageContext.isEmpty()) {
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
      IntList ids = pageContext.remove(currentPageNumber);
      if (ids == null || ids.isEmpty()) {
        currentPageNumber++;
        return this.nextBatch();
      } else {
        if (!returnColumnarBatch) {
          batchIds = ids;
          numBatched = ids.size();
        }
        currentPageNumber++;
        return true;
      }
    }

    private void divideRowIdsIntoPages() {
      Preconditions.checkState(pageContext.isEmpty(), IDS_MAP_STATE_ERROR_MSG);
      Preconditions.checkState(rowIdsIter.hasNext(), IDS_ITER_STATE_ERROR_MSG);
      currentPageNumber = 0;
      pageContext.put(rowIdsIter.next());
    }
}
