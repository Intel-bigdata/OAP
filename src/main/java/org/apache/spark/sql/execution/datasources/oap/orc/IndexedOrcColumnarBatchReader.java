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

package org.apache.spark.sql.execution.datasources.oap.orc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * The OapIndexOrcColumnarBatchReader class has rowIds in order to scan the data
 * from the predefined row ids.
 */
public class IndexedOrcColumnarBatchReader extends OrcColumnarBatchReader {

  /* Below three fields are added by Oap index.
   * rowIds is assumed to be sorted in ascending order. The oap index will provide it.
   * curRowIndex is the index in rowIds array to directly jump to that row for efficient scanning.
   * rowLength is the length of rowIds array.
   */
  private int[] rowIds;

  private int curRowIndex;

  private int rowLength;

  public IndexedOrcColumnarBatchReader(boolean useOffHeap, boolean copyToSpark, int[] rowIds) {
    super(useOffHeap, copyToSpark);
    this.rowIds = rowIds;
  }

  /**
   * Initialize ORC file reader and batch record reader.
   * Please note that `initBatch` is needed to be called after this.
   * This method is customized by Oap.
   */
  @Override
  public void initialize(
      Path file, Configuration conf) throws IOException {
    super.initialize(file, conf);
    this.rowLength = this.rowIds.length;
    this.curRowIndex = 0;
    recordReader.seekToRow(rowIds[curRowIndex]);
  }

  /**
   * Return true if there exists more data in the next batch. If exists, prepare the next batch
   * by copying from ORC VectorizedRowBatch columns to Spark ColumnarBatch columns.
   */
  @Override
  public boolean nextBatch() throws IOException {
    if (curRowIndex >= rowLength) return false;
    recordReader.nextBatch(batch);
    int batchSize = batch.size;
    if (batchSize == 0) {
      return false;
    }
    int nextRowIndex = curRowIndex + 1;
    /* Orc readers support backward scan if the row Ids are out of order.
     * However, with the ascending ordered row Ids, the adjacent rows will
     * be scanned in the same batch. Below is expected that the row Ids are
     * ascending order.
     * Find the next row Id which is not in the same batch with the current row Id.
     */
    int curBatchMaxRowOffset = rowIds[curRowIndex] + batchSize;
    while (nextRowIndex < rowLength && curBatchMaxRowOffset >= rowIds[nextRowIndex]) {
      nextRowIndex++;
    }
    curRowIndex = nextRowIndex;
    // Prepare to jump to the row for the next batch if it's not adjacent to the previous one.
    // Use seekToRow if the next row index is four batch size greater than the current one
    // in order to avoid the overhead of over frequent seeking.
    if (curRowIndex < rowLength && rowIds[curRowIndex] > (curBatchMaxRowOffset + 4 * batchSize)) {
      recordReader.seekToRow(rowIds[curRowIndex]);
    }
    columnarBatch.setNumRows(batchSize);

    return readToColumnVectors(batchSize);
  }
}
