/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.oap.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * This record reader has rowIds in order to seek to specific rows to skip unused data.
 * @param <V> the root type of the file
 */
public class IndexedOrcMapreduceRecordReader<V extends WritableComparable>
    extends OrcMapreduceRecordReader<V> {

  // Below four fields are added by Oap index.
  private int[] rowIds;

  private int rowLength;

  private int curRowIndex;

  private int preRowIndex;

  public IndexedOrcMapreduceRecordReader(Path file, Configuration conf,
      int[] rowIds) throws IOException {
    super(file, conf);
    this.rowIds = rowIds;
    this.rowLength = rowIds.length;
    this.curRowIndex = 0;
    this.preRowIndex = 0;
    batchReader.seekToRow(rowIds[curRowIndex]);
  }

  /**
   * If the current batch is empty, get a new one.
   * @return true if we have rows available.
   * @throws IOException
   */
  @Override
  boolean ensureBatch() throws IOException {
    if (rowInBatch >= batch.size) {
      rowInBatch = 0;
      if (curRowIndex >= rowLength) return false;
      boolean ret = batchReader.nextBatch(batch);
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
      preRowIndex = curRowIndex;
      int curBatchMaxRowOffset = rowIds[curRowIndex] + batchSize;
      while (nextRowIndex < rowLength && curBatchMaxRowOffset >= rowIds[nextRowIndex]) {
        nextRowIndex++;
      }
      curRowIndex = nextRowIndex;
      // Prepare to jump to the row for the next batch.
      // Use seekToRow if the next row index is four batch size greater than the current one
      // in order to avoid the overhead of over frequent seeking.
      if (curRowIndex < rowLength && rowIds[curRowIndex] > (curBatchMaxRowOffset + 4 * batchSize)) {
        batchReader.seekToRow(rowIds[curRowIndex]);
      }
      return ret;
    }
    return true;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!ensureBatch()) {
      return false;
    }
    // The first row in this current batch is definitely in the row Ids, because
    // it's the just seeking row.
    readNextRow();
    rowInBatch += 1;
    // Skip the rows in the current batch which is not in the row Ids.
    // preRowIndex is the starting row in the current batch.
    // Then jump to the next row which matches the next row Id.
    while (rowInBatch < batch.size && (preRowIndex + 1) < rowLength &&
      (rowInBatch + rowIds[preRowIndex]) < rowIds[preRowIndex + 1]) {
      rowInBatch += 1;
    }
    if ((preRowIndex + 1) < rowLength) {
      preRowIndex += 1;
    }
    return true;
  }
}
