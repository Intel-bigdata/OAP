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
package org.apache.spark.sql.execution.datasources.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * This record reader has rowIds in order to seek to specific rows to skip unused data.
 * @param <V> the root type of the file
 */
public class OrcMapreduceRecordReaderForOapIndex<V extends WritableComparable>
    extends OrcMapreduceRecordReaderForOap<V> {

  // Below three fields are added by Oap index.
  private int[] rowIds;

  private int rowLength;

  private int curRowIndex;

  public OrcMapreduceRecordReaderForOapIndex(Path file, Configuration conf,
                                              int[] rowIds) throws IOException {
    super(file, conf);
    this.rowIds = rowIds;
    this.rowLength = rowIds.length;
    this.curRowIndex = 0;
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
      int i = curRowIndex + 1;
      while (i < rowLength && (rowIds[curRowIndex] + batchSize) >= rowIds[i]) {
        i++;
        if (i == rowLength) break;
      }
      curRowIndex = i;
      if (i < rowLength) {
        batchReader.seekToRow(rowIds[curRowIndex]);
      }
      return ret;
    }
    return true;
  }
}
