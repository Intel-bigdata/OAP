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
package org.apache.parquet.io;

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

public class PositionableRecordReaderImpl<T> extends RecordReaderImplementation<T> {

    private final long recordMaxCount;

    private int recordsRead = 0;

    private IntList rowIdList;

    private int currentIndex = 0;

    public PositionableRecordReaderImpl(
        MessageColumnIO root,
        RecordMaterializer<T> recordMaterializer,
        ColumnReadStoreImpl columnStore,
        long recordCount,
        IntList rowIdList) {
      super(root, recordMaterializer, false, columnStore);
      Preconditions.checkNotNull(rowIdList, "rowIdList can not be null.");
      Preconditions.checkArgument(!rowIdList.isEmpty(), "rowIdList must has item.");
      this.recordMaxCount = recordCount;
      this.rowIdList = rowIdList;
    }

    public T read() {
      int currentRowId = rowIdList.getInt(currentIndex);
      seek(currentRowId);
      if (recordsRead == recordMaxCount) {
        return null;
      }
      ++recordsRead;
      ++currentIndex;
      return super.read();
    }

    private void seek(int position) {
      Preconditions.checkArgument(
        position >= recordsRead,
        "Not support seek to backward position, recordsRead: %s want to read: %s",
        recordsRead,
        position);
      Preconditions.checkArgument(
        position < recordMaxCount,
        "Seek position must less than recordCount");
      while (recordsRead < position) {
        State currentState = getState(0);
        do {
          ColumnReader columnReader = currentState.column;
          // has value, skip it
          if (columnReader.getCurrentDefinitionLevel() >= currentState.maxDefinitionLevel) {
            columnReader.skip();
          }
          // change r,d state
          columnReader.consume();
          int nextR =
            currentState.maxRepetitionLevel == 0 ? 0 : columnReader.getCurrentRepetitionLevel();
          currentState = currentState.getNextState(nextR);
        } while (currentState != null);
          recordsRead++;
      }
    }

}
