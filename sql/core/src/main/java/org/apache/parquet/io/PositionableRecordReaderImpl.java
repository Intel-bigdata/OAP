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
import org.apache.parquet.io.RecordReaderImplementation.State;
import org.apache.parquet.utils.Reflections;

public abstract class PositionableRecordReaderImpl<T> implements PositionableRecordReader<T> {

    protected final long recordMaxCount;

    private long recordsRead = 0;

    private RecordReader<T> recordReader;

    private State[] states;

    protected Long currentRowId = -1L;

    public PositionableRecordReaderImpl(RecordReader<T> recordReader, long recordCount) {
        this.recordReader = recordReader;
        this.recordMaxCount = recordCount;
        if(recordReader instanceof RecordReaderImplementation){
            this.states = (State[]) Reflections.getFieldValue(recordReader, "states");
        }

    }

    public T read() {
        if(this.states != null){
            currentRowId = this.nextRowId();
            seek(currentRowId);
        }

        if (recordsRead == recordMaxCount) {
            return null;
        }

        ++recordsRead;
        return recordReader.read();
    }

    private void seek(long position) {

        Preconditions.checkArgument(position >= recordsRead,
                "Not support seek to backward position, recordsRead: %s want to read: %s", recordsRead, position);
        Preconditions.checkArgument(position < recordMaxCount, "Seek position must less than recordCount");

        while (recordsRead < position) {
            State currentState = getState(0);
            do {
                ColumnReader columnReader = currentState.column;

                // currentLevel = depth + 1 at this point
                // set the current value
                if (columnReader.getCurrentDefinitionLevel() >= currentState.maxDefinitionLevel) {
                    columnReader.skip();
                }
                columnReader.consume();

                // Based on repetition level work out next state to go to
                int nextR =
                        currentState.maxRepetitionLevel == 0 ? 0 : columnReader.getCurrentRepetitionLevel();
                currentState = currentState.getNextState(nextR);
            } while (currentState != null);
            recordsRead++;
        }
    }

    private State getState(int i) {
        return states[i];
    }

    public boolean shouldSkipCurrentRecord() {
        return this.recordReader.shouldSkipCurrentRecord();
    }

    @Override
    public Long getCurrentRowId() {
        return currentRowId;
    }

    protected abstract Long nextRowId();

}
