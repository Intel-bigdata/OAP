/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.io;

import java.util.Arrays;
import java.util.List;

import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * Message level of the IO structure
 *
 * @author Julien Le Dem
 */
public class SColumnMessageIO extends GroupColumnIO {

    private List<PrimitiveColumnIO> leaves;

    private final String createdBy;

    SColumnMessageIO(MessageType messageType, String createdBy) {
        super(messageType, null, 0);
        this.createdBy = createdBy;
    }

    public List<String[]> getColumnNames() {
        return super.getColumnNames();
    }

    public <T> RecordReader<T> getRecordReader(final PageReadStore columns,
                                               final RecordMaterializer<T> recordMaterializer,
                                               List<Long> rowIdList) {
        checkNotNull(columns, "columns");
        checkNotNull(recordMaterializer, "recordMaterializer");

        if (leaves.isEmpty()) {
            return new EmptyRecordReader<>(recordMaterializer);
        }

        return  new PositionableRecordReaderImpl<>(
                SColumnMessageIO.this,
                recordMaterializer,
                new ColumnReadStoreImpl(columns, recordMaterializer.getRootConverter(), getType(), createdBy),
                columns.getRowCount(),
                rowIdList);
    }

    void setLevels() {
        setLevels(0, 0, new String[0], new int[0], Arrays.<ColumnIO>asList(this), Arrays.<ColumnIO>asList(this));
    }

    void setLeaves(List<PrimitiveColumnIO> leaves) {
        this.leaves = leaves;
    }

    public List<PrimitiveColumnIO> getLeaves() {
        return this.leaves;
    }

    @Override
    public MessageType getType() {
        return (MessageType) super.getType();
    }
}
