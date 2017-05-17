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

import java.util.Iterator;
import java.util.List;

import org.apache.parquet.Preconditions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.api.SpinachReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.PositionableRecordReader;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.RowIdIteratorRecordReaderImpl;

public class RowIdsIterInternalSpinachRecordReader<T> extends InternalSpinachRecordReader<T> {

    protected Iterator<List<Long>> rowIdsIter = null;

    public RowIdsIterInternalSpinachRecordReader(SpinachReadSupport<T> readSupport, Filter filter) {
        super(readSupport, filter);
    }

    public RowIdsIterInternalSpinachRecordReader(SpinachReadSupport<T> readSupport) {
        this(readSupport, FilterCompat.NOOP);
    }

    protected void initOthers(List<List<Long>> rowIdsList, List<BlockMetaData> blocks) {
        Preconditions.checkArgument(rowIdsList != null && !rowIdsList.isEmpty(), "RowIdsList must not empty");
        this.rowIdsIter = rowIdsList.iterator();
        for (List<Long> rowIdList : rowIdsList) {
            total += rowIdList.size();
        }
    }

    @Override
    protected PositionableRecordReader<T> getPositionableRecordReader(RecordReader<T> recordReader,
            long rowCount) {
        return new RowIdIteratorRecordReaderImpl<T>(recordReader, rowIdsIter.next(), rowCount);
    }

}
