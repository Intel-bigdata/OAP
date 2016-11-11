/**
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
package org.apache.parquet.hadoop;

import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.api.RecordReader;
import org.apache.parquet.hadoop.api.SpinachReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import com.google.common.collect.Lists;

public class SpinachRecordReader<T> implements RecordReader<Long, T> {

    private Configuration configuration;
    private Path file;
    private Filter filter;
    private List<Long> filteredStartRowIdList = Lists.newArrayList();
    private long[] globalRowIds;

    private InternalSpinachRecordReader<T> internalReader;

    private SpinachReadSupport<T> readSupport;

    private SpinachRecordReader(SpinachReadSupport<T> readSupport, Path file, Configuration configuration,
            Filter filter, long[] globalRowIds) {
        this.readSupport = readSupport;
        this.filter = checkNotNull(filter, "filter");
        this.file = file;
        this.configuration = configuration;
        this.globalRowIds = globalRowIds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        internalReader.close();
    }

    @Override
    public Long getCurrentRowId() throws IOException, InterruptedException {
        int currentBlockIndex = internalReader.getCurrentBlockIndex();
        long baseValue = filteredStartRowIdList.get(currentBlockIndex);
        return baseValue + internalReader.getInternalRowId();
    }

    @Override
    public T getCurrentValue() throws IOException, InterruptedException {
        return internalReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return internalReader.getProgress();
    }

    public void initialize() throws IOException, InterruptedException {

        ParquetMetadata footer = readFooter(configuration, file, NO_FILTER);
        MessageType fileSchema = footer.getFileMetaData().getSchema();

        List<BlockMetaData> blocks = footer.getBlocks();

        long currentRowGroupStartRowId = 0;
        long nextRowGroupStartRowId = 0;

        List<BlockMetaData> inputBlockList = Lists.newArrayList();

        List<List<Long>> rowIdsList = Lists.newArrayList();

        if (globalRowIds != null && globalRowIds.length != 0) {
            int totalCount = globalRowIds.length;
            int index = 0;

            for (BlockMetaData block : blocks) {
                currentRowGroupStartRowId = nextRowGroupStartRowId;
                nextRowGroupStartRowId += block.getRowCount();
                List<Long> rowIdList = Lists.newArrayList();
                while (index < totalCount) {
                    long globalRowGroupId = globalRowIds[index];
                    if (globalRowGroupId < nextRowGroupStartRowId) {
                        rowIdList.add(globalRowGroupId - currentRowGroupStartRowId);
                        index++;
                    } else {
                        break;
                    }

                }
                if (rowIdList != null && !rowIdList.isEmpty()) {
                    inputBlockList.add(block);
                    rowIdsList.add(rowIdList);
                    filteredStartRowIdList.add(currentRowGroupStartRowId);
                }
            }
            internalReader = new RowIdsIterInternalSpinachRecordReader<T>(readSupport, filter);
            internalReader.initialize(fileSchema, footer.getFileMetaData().getKeyValueMetaData(), file,
                    inputBlockList, rowIdsList, configuration);
        } else {

            for (BlockMetaData block : blocks) {
                currentRowGroupStartRowId = nextRowGroupStartRowId;
                nextRowGroupStartRowId += block.getRowCount();
                filteredStartRowIdList.add(currentRowGroupStartRowId);
            }
            internalReader = new CounterInternalSpinachRecordReader<T>(readSupport, filter);
            internalReader.initialize(fileSchema, footer.getFileMetaData().getKeyValueMetaData(), file,
                    blocks, rowIdsList, configuration);
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return internalReader.nextKeyValue();
    }

    public static <T> Builder<T> builder(SpinachReadSupport<T> readSupport, Path path) {
        return new Builder<T>(readSupport, path);
    }

    public static <T> Builder<T> builder(SpinachReadSupport<T> readSupport, Path path, Configuration conf) {
        return new Builder<T>(readSupport, path, conf);
    }

    public static class Builder<T> {
        private final SpinachReadSupport<T> readSupport;
        private final Path file;
        private Configuration conf;
        private Filter filter;
        private long[] globalRowIds;

        private Builder(SpinachReadSupport<T> readSupport, Path path, Configuration conf) {
            this.readSupport = checkNotNull(readSupport, "readSupport");
            this.file = checkNotNull(path, "path");
            this.conf = checkNotNull(conf, "configuration");
            this.filter = FilterCompat.NOOP;
        }

        private Builder(SpinachReadSupport<T> readSupport, Path path) {
            this.readSupport = checkNotNull(readSupport, "readSupport");
            this.file = checkNotNull(path, "path");
            this.conf = new Configuration();
            this.filter = FilterCompat.NOOP;
        }

        public Builder<T> withConf(Configuration conf) {
            this.conf = checkNotNull(conf, "conf");
            return this;
        }

        public Builder<T> withFilter(Filter filter) {
            this.filter = checkNotNull(filter, "filter");
            return this;
        }

        public Builder<T> withGlobalRowIds(long[] globalRowIds) {
            this.globalRowIds = globalRowIds;
            return this;
        }

        public SpinachRecordReader<T> build() throws IOException {
            return new SpinachRecordReader<T>(readSupport, file, conf, filter, globalRowIds);
        }
    }
}
