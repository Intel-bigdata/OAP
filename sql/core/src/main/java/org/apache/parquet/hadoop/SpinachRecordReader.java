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

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.RecordReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.google.common.collect.Lists;

public class SpinachRecordReader<T> implements RecordReader<T> {

    private Configuration configuration;
    private Path file;
    private long[] globalRowIds;

    private InternalSpinachRecordReader<T> internalReader;

    private ReadSupport<T> readSupport;

    SpinachRecordReader(ReadSupport<T> readSupport, Path file, Configuration configuration, long[] globalRowIds) {
        Preconditions.checkNotNull(globalRowIds,"index collection can not Bbe null!");
        this.readSupport = readSupport;
        this.file = file;
        this.configuration = configuration;
        this.globalRowIds = globalRowIds;
    }

    @Override
    public void close() throws IOException {
        internalReader.close();
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

        List<BlockMetaData> blocks = footer.getBlocks();

        List<BlockMetaData> inputBlockList = Lists.newArrayList();

        List<List<Long>> rowIdsList = Lists.newArrayList();

        long nextRowGroupStartRowId = 0;
        int totalCount = globalRowIds.length;
        int index = 0;

        for (BlockMetaData block : blocks) {
            long currentRowGroupStartRowId = nextRowGroupStartRowId;
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
            }
        }
        ParquetFileReader parquetFileReader = ParquetFileReader.open(configuration, file,
                new ParquetMetadata(footer.getFileMetaData(), inputBlockList));
        this.internalReader = new InternalSpinachRecordReader<>(readSupport);
        this.internalReader.initialize(parquetFileReader, configuration);
        this.internalReader.initOthers(rowIdsList);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return internalReader.nextKeyValue();
    }
}
