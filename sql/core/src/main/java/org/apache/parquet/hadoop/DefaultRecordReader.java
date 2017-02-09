/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.RecordReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

public class DefaultRecordReader<T> implements RecordReader<Long, T> {

    private Configuration configuration;
    private Path file;

    private InternalParquetRecordReader<T> internalReader;

    private ReadSupport<T> readSupport;

    private DefaultRecordReader(ReadSupport<T> readSupport, Path file, Configuration configuration) {
        this.readSupport = readSupport;
        this.file = file;
        this.configuration = configuration;
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
        return 0L;
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
        ParquetFileReader parquetFileReader = ParquetFileReader.open(configuration, file, footer);
        this.internalReader = new InternalParquetRecordReader<T>(readSupport);
        this.internalReader.initialize(parquetFileReader, configuration);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return internalReader.nextKeyValue();
    }

    public static <T> Builder<T> builder(ReadSupport<T> readSupport, Path path) {
        return new Builder<>(readSupport, path);
    }

    public static <T> Builder<T> builder(ReadSupport<T> readSupport, Path path, Configuration conf) {
        return new Builder<>(readSupport, path, conf);
    }

    public static class Builder<T> {
        private final ReadSupport<T> readSupport;
        private final Path file;
        private Configuration conf;

        private Builder(ReadSupport<T> readSupport, Path path, Configuration conf) {
            this.readSupport = checkNotNull(readSupport, "readSupport");
            this.file = checkNotNull(path, "path");
            this.conf = checkNotNull(conf, "configuration");
        }

        private Builder(ReadSupport<T> readSupport, Path path) {
            this.readSupport = checkNotNull(readSupport, "readSupport");
            this.file = checkNotNull(path, "path");
            this.conf = new Configuration();
        }

        public DefaultRecordReader<T> build() throws IOException {
            return new DefaultRecordReader<>(readSupport, file, conf);
        }
    }
}
