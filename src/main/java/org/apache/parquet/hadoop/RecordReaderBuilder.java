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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.RecordReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;

import static org.apache.parquet.Preconditions.checkNotNull;


public class RecordReaderBuilder<T> {

    private final ReadSupport<T> readSupport;
    private final Path file;
    private Configuration conf;
    private int[] globalRowIds = new int[0];
    private ParquetMetadata footer;

    private RecordReaderBuilder(ReadSupport<T> readSupport, Path path, Configuration conf) {
        this.readSupport = checkNotNull(readSupport, "readSupport");
        this.file = checkNotNull(path, "path");
        this.conf = checkNotNull(conf, "configuration");
    }

    private RecordReaderBuilder(ReadSupport<T> readSupport, Path path) {
        this.readSupport = checkNotNull(readSupport, "readSupport");
        this.file = checkNotNull(path, "path");
        this.conf = new Configuration();
    }

    public RecordReaderBuilder<T> withGlobalRowIds(int[] globalRowIds) {
        this.globalRowIds = globalRowIds;
        return this;
    }

    public RecordReaderBuilder<T> withFooter(ParquetMetadata footer) {
        this.footer = footer;
        return this;
    }

    public RecordReader<T> buildDefault() throws IOException {
        return new DefaultRecordReader<>(readSupport, file, conf, footer);
    }


    public RecordReader<T> buildIndexed() throws IOException {
        return new OapRecordReader<>(readSupport, file, conf, globalRowIds, footer);
    }

    public static <T> RecordReaderBuilder<T> builder(ReadSupport<T> readSupport, Path path) {
        return new RecordReaderBuilder<>(readSupport, path);
    }

    public static <T> RecordReaderBuilder<T> builder(ReadSupport<T> readSupport, Path path, Configuration conf) {
        return new RecordReaderBuilder<>(readSupport, path, conf);
    }
}
