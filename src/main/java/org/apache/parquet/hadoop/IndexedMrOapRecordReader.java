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
package org.apache.parquet.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.RecordReader;
import org.apache.parquet.hadoop.metadata.ParquetFooter;

public class IndexedMrOapRecordReader<T> implements RecordReader<T> {

    private Configuration configuration;
    private Path file;
    private int[] globalRowIds;
    private ParquetFooter footer;

    private InternalOapRecordReader<T> internalReader;

    private ReadSupport<T> readSupport;

    public IndexedMrOapRecordReader(
        ReadSupport<T> readSupport,
        Path file,
        Configuration configuration,
        int[] globalRowIds,
        ParquetFooter footer) {
      Preconditions.checkNotNull(globalRowIds,"index collection can not be null!");
      this.readSupport = readSupport;
      this.file = file;
      this.configuration = configuration;
      this.globalRowIds = globalRowIds;
      this.footer = footer;
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

      OapParquetFileReader reader = OapParquetFileReader.open(configuration, file,
        footer.toParquetMetadata(globalRowIds));
      this.internalReader = new InternalOapRecordReader<>(readSupport);
      this.internalReader.initialize(reader, configuration);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return internalReader.nextKeyValue();
    }
}
