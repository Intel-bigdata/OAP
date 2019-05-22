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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.IndexedBlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetFooter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.schema.MessageType;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class OapParquetFileReader implements Closeable {

  private ParquetCacheableFileReader reader;
  private int currentBlock = 0;

  private OapParquetFileReader(ParquetCacheableFileReader reader) {
    this.reader = reader;
  }

  public static OapParquetFileReader open(Configuration conf, Path file, ParquetMetadata footer)
          throws IOException {
    return new OapParquetFileReader(new ParquetCacheableFileReader(conf, file, footer));
  }

  public RowGroupDataAndRowIds readNextRowGroupAndRowIds() throws IOException {
    PageReadStore pageReadStore = reader.readNextRowGroup();
    BlockMetaData blockMetaData = reader.getRowGroups().get(currentBlock);
    currentBlock ++;
    IntList needRowIds = ((IndexedBlockMetaData) blockMetaData).getNeedRowIds();
    return new RowGroupDataAndRowIds(pageReadStore, needRowIds);
  }

  public PageReadStore readNextRowGroup() throws IOException {
    PageReadStore pageReadStore = this.reader.readNextRowGroup();
    currentBlock ++;
    return pageReadStore;
  }

  public void setRequestedSchema(MessageType projection) {
    this.reader.setRequestedSchema(projection);
  }

  public List<BlockMetaData> getRowGroups() {
    return this.reader.getRowGroups();
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }

  public ParquetMetadata getFooter() {
    return this.reader.getFooter();
  }

  public Path getPath() {
    return this.reader.getPath();
  }

  public static ParquetFooter readParquetFooter(
      Configuration configuration,
      Path file) throws IOException {
    return readParquetFooter(configuration, file, NO_FILTER);
  }

  public static ParquetFooter readParquetFooter(
      Configuration configuration,
      Path file,
      ParquetMetadataConverter.MetadataFilter filter) throws IOException {
    return ParquetFooter.from(ParquetFileReader.readFooter(configuration, file, filter));
  }

  public static class RowGroupDataAndRowIds {
    private PageReadStore pageReadStore;
    private IntList rowIds;

    RowGroupDataAndRowIds(PageReadStore pageReadStore, IntList rowIds) {
      this.pageReadStore = pageReadStore;
      this.rowIds = rowIds;
    }

    public PageReadStore getPageReadStore() {
      return pageReadStore;
    }

    public IntList getRowIds() {
      return rowIds;
    }
  }
}
