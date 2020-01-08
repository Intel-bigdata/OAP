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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.OrderedBlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.SeekableInputStream;

import org.apache.spark.sql.execution.datasources.oap.filecache.BinaryDataFiberId;
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache;
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheManager;
import org.apache.spark.sql.execution.datasources.oap.io.DataFile;
import org.apache.spark.sql.execution.datasources.oap.io.ParquetDataFile;
import org.apache.spark.sql.internal.oap.OapConf$;
import org.apache.spark.sql.oap.OapRuntime$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;


public class ParquetCacheableFileReader extends ParquetFileReader {

  private FiberCacheManager cacheManager = OapRuntime$.MODULE$.getOrCreate().fiberCacheManager();

  private DataFile dataFile;

  private boolean binaryCacheEnabled;

  private Map<ColumnPath, Integer> pathToColumnIndexMap = Collections.emptyMap();

  ParquetCacheableFileReader(Configuration conf, Path file, ParquetMetadata footer)
          throws IOException {
    super(conf, file, footer);
    this.binaryCacheEnabled =
      conf.getBoolean(OapConf$.MODULE$.OAP_PARQUET_BINARY_DATA_CACHE_ENABLED().key(), false);

    if (binaryCacheEnabled) {
      this.dataFile = new ParquetDataFile(file.toUri().toString(), new StructType(), conf);
      List<String[]> paths = footer.getFileMetaData().getSchema().getPaths();
      int size = paths.size();
      // TODO only required columns needed in pathToColumnIndexMap, but include all columns for now,
      // better to do the filter.
      pathToColumnIndexMap = Maps.newHashMapWithExpectedSize(size);
      for (int i = 0; i < size; i++) {
        pathToColumnIndexMap.put(ColumnPath.get(paths.get(i)), i);
      }
    }
  }

  public PageReadStore readNextRowGroup() throws IOException {

    // if binaryCache not enable, use original readNextRowGroup method.
    if (!binaryCacheEnabled) {
      return super.readNextRowGroup();
    }

    // if binaryCache enable or mixed read enable, we should run this logic.
    if (currentBlock == blocks.size()) {
      return null;
    }

    OrderedBlockMetaData block = (OrderedBlockMetaData)blocks.get(currentBlock);
    int rowGroupId = block.getRowGroupId();
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }
    this.currentRowGroup = new ColumnChunkPageReadStore(block.getRowCount());

    List<ColumnChunk> allChunks = new ArrayList<>();
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      BenchmarkCounter.incrementTotalBytes(mc.getTotalSize());
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        long startingPos = mc.getStartingPos();
        int totalSize = (int) mc.getTotalSize();
        int columnIndex = pathToColumnIndexMap.get(pathKey);
        ColumnChunk columnChunk =
          new ColumnChunk(columnDescriptor, mc, startingPos, totalSize, rowGroupId, columnIndex);
        allChunks.add(columnChunk);
      }
    }
    // actually read all the chunks
    for (ColumnChunk columnChunk : allChunks) {
      final Chunk chunk = columnChunk.read(f);
      currentRowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
    }

    // avoid re-reading bytes the dictionary reader is used after this call
    if (nextDictionaryReader != null) {
      nextDictionaryReader.setRowGroup(currentRowGroup);
    }

    advanceToNextBlock();

    return currentRowGroup;
  }

  public class ColumnChunk {
    private final long offset;
    private final int length;
    private final ChunkDescriptor descriptor;
    private final int rowGroupId;
    private final int columnIndex;

    ColumnChunk(
        ColumnDescriptor col,
        ColumnChunkMetaData metadata,
        long offset,
        int length,
        int rowGroupId,
        int columnIndex) {
      this.offset = offset;
      this.length = length;
      this.rowGroupId = rowGroupId;
      this.columnIndex = columnIndex;
      descriptor = new ChunkDescriptor(col, metadata, offset, length);
    }

    Chunk read(SeekableInputStream f) throws IOException {
      // TODO impl mixed read model.
      byte[] chunksBytes = binaryCacheEnabled ? readFromCache(f) : readFromFile(f);
      return new WorkaroundChunk(
          descriptor, Collections.singletonList(ByteBuffer.wrap(chunksBytes)), f);
    }

    private byte[] readFromCache(SeekableInputStream f) {
      FiberCache fiberCache = null;
      BinaryDataFiberId fiberId = null;
      try {
        byte[] data = new byte[length];
        fiberId = new BinaryDataFiberId(dataFile, columnIndex, rowGroupId);
        fiberId.withLoadCacheParameters(f, offset, length);
        fiberCache = cacheManager.get(fiberId);
        long fiberOffset = fiberCache.getBaseOffset();
        Platform.copyMemory(null, fiberOffset, data, Platform.BYTE_ARRAY_OFFSET, length);
        return data;
      } finally {
        if (fiberCache != null) {
          fiberCache.release();
        }
        if (fiberId != null) {
          fiberId.cleanLoadCacheParameters();
        }
      }
    }

    private byte[] readFromFile(SeekableInputStream f) throws IOException {
      byte[] data = new byte[length];
      f.seek(offset);
      f.readFully(data);
      // report in a counter the data we just scanned
      BenchmarkCounter.incrementBytesRead(length);
      return data;
    }
  }
}
