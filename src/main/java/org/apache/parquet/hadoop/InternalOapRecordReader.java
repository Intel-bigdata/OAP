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

import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.hadoop.ParquetInputFormat.STRICT_TYPE_CHECKING;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.IndexedBlockMetaData;
import org.apache.parquet.hadoop.OapParquetFileReader.RowGroupDataAndRowIds;
import org.apache.parquet.hadoop.utils.Collections3;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.RecordReaderFactory;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalOapRecordReader<T> {

    private static final Logger LOG = LoggerFactory.getLogger(InternalOapRecordReader.class);

    private ColumnIOFactory columnIOFactory;

    private MessageType requestedSchema;
    private MessageType fileSchema;
    private int columnCount;
    private final ReadSupport<T> readSupport;

    private RecordMaterializer<T> recordConverter;

    private T currentValue;
    private long total;
    private long current = 0;
    private int currentBlock = -1;

    private boolean strictTypeChecking;

    private long totalCountLoadedSoFar = 0;

    private OapParquetFileReader reader;

    private RecordReader<T> recordReader;

    private String createdBy;

    private ParquetReadMetrics metrics;

    /**
     * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
     */
    InternalOapRecordReader(ReadSupport<T> readSupport) {
      this.readSupport = readSupport;
    }

    private void checkRead() throws IOException {
      if (current == totalCountLoadedSoFar) {
        if (current != 0) {
          metrics.recordMetrics(totalCountLoadedSoFar,columnCount);
        }

        LOG.info("at row " + current + ". reading next block");
        metrics.startReadOneRowGroup();
        RowGroupDataAndRowIds rowGroupDataAndRowIds = reader.readNextRowGroupAndRowIds();
        PageReadStore pages = rowGroupDataAndRowIds.getPageReadStore();
        checkIOState(pages);
        metrics.overReadOneRowGroup(pages);
        if (LOG.isDebugEnabled()) {
          LOG.debug("initializing Record assembly with requested schema {}", requestedSchema);
        }
        MessageColumnIO columnIO =
          columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking);
        IntList rowIdList = rowGroupDataAndRowIds.getRowIds();
        this.recordReader = getRecordReader(columnIO,pages,rowIdList);
        metrics.startRecordAssemblyTime();
        totalCountLoadedSoFar += rowIdList.size();
        ++currentBlock;
      }
    }

    private RecordReader<T> getRecordReader(
        MessageColumnIO columnIO,
        PageReadStore pages,
        IntList rowIdList) {
      return RecordReaderFactory
        .getRecordReader(columnIO, pages, recordConverter, createdBy, rowIdList);
    }

    public void close() throws IOException {
      if (reader != null) {
        reader.close();
      }
    }

    public void initialize(OapParquetFileReader readerWrapper, Configuration configuration)
      throws IOException {
      this.reader = readerWrapper;
      FileMetaData parquetFileMetadata = readerWrapper.getFooter().getFileMetaData();
      this.fileSchema = parquetFileMetadata.getSchema();
      Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
      ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
        configuration, Collections3.toSetMultiMap(fileMetadata), fileSchema));
      this.createdBy = parquetFileMetadata.getCreatedBy();
      this.columnIOFactory = new ColumnIOFactory(createdBy);
      this.requestedSchema = readContext.getRequestedSchema();
      this.columnCount = requestedSchema.getPaths().size();
      this.recordConverter = readSupport.prepareForRead(
        configuration, fileMetadata, fileSchema, readContext);
      this.strictTypeChecking = configuration.getBoolean(STRICT_TYPE_CHECKING, true);
      List<BlockMetaData> rowGroups = readerWrapper.getRowGroups();
      for (BlockMetaData rowGroup : rowGroups) {
        total += ((IndexedBlockMetaData)rowGroup).getNeedRowIds().size();
      }
      this.reader.setRequestedSchema(requestedSchema);
      this.metrics = new ParquetReadMetrics();
      LOG.info("RecordReader initialized will read a total of {} records.", total);
    }


    boolean nextKeyValue() throws IOException, InterruptedException {
      boolean recordFound = false;

      while (!recordFound) {
        // no more records left
        if (current >= total) {
          return false;
        }
        try {
          checkRead();
          this.currentValue = recordReader.read();
          current++;
          if (recordReader.shouldSkipCurrentRecord()) {
            // this record is being filtered via the filter2 package
            if (DEBUG) {
              LOG.debug("skipping record");
            }
            continue;
          }
          recordFound = true;
          if (DEBUG) {
            LOG.debug("read value: " + currentValue);
          }
        } catch (RuntimeException e) {
          throw new ParquetDecodingException(
            String.format("Can not read value at %d in block %d in file %s",
              current,
              currentBlock,
              reader.getPath()),
            e);
        }
      }
      return true;
    }

    T getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }

    private void checkIOState(PageReadStore pages) throws IOException {
      if (pages == null) {
        throw new IOException(
          "expecting more rows but reached last block. Read " + current + " out of " + total);
      }
    }

}
