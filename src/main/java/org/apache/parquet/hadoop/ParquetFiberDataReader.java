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

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFiberDataReader implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetFiberDataReader.class);

  private final FileMetaData fileMetaData;

  private final FileStatus fileStatus;

  private final ParquetMetadataConverter converter;

  private final SeekableInputStream f;

  private final CodecFactory codecFactory;

  private ParquetMetadata footer;

  public static ParquetFiberDataReader open(
          Configuration conf,
          Path file,
          ParquetMetadata footer) throws IOException {
    return new ParquetFiberDataReader(conf, file, footer);
  }

  private ParquetFiberDataReader(
      Configuration conf,
      Path file,
      ParquetMetadata footer) throws IOException {
    this.converter = new ParquetMetadataConverter(conf);
    FileSystem fs = file.getFileSystem(conf);
    this.fileStatus = fs.getFileStatus(file);
    this.footer = footer;
    this.f = HadoopStreams.wrap(fs.open(file));
    this.fileMetaData = footer.getFileMetaData();
    this.codecFactory = new CodecFactory(conf);
  }

  public PageReadStore readFiberData(
      BlockMetaData block,
      ColumnDescriptor columnDescriptor) throws IOException {

    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }
    ColumnChunkPageReadStore rowGroup = new ColumnChunkPageReadStore(block.getRowCount());
    ColumnChunkMetaData mc = findColumnMeta(block, columnDescriptor);

    DataFiberDescriptor descriptor =
      new DataFiberDescriptor(
        columnDescriptor,
        mc,
        mc.getStartingPos(),
        (int) mc.getTotalSize());
    DataFiber dataFiber = readChunkData(descriptor);
    rowGroup.addColumn(dataFiber.descriptor.col, dataFiber.readAllPages());
    return rowGroup;
  }

  @Override
  public void close() throws IOException {
    try {
      if (f != null) {
        f.close();
      }
    } finally {
      if (codecFactory != null) {
        codecFactory.release();
      }
    }
  }

  public ParquetMetadata getFooter() {
    return footer;
  }

  private DataFiber readChunkData(DataFiberDescriptor descriptor) throws
          IOException {
    f.seek(descriptor.fileOffset);
    byte[] chunksBytes = new byte[descriptor.size];
    f.readFully(chunksBytes);
    return new DataFiber(descriptor, chunksBytes, 0, f) ;
  }

  private ColumnChunkMetaData findColumnMeta(
      BlockMetaData block,
      ColumnDescriptor columnDescriptor) throws IOException {
    ColumnPath columnPath = ColumnPath.get(columnDescriptor.getPath());
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      if (columnPath.equals(pathKey)) {
        return mc;
      }
    }
    throw new IOException("Can not found column meta of cloumn + " + columnPath);
  }

  private FileMetaData getFileMetaData() {
    return fileMetaData;
  }

  private class DataFiber extends ByteArrayInputStream {

    private final DataFiberDescriptor descriptor;
    private final SeekableInputStream f;

    DataFiber(DataFiberDescriptor descriptor,
        byte[] data,
        int offset,
        SeekableInputStream f) {
      super(data);
      this.descriptor = descriptor;
      this.pos = offset;
      this.f = f;
    }

    private PageHeader readPageHeader() throws IOException {
      PageHeader pageHeader;
      int initialPos = this.pos;
      try {
        pageHeader = Util.readPageHeader(this);
      } catch (IOException e) {
        this.pos = initialPos;
        LOG.info("completing the column chunk to read the page header");
        pageHeader = Util.readPageHeader(new SequenceInputStream(this, f));
      }
      return pageHeader;
    }

    ColumnChunkPageReadStore.ColumnChunkPageReader readAllPages() throws IOException {
      List<DataPage> pagesInChunk = new ArrayList<>();
      DictionaryPage dictionaryPage = null;
      PrimitiveType type = getFileMetaData().getSchema()
              .getType(descriptor.col.getPath()).asPrimitiveType();
      long valuesCountReadSoFar = 0;
      while (valuesCountReadSoFar < descriptor.metadata.getValueCount()) {
        PageHeader pageHeader = readPageHeader();
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();
        switch (pageHeader.type) {
          case DICTIONARY_PAGE:
            // there is only one dictionary page per column chunk
            if (dictionaryPage != null) {
              throw new ParquetDecodingException("more than one dictionary page in column "
                      + descriptor.col);
            }
            DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
            dictionaryPage =
              new DictionaryPage(
                this.readAsBytesInput(compressedPageSize),
                uncompressedPageSize,
                dicHeader.getNum_values(),
                converter.getEncoding(dicHeader.getEncoding()));
            break;
          case DATA_PAGE:
            DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
            pagesInChunk.add(
              new DataPageV1(
                this.readAsBytesInput(compressedPageSize),
                dataHeaderV1.getNum_values(),
                uncompressedPageSize,
                converter.fromParquetStatistics(
                  getFileMetaData().getCreatedBy(),
                  dataHeaderV1.getStatistics(),
                  type),
                converter.getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                converter.getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                converter.getEncoding(dataHeaderV1.getEncoding())));
            valuesCountReadSoFar += dataHeaderV1.getNum_values();
            break;
          case DATA_PAGE_V2:
            DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
            int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length()
              - dataHeaderV2.getDefinition_levels_byte_length();
            pagesInChunk.add(
              new DataPageV2(
                dataHeaderV2.getNum_rows(),
                dataHeaderV2.getNum_nulls(),
                dataHeaderV2.getNum_values(),
                this.readAsBytesInput(dataHeaderV2.getRepetition_levels_byte_length()),
                this.readAsBytesInput(dataHeaderV2.getDefinition_levels_byte_length()),
                converter.getEncoding(dataHeaderV2.getEncoding()),
                this.readAsBytesInput(dataSize),
                uncompressedPageSize,
                converter.fromParquetStatistics(
                  getFileMetaData().getCreatedBy(),
                  dataHeaderV2.getStatistics(),
                  type),
                dataHeaderV2.isIs_compressed()));
            valuesCountReadSoFar += dataHeaderV2.getNum_values();
            break;
          default:
            LOG.debug("skipping page of type {} of size {}",
                    pageHeader.getType(), compressedPageSize);
            this.skip(compressedPageSize);
            break;
        }
      }
      if (valuesCountReadSoFar != descriptor.metadata.getValueCount()) {
        throw new CorruptParquetFileException(
          descriptor,
          fileStatus.getPath(),
          valuesCountReadSoFar,
          pagesInChunk.size(),
          pos);
      }
      CodecFactory.BytesDecompressor decompressor = codecFactory.getDecompressor(
              descriptor.metadata.getCodec());
      return new ColumnChunkPageReadStore
        .ColumnChunkPageReader(decompressor, pagesInChunk, dictionaryPage);
    }

    private BytesInput readAsBytesInput(int size) throws IOException {
      if (pos + size > count) {
        int l1 = count - pos;
        int l2 = size - l1;
        LOG.info("completed the column chunk with {} bytes", l2);
        return BytesInput.concat(readAsBytesInputInternal(l1),
                BytesInput.copy(BytesInput.from(f, l2)));
      }
      return readAsBytesInputInternal(size);
    }

    private BytesInput readAsBytesInputInternal(int size) {
      final BytesInput r = BytesInput.from(this.buf, this.pos, size);
      this.pos += size;
      return r;
    }
  }

  private static class DataFiberDescriptor {
    private final ColumnDescriptor col;
    private final ColumnChunkMetaData metadata;
    private final long fileOffset;
    private final int size;

    private DataFiberDescriptor(
        ColumnDescriptor col,
        ColumnChunkMetaData metadata,
        long fileOffset,
        int size) {
      this.col = col;
      this.metadata = metadata;
      this.fileOffset = fileOffset;
      this.size = size;
    }
  }

  private static class CorruptParquetFileException extends IOException {

    CorruptParquetFileException(
        DataFiberDescriptor descriptor,
        Path path,
        long valuesCountReadSoFar,
        int pagesInChunkSize,
        int position) {
      super("Expected " + descriptor.metadata.getValueCount() + " values in column chunk at " +
        path + " offset " + descriptor.metadata.getFirstDataPageOffset() +
        " but got " + valuesCountReadSoFar + " values instead over " + pagesInChunkSize
        + " pages ending at file offset " + (descriptor.fileOffset + position));
    }
  }
}
