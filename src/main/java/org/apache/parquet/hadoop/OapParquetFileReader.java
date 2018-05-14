package org.apache.parquet.hadoop;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.IndexedBlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetFooter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.schema.MessageType;

public class OapParquetFileReader {

  private ParquetFileReader reader;
  private int currentBlock = 0;

  private OapParquetFileReader(ParquetFileReader reader) {
    this.reader = reader;
  }

  public static OapParquetFileReader open(Configuration conf, Path file, ParquetMetadata footer)
          throws IOException {
    return new OapParquetFileReader(new ParquetFileReader(conf, file, footer));
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

  public void filterRowGroups(FilterCompat.Filter filter) throws IOException {
    this.reader.filterRowGroups(filter);
  }

  public void setRequestedSchema(MessageType projection) {
    this.reader.setRequestedSchema(projection);
  }

  public List<BlockMetaData> getRowGroups() {
    return this.reader.getRowGroups();
  }

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
}
