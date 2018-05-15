package org.apache.spark.sql.execution.datasources.oap.io;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.IndexedParquetMetadata;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

public class IndexedCachedRecordReader extends CachedRecordReader{

  private int[] globalRowIds;
  // Rowid Iter of RowGroup granularity
  private Iterator<IntList> rowIdsIter;
  private IndexedParquetMetadata indexedFooter;
  private IntList batchIds;

  public IndexedCachedRecordReader(
      Path file,
      ParquetMetadata footer,
      Configuration configuration,
      ParquetDataFile dataFile,
      int[] requiredColumnIds,
      int[] globalRowIds) {
    super(file, footer, configuration, dataFile, requiredColumnIds);
    this.globalRowIds = globalRowIds;
  }

  @Override
  public void initialize() throws IOException, InterruptedException {
    if (this.footer == null) {
      footer = readFooter(configuration, file, NO_FILTER);
    }
    indexedFooter = IndexedParquetMetadata.from(footer, globalRowIds);
    // use indexedFooter read data, need't do filterRowGroups.
    initialize(indexedFooter, configuration, false, false);
    initializeInternal();
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    if (returnColumnarBatch) {
      return columnarBatch;
    }
    return columnarBatch.getRow(batchIds.get(batchIdx - 1));
  }

  @Override
  protected void initStateVars() {
    this.rowIdsIter = indexedFooter.getRowIdsList().iterator();
    List<BlockMetaData> blocks = this.footer.getBlocks();
    IntList needRowGroupIds = indexedFooter.getNeedRowGroupIds();
    Map<Integer, Long> idxToRowCountMap = Maps.newLinkedHashMap();
    for (Integer needRowGroupId : needRowGroupIds) {
      long rowCount = blocks.get(needRowGroupId).getRowCount();
      Preconditions.checkArgument(rowCount < Integer.MAX_VALUE,
        "RowGroup too large, can't use cache.");
      this.maxRows = Math.max(maxRows, rowCount);
      idxToRowCountMap.put(needRowGroupId, rowCount);
    }
    rowGroupIdxToRowCountIter = idxToRowCountMap.entrySet().iterator();
  }

  @Override
  public boolean nextBatch() throws IOException {
    boolean hasNextBatch = super.nextBatch();
    if (hasNextBatch) {
      Preconditions.checkState(rowIdsIter.hasNext(), "hasNextBatch, rowIds state error.");
      batchIds = rowIdsIter.next();
      numBatched = batchIds.size();
      if (returnColumnarBatch) {
        columnarBatch.markAllFiltered();
        for (Integer rowId : batchIds) {
          columnarBatch.markValid(rowId);
        }
      }
    }
    return hasNextBatch;
  }
}
