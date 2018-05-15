package org.apache.spark.sql.execution.datasources.oap.io;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.VectorizedOapRecordReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.oap.filecache.DataFiber;
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache;
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheManager;
import org.apache.spark.sql.types.StructType;

public class CachedRecordReader extends VectorizedOapRecordReader {

    protected Path file;
    protected ParquetMetadata footer;
    protected Configuration configuration;
    private int[] requiredColumnIds;
    private ParquetDataFile dataFile;
    protected long maxRows = 0L;
    protected Iterator<Entry<Integer, Long>> rowGroupIdxToRowCountIter;

    public CachedRecordReader(
        Path file,
        ParquetMetadata footer,
        Configuration configuration,
        ParquetDataFile dataFile,
        int[] requiredColumnIds) {
      super(file, configuration, footer);
      this.dataFile = dataFile;
      this.requiredColumnIds = requiredColumnIds;
    }

    @Override
    public void initialize() throws IOException, InterruptedException {
      if (this.footer == null) {
        footer = readFooter(configuration, file, NO_FILTER);
      }
      // read by cache, needn't open file reader
      initialize(footer, configuration, false, false);
      initializeInternal();
    }

  @Override
  protected void initializeInternal() throws IOException, UnsupportedOperationException {
    super.initializeInternal();
    initStateVars();
  }

  protected void initStateVars() {
    List<BlockMetaData> blocks = this.footer.getBlocks();
    Map<Integer, Long> idxToRowCountMap = Maps.newLinkedHashMap();
    for (int i = 0; i < blocks.size(); i++) {
      long rowCount = blocks.get(i).getRowCount();
      Preconditions.checkArgument(rowCount < Integer.MAX_VALUE,
        "rowgroup too large, can't use cache.");
      this.maxRows = Math.max(maxRows, rowCount);
      idxToRowCountMap.put(i, rowCount);
    }
    rowGroupIdxToRowCountIter = idxToRowCountMap.entrySet().iterator();
  }

  public void initBatch(
      MemoryMode memMode,
      StructType partitionColumns,
      InternalRow partitionValues) {
      initBatch(memMode, (int)maxRows, partitionColumns, partitionValues);
    }


    public boolean nextBatch() throws IOException {
      columnarBatch.reset();
      if (rowsReturned >= totalRowCount) return false;
      Preconditions.checkState(rowGroupIdxToRowCountIter.hasNext(),
        "rowGroupIdxToRowCountIter state error");
      Entry<Integer, Long> rowGroupIdxToRowCount = rowGroupIdxToRowCountIter.next();
      Integer rowGroupIdx = rowGroupIdxToRowCount.getKey();
      int rowCount = rowGroupIdxToRowCount.getValue().intValue();
      int columnVectorIdx = 0;
      for (int requiredColumnId : requiredColumnIds) {
        DataFiber dataFiber = new DataFiber(dataFile, requiredColumnId, rowGroupIdx);
        FiberCache fiberCache = FiberCacheManager.get(dataFiber, configuration);
        dataFile.release(requiredColumnId);
        dataFile.update(requiredColumnId, fiberCache);
        columnarBatch.column(columnVectorIdx).loadBytes(fiberCache.getBaseOffset());
        columnVectorIdx++;
      }
      rowsReturned += rowCount;
      columnarBatch.setNumRows(rowCount);
      numBatched = rowCount;
      batchIdx = 0;
      return true;
    }

  @Override
  public void close() throws IOException {
    super.close();
    for (int requiredColumnId : requiredColumnIds) {
      dataFile.release(requiredColumnId);
    }
  }
}
