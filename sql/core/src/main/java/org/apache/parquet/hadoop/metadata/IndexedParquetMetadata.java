package org.apache.parquet.hadoop.metadata;


import org.apache.parquet.it.unimi.dsi.fastutil.longs.LongList;

import java.util.List;

public class IndexedParquetMetadata extends ParquetMetadata {

    private List<LongList> rowIdsList;

    public IndexedParquetMetadata(FileMetaData fileMetaData, List<BlockMetaData> blocks, List<LongList> rowIdsList) {
        super(fileMetaData, blocks);
        this.rowIdsList = rowIdsList;
    }

    public List<LongList> getRowIdsList() {
        return rowIdsList;
    }
}
