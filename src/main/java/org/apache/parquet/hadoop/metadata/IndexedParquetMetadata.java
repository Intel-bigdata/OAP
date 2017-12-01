package org.apache.parquet.hadoop.metadata;


import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;

public class IndexedParquetMetadata extends ParquetMetadata {

    private List<IntList> rowIdsList;

    public IndexedParquetMetadata(FileMetaData fileMetaData, List<BlockMetaData> blocks, List<IntList> rowIdsList) {
        super(fileMetaData, blocks);
        this.rowIdsList = rowIdsList;
    }

    public List<IntList> getRowIdsList() {
        return rowIdsList;
    }
}
