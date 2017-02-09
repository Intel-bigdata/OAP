package org.apache.parquet.hadoop.metadata;


import java.util.List;

public class IndexedParquetMetadata extends ParquetMetadata {

    private List<List<Long>> rowIdsList;

    public IndexedParquetMetadata(FileMetaData fileMetaData, List<BlockMetaData> blocks, List<List<Long>> rowIdsList) {
        super(fileMetaData, blocks);
        this.rowIdsList = rowIdsList;
    }

    public List<List<Long>> getRowIdsList() {
        return rowIdsList;
    }
}
