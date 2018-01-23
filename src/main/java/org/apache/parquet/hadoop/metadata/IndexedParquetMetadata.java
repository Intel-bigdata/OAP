package org.apache.parquet.hadoop.metadata;


import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntSet;

import java.util.List;

import com.google.common.collect.Lists;

public class IndexedParquetMetadata extends ParquetMetadata {

    private List<IntList> rowIdsList;

    public IndexedParquetMetadata(FileMetaData fileMetaData, List<BlockMetaData> blocks, List<IntList> rowIdsList) {
        super(fileMetaData, blocks);
        this.rowIdsList = rowIdsList;
    }

    public List<IntList> getRowIdsList() {
        return rowIdsList;
    }

    public static IndexedParquetMetadata from(ParquetMetadata footer, int[] globalRowIds) {
        List<BlockMetaData> inputBlockList = Lists.newArrayList();

        List<IntList> rowIdsList = Lists.newArrayList();
        int nextRowGroupStartRowId = 0;
        int totalCount = globalRowIds.length;
        int index = 0;

        for (BlockMetaData block : footer.getBlocks()) {
            int currentRowGroupStartRowId = nextRowGroupStartRowId;
            nextRowGroupStartRowId += block.getRowCount();
            IntList rowIdList = new IntArrayList();
            while (index < totalCount) {
                int globalRowGroupId = globalRowIds[index];
                if (globalRowGroupId < nextRowGroupStartRowId) {
                    rowIdList.add(globalRowGroupId - currentRowGroupStartRowId);
                    index++;
                } else {
                    break;
                }

            }
            if (!rowIdList.isEmpty()) {
                inputBlockList.add(block);
                rowIdsList.add(rowIdList);
            }
        }
        return new IndexedParquetMetadata(footer.getFileMetaData(), inputBlockList, rowIdsList);
    }
}
