package org.apache.parquet.hadoop.metadata;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

public class ParquetFooter {

  private final FileMetaData fileMetaData;
  private final List<OrderedBlockMetaData> blocks;

  public ParquetFooter(
      FileMetaData fileMetaData,
      List<OrderedBlockMetaData> blocks) {
    this.fileMetaData = fileMetaData;
    this.blocks = blocks;
  }

  public FileMetaData getFileMetaData() {
    return fileMetaData;
  }

  public List<OrderedBlockMetaData> getBlocks() {
    return blocks;
  }

  public static ParquetFooter from(ParquetMetadata meta) {
    List<OrderedBlockMetaData> blockMetas = Lists.newArrayList();
    List<BlockMetaData> blocks = meta.getBlocks();
    for (int order = 0; order < blocks.size(); order++) {
      blockMetas.add(new OrderedBlockMetaData(order, blocks.get(order)));
    }
    return new ParquetFooter(meta.getFileMetaData(), blockMetas);
  }

  public ParquetMetadata toParquetMetadata() {
    List<BlockMetaData> validBlocks = Lists.newArrayList();
    validBlocks.addAll(blocks);
    return new ParquetMetadata(fileMetaData, validBlocks);
  }

  public ParquetMetadata toParquetMetadata(int rowGroupId) {
    List<BlockMetaData> validBlocks = Lists.newArrayList();
    validBlocks.add(blocks.get(rowGroupId));
    return new ParquetMetadata(fileMetaData, validBlocks);
  }


  public ParquetMetadata toParquetMetadata(int[] globalRowIds) {
    List<BlockMetaData> validBlocks = Lists.newArrayList();
    int nextRowGroupStartRowId = 0;
    int totalCount = globalRowIds.length;
    int index = 0;

    for (OrderedBlockMetaData block : blocks) {
      int currentRowGroupStartRowId = nextRowGroupStartRowId;
      nextRowGroupStartRowId += block.getRowCount();
      IntList rowIdList = new IntArrayList();
      while (index < totalCount) {
        int globalRowId = globalRowIds[index];
        if (globalRowId < nextRowGroupStartRowId) {
          rowIdList.add(globalRowId - currentRowGroupStartRowId);
          index++;
        } else {
          break;
        }
      }
      if (!rowIdList.isEmpty()) {
        validBlocks.add(new IndexedBlockMetaData(block, rowIdList));
      }
    }
    return new ParquetMetadata(fileMetaData, validBlocks);
  }
}
