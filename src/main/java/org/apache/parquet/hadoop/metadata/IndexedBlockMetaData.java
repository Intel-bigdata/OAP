package org.apache.parquet.hadoop.metadata;

import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

public class IndexedBlockMetaData extends OrderedBlockMetaData {

  private IntList needRowIds;

  public IndexedBlockMetaData(OrderedBlockMetaData orderedBlockMetaData, IntList needRowIds) {
    super(orderedBlockMetaData.rowGroupId, orderedBlockMetaData.meta);
    this.needRowIds = needRowIds;
  }

  public IntList getNeedRowIds() {
    return needRowIds;
  }
}
