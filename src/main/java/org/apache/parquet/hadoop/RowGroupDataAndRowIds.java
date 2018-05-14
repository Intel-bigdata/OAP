package org.apache.parquet.hadoop;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

public class RowGroupDataAndRowIds {
  private PageReadStore pageReadStore;
  private IntList rowIds;

  public RowGroupDataAndRowIds(
          PageReadStore pageReadStore,
      IntList rowIds) {
    this.pageReadStore = pageReadStore;
    this.rowIds = rowIds;
  }

  public PageReadStore getPageReadStore() {
    return pageReadStore;
  }

  public IntList getRowIds() {
    return rowIds;
  }
}
