package org.apache.parquet.hadoop;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

public class PageContext {

    private static final IntList DEFAULT_VALUE = IntArrayList.wrap(new int[] {1});

    private Map<Integer, IntList> idsMap = Maps.newHashMap();

    private boolean returnColumnarBatch = false;

    private int pageSize;

    PageContext(int pageSize) {
        this.pageSize = pageSize;
    }

    public boolean isEmpty() {
        return idsMap.isEmpty();
    }

    public IntList remove(int key) {
        return idsMap.remove(key);
    }

    public void put(IntList idList) {
      if (returnColumnarBatch) {
        for (int rowId : idList) {
          coarseGrainedPut(rowId);
        }
      } else {
        for (int rowId : idList) {
          fineGrainedPut(rowId);
        }
      }
    }

    public void enableReturnColumnarBatch() {
        this.returnColumnarBatch = true;
    }

    private void coarseGrainedPut(int rowId) {
      int pageNumber = rowId / pageSize;
      if (!idsMap.containsKey(pageNumber)) {
        idsMap.put(pageNumber, DEFAULT_VALUE);
      }
    }

    private void fineGrainedPut(int rowId) {
      int pageNumber = rowId / pageSize;
      if (idsMap.containsKey(pageNumber)) {
        idsMap.get(pageNumber).add(rowId - pageNumber * pageSize);
      } else {
        IntArrayList ids = new IntArrayList(pageSize / 2);
        ids.add(rowId - pageNumber * pageSize);
        idsMap.put(pageNumber, ids);
      }
    }
}
