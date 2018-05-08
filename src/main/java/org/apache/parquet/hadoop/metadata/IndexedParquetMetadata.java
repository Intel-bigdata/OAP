/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop.metadata;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

public class IndexedParquetMetadata extends ParquetMetadata {

    private List<IntList> rowIdsList;

    private IntList needRowGroupIds;

    public IndexedParquetMetadata(
        FileMetaData fileMetaData,
        IntList needRowGroupIds,
        List<BlockMetaData> blocks,
        List<IntList> rowIdsList) {
      super(fileMetaData, blocks);
      this.needRowGroupIds = needRowGroupIds;
      this.rowIdsList = rowIdsList;
    }

    public List<IntList> getRowIdsList() {
      return rowIdsList;
    }

    public IntList getNeedRowGroupIds() {
      return needRowGroupIds;
    }

    public static IndexedParquetMetadata from(ParquetMetadata footer, int[] globalRowIds) {
      List<BlockMetaData> inputBlockList = Lists.newArrayList();
      List<IntList> rowIdsList = Lists.newArrayList();
      IntList needRowGroupIds = new IntArrayList();
      int nextRowGroupStartRowId = 0;
      int totalCount = globalRowIds.length;
      int index = 0;
      List<BlockMetaData> blocks = footer.getBlocks();

      for (int id = 0; id < blocks.size(); id++) {
        BlockMetaData block = blocks.get(id);
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
          needRowGroupIds.add(id);
          inputBlockList.add(block);
          rowIdsList.add(rowIdList);
        }
      }
      return new IndexedParquetMetadata(
        footer.getFileMetaData(),
        needRowGroupIds,
        inputBlockList,
        rowIdsList);
    }
}
