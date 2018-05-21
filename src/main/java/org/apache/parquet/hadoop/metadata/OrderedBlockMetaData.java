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

/**
 * OrderedBlockMetaData wrap BlockMetaData and add row group order id.
 */
public class OrderedBlockMetaData extends BlockMetaData {

  protected final int rowGroupId;

  protected final BlockMetaData meta;

  public OrderedBlockMetaData(int rowGroupId, BlockMetaData meta) {
    this.rowGroupId = rowGroupId;
    this.meta = meta;
  }

  @Override
  public void setPath(String path) {
    this.meta.setPath(path);
  }

  @Override
  public void setRowCount(long rowCount) {
    this.meta.setRowCount(rowCount);
  }

  @Override
  public void setTotalByteSize(long totalByteSize) {
    this.meta.setTotalByteSize(totalByteSize);
  }

  @Override
  public void addColumn(ColumnChunkMetaData column) {
    this.meta.addColumn(column);
  }

  @Override
  public String getPath() {
    return meta.getPath();
  }

  @Override
  public long getRowCount() {
    return meta.getRowCount();
  }

  @Override
  public long getTotalByteSize() {
    return meta.getTotalByteSize();
  }

  @Override
  public List<ColumnChunkMetaData> getColumns() {
    return meta.getColumns();
  }

  @Override
  public long getStartingPos() {
    return meta.getStartingPos();
  }

  @Override
  public String toString() {
    return meta.toString();
  }

  @Override
  public long getCompressedSize() {
    return meta.getCompressedSize();
  }

  public int getRowGroupId() {
    return rowGroupId;
  }

  public BlockMetaData getMeta() {
    return meta;
  }
}
