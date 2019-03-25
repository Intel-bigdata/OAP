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

package org.apache.spark.sql.execution.datasources.oap.io;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.*;
import org.apache.orc.storage.serde2.io.HiveDecimalWritable;
import org.apache.parquet.hadoop.ParquetFiberDataReader;
import org.apache.parquet.hadoop.metadata.IndexedStripeMeta;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntListIterator;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.oap.filecache.DataFiberId;
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache;
import org.apache.spark.sql.execution.datasources.orc.OrcColumnVector;
import org.apache.spark.sql.execution.datasources.orc.OrcColumnVectorAllocator;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.oap.OapRuntime$;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.datanucleus.store.types.simple.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexedOrcCacheReader extends OrcCacheReader {
  private static final Logger LOG = LoggerFactory.getLogger(IndexedOrcCacheReader.class);

  private int[] rowIds;
  private List<IndexedStripeMeta> validStripes = new java.util.ArrayList<IndexedStripeMeta>();
//  private IntList validStripesIndex = new IntArrayList();
  private int curStripeIndex = -1;
//  private int validStripesLength;

  private int currentBatchNumInStripe = 0;

  private Map<Integer, IntList> mapBatchIds = new HashMap<Integer, IntList>();

  private static final String IDS_MAP_STATE_ERROR_MSG =
    "The divideRowIdsIntoPages method should not be called when mapBatchIds is not empty.";
  private static final String IDS_ITER_STATE_ERROR_MSG =
    "The divideRowIdsIntoPages method should not be called when currentIndexList is Empty.";

  public IndexedOrcCacheReader(Configuration configuration,
                               OrcDataFileMeta meta,
                               OrcDataFile dataFile,
                               int[] requiredColumnIds,
                               boolean useOffHeap,
                               boolean copyToSpark,
                               int[] rowIds) {
    super(configuration, meta, dataFile, requiredColumnIds, useOffHeap, copyToSpark);
    this.rowIds = rowIds;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    // nothing required
  }

  /**
   * Initialize ORC file reader and batch record reader.
   * Please note that `initBatch` is needed to be called after this.
   * This method is customized by Oap.
   */
  @Override
  public void initialize(
      Path file, Configuration conf) throws IOException {


    int nextStripeStartRowId = 0;
    int totalCount = rowIds.length;
    int index = 0;
    for (int i = 0; i < meta.listStripeInformation().size(); ++i) {
      StripeInformation stripe = meta.listStripeInformation().get(i);
      int currentStripeStartRowId = nextStripeStartRowId;
      nextStripeStartRowId += stripe.getNumberOfRows();
      IntList rowIdList = new IntArrayList();
      while (index < totalCount) {
        int globalRowId = rowIds[index];
        if (globalRowId < nextStripeStartRowId) {
          rowIdList.add(globalRowId - currentStripeStartRowId);
          index++;
        } else {
          break;
        }
      }
      if (!rowIdList.isEmpty()) {
//        validStripesIndex.add(i);
        validStripes.add(new IndexedStripeMeta(i, stripe, rowIdList));
      }
    }

    for(IndexedStripeMeta stripeMeta : validStripes)
    {
      totalRowCount += stripeMeta.stripe.getNumberOfRows();
    }

//    validStripesLength = validStripesIndex.size();
  }

  /**
   * Return true if there exists more data in the next batch. If exists, prepare the next batch
   * by copying from ORC VectorizedRowBatch columns to Spark ColumnarBatch columns.
   */
  public boolean nextBatch() throws IOException {
    // if idsMap is Empty, needn't read remaining data in this row group
    // rowsReturned = totalCountLoadedSoFar to skip remaining data
    if (mapBatchIds.isEmpty()) {
      rowsReturned = totalCountLoadedSoFar;
    }

    if (rowsReturned >= totalRowCount) {
      return false;
    }

    checkEndOfRowGroup();

    IntList ids = mapBatchIds.remove(currentBatchNumInStripe);
    currentBatchNumInStripe += 1;

    while (ids == null || ids.isEmpty()) {
      skipBatchInternal();
      ids = mapBatchIds.remove(currentBatchNumInStripe);
      currentBatchNumInStripe += 1;
    }

    nextBatchInternal();
    return true;
  }

  @Override
  protected void readNextRowGroup() {
    curStripeIndex += 1;
    if (curStripeIndex >= validStripes.size()) {
      throw new IndexOutOfBoundsException("over validStripesIndex's boundary.\n");
    }

    StripeInformation stripeInformation = meta.listStripeInformation().get(validStripes.get(curStripeIndex).stripeId);
    long loadFiberTime = 0L;
    long loadDicTime = 0L;


    int rowCount = (int)stripeInformation.getNumberOfRows();
    for (int i = 0; i < requiredColumnIds.length; ++i) {
      long start = System.nanoTime();
      FiberCache fiberCache =
              OapRuntime$.MODULE$.getOrCreate().fiberCacheManager().get(new DataFiberId(dataFile, requiredColumnIds[i], validStripes.get(curStripeIndex).stripeId));
      long end = System.nanoTime();
      loadFiberTime += (end - start);
      dataFile.update(requiredColumnIds[i], fiberCache);
      long start2 = System.nanoTime();
      fiberReaders[i] = ParquetDataFiberReader$.MODULE$.apply(fiberCache.getBaseOffset(),
              columnarBatch.column(i).dataType(), rowCount);
      long end2 = System.nanoTime();
      loadDicTime += (end2 - start2);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("load row group with cols = {}, loadFiberTime = {} , loadDicTime = {} ",
              columnarBatch.numCols(), loadFiberTime, loadDicTime);
    }

    totalCountLoadedSoFar += rowCount;
    currentRowGroupRowsReturned = 0;

    divideRowIdsIntoPages(validStripes.get(curStripeIndex).needRowIdsinStripe);
  }

  private void divideRowIdsIntoPages(IntList needRowIdsinStripe) {
    Preconditions.checkState(mapBatchIds.isEmpty(), IDS_MAP_STATE_ERROR_MSG);
    Preconditions.checkState(!needRowIdsinStripe.isEmpty(), IDS_ITER_STATE_ERROR_MSG);
    this.currentBatchNumInStripe = 0;
    IntListIterator iterator = needRowIdsinStripe.iterator();
    while (iterator.hasNext()) {
      int rowId = iterator.nextInt();
      int batchNum = rowId / CAPACITY;
      if (mapBatchIds.containsKey(batchNum)) {
        mapBatchIds.get(batchNum).add(rowId - batchNum * CAPACITY);
      }
      else {
        IntArrayList ids = new IntArrayList(CAPACITY / 2);
        ids.add(rowId - batchNum * CAPACITY);
        mapBatchIds.put(batchNum, ids);
      }
    }
  }

  protected void skipBatchInternal() {
    int num = CAPACITY;
    rowsReturned += num;
    currentRowGroupRowsReturned += num;
  }

}
