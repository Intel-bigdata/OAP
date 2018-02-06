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
package org.apache.parquet.hadoop;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.IndexedParquetMetadata;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.utils.Collections3;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntListIterator;
import org.apache.spark.sql.execution.datasources.oap.io.OapReadSupportImpl;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportHelper;
import org.apache.spark.sql.types.StructType$;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class IndexedVectorizedOapRecordReader extends VectorizedOapRecordReader {

    private Map<Integer, IntList> idsMap = Maps.newHashMap();
    private int currentPageNumber;
    private int[] globalRowIds;
    private Iterator<IntList> rowIdsIter;
    // for returnColumnarBatch is false.
    private IntListIterator batchIdsIter;
    private static final String IDS_MAP_STATE_ERROR_MSG =
            "The divideRowIdsIntoPages method should not be called when idsMap is not empty.";
    private static final String IDS_ITER_STATE_ERROR_MSG =
            "The divideRowIdsIntoPages method should not be called when rowIdsIter hasNext if false.";
    private static final String BATCH_IDS_ITER_STATE_ERROR_MSG =
            "batchIdsIter.hasNext must true when call getCurrentValue && returnColumnarBatch is false.";

    public IndexedVectorizedOapRecordReader(
            Path file,
            Configuration configuration,
            ParquetMetadata footer,
            int[] globalRowIds) {
        super(file, configuration, footer);
        this.globalRowIds = globalRowIds;
    }

    @Override
    public void initialize() throws IOException, InterruptedException {
        if (this.footer == null) {
            footer = readFooter(configuration, file, NO_FILTER);
        }

        IndexedParquetMetadata indexedFooter = IndexedParquetMetadata.from(footer, globalRowIds);
        this.fileSchema = footer.getFileMetaData().getSchema();
        Map<String, String> fileMetadata = footer.getFileMetaData().getKeyValueMetaData();
        ReadSupport.ReadContext readContext = new OapReadSupportImpl().init(new InitContext(
                configuration, Collections3.toSetMultiMap(fileMetadata), fileSchema));
        this.requestedSchema = readContext.getRequestedSchema();
        String sparkRequestedSchemaString =
                configuration.get(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA());
        this.sparkSchema = StructType$.MODULE$.fromString(sparkRequestedSchemaString);
        this.reader = ParquetFileReader.open(configuration, file, indexedFooter);
        this.reader.setRequestedSchema(requestedSchema);
        this.rowIdsIter = indexedFooter.getRowIdsList().iterator();
        for (BlockMetaData block : indexedFooter.getBlocks()) {
            this.totalRowCount += block.getRowCount();
        }
        super.initializeInternal();
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        if (returnColumnarBatch) return columnarBatch;
        // if returnColumnarBatch, use batchIdsIter && batchIdsIter.hasNext must true
        Preconditions.checkState(batchIdsIter.hasNext(), BATCH_IDS_ITER_STATE_ERROR_MSG);
        return columnarBatch.getRow(batchIdsIter.next());
    }

    /**
     * Advances to the next batch of rows. Returns false if there are no more.
     */
    @Override
    public boolean nextBatch() throws IOException {
        // if idsMap is Empty, needn't read remaining data in this row group
        // rowsReturned = totalCountLoadedSoFar to skip remaining data
        if (idsMap.isEmpty()) {
            rowsReturned = totalCountLoadedSoFar;
        }
        return super.nextBatch() && filterRowsWithIndex();
    }

    @Override
    protected void checkEndOfRowGroup() throws IOException {
        if (rowsReturned != totalCountLoadedSoFar) return;
        // if rowsReturned == totalCountLoadedSoFar
        // readNextRowGroup & divideRowIdsIntoPages
        super.readNextRowGroup();
        this.divideRowIdsIntoPages();
    }
    
    private boolean filterRowsWithIndex() throws IOException {
        IntList ids = idsMap.remove(currentPageNumber);
        if (ids == null || ids.isEmpty()) {
            currentPageNumber++;
            return this.nextBatch();
        } else {
            // if returnColumnarBatch, mark columnarBatch filtered status.
            // else assignment batchIdsIter.
            if(returnColumnarBatch) {
                columnarBatch.markAllFiltered();
                for (Integer rowid : ids) {
                    columnarBatch.markValid(rowid);
                }
            } else {
                batchIdsIter = ids.iterator();
                numBatched = ids.size();
            }
            currentPageNumber++;
            return true;
        }
    }

    private void divideRowIdsIntoPages() {
        Preconditions.checkState(idsMap.isEmpty(), IDS_MAP_STATE_ERROR_MSG);
        Preconditions.checkState(rowIdsIter.hasNext(), IDS_ITER_STATE_ERROR_MSG);
        this.currentPageNumber = 0;
        int pageSize = columnarBatch.capacity();
        IntList currentIndexList = rowIdsIter.next();
        for (int rowId : currentIndexList) {
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
}
