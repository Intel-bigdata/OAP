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
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.IndexedParquetMetadata;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.oap.io.OapReadSupportImpl;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportHelper;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;

import com.google.common.collect.Maps;

public class IndexedVectorizedOapRecordReader extends VectorizedOapRecordReader {

    private Map<Integer, IntList> idsMap = Maps.newHashMap();
    private int currentPageNumber;
    private int[] globalRowIds;
    private Iterator<IntList> rowIdsIter;

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
                configuration, toSetMultiMap(fileMetadata), fileSchema));
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

    protected void checkEndOfRowGroup() throws IOException {
        if (rowsReturned != totalCountLoadedSoFar) return;
        super.readNextRowGroup();
        this.collectRowIdsToPages();
    }

    @Override
    public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
        super.initBatch(partitionColumns, partitionValues);
        columnarBatch.markAllFiltered();
    }

    private boolean filterRowsWithIndex() throws IOException {
        IntList ids = idsMap.remove(currentPageNumber);
        if (ids == null || ids.isEmpty()) {
            currentPageNumber++;
            return this.nextBatch();
        } else {
            for (Integer rowid : ids) {
                columnarBatch.markValid(rowid);
            }
            currentPageNumber++;
            return true;
        }
    }

    private void collectRowIdsToPages() {
        this.idsMap.clear();
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
