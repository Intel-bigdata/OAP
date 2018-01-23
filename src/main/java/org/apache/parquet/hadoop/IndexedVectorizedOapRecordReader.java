package org.apache.parquet.hadoop;

import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.IndexedParquetMetadata;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.sql.execution.datasources.oap.io.OapReadSupportImpl;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportHelper;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReaderWrapper;
import org.apache.spark.sql.types.StructType$;

import com.google.common.collect.Maps;

public class IndexedVectorizedOapRecordReader extends VectorizedOapRecordReader {

    private Map<Integer, IntList> idsMap = Maps.newHashMap();
    private int currentPageNumber;
    private int[] globalRowIds;
    private Iterator<IntList> rowIdsIter;
    private IntList currentIndexList;

    public IndexedVectorizedOapRecordReader(
            Path file,
            Configuration configuration,
            ParquetMetadata footer,
            int[] globalRowIds) {
        super(file, configuration, footer);
        this.globalRowIds = globalRowIds;
    }

    /**
     * Implementation of RecordReader API.
     */
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
    public boolean nextBatch() throws IOException {
        columnarBatch.reset();
        if (rowsReturned >= totalRowCount) {
            return false;
        }
        checkEndOfRowGroup();

        int num = (int) Math.min((long) columnarBatch.capacity(), totalCountLoadedSoFar - rowsReturned);
        for (int i = 0; i < columnReaders.length; ++i) {
            if (columnReaders[i] == null) {
                continue;
            }
            columnReaders[i].readBatch(num, columnarBatch.column(i));
        }
        rowsReturned += num;
        columnarBatch.setNumRows(num);
        numBatched = num;
        batchIdx = 0;
        return filterRowsWithIndex();
    }

    protected void checkEndOfRowGroup() throws IOException {
        // if idsMap is Empty, needn't read remaining data in this row group
        // rowsReturned = totalCountLoadedSoFar to skip remaining data
        if (idsMap.isEmpty()) {
            rowsReturned = totalCountLoadedSoFar;
        }
        super.checkEndOfRowGroup();
        this.currentIndexList = rowIdsIter.next();
        regroupRowIds();
        this.currentPageNumber = 0;
    }

    private boolean filterRowsWithIndex() throws IOException {
        IntList ids = idsMap.remove(currentPageNumber);
        if (ids == null || ids.isEmpty()) {
            currentPageNumber++;
            // correct logical ?
            return nextBatch();
        } else {
            int current = 0;
            for (Integer target : ids) {
                while (current < target) {
                    columnarBatch.markFiltered(current);
                    current++;
                }
                // skip this row
                current++;
            }
            // skip this row
            current++;
            while (current < numBatched) {
                columnarBatch.markFiltered(current);
                // skip this row
                current++;
            }
            currentPageNumber++;
            return true;
        }
    }

    private void regroupRowIds() {
        idsMap.clear();
        int pageSize = columnarBatch.capacity();
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
