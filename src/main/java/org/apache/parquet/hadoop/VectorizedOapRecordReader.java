package org.apache.parquet.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.schema.Type;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReaderWrapper;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

import com.google.common.collect.Maps;

public class VectorizedOapRecordReader extends SpecificOapRecordReaderBase<Object> {

    /**
     * Batch of rows that we assemble and the current index we've returned. Every time this
     * batch is used up (batchIdx == numBatched), we populated the batch.
     */
    protected int batchIdx = 0;
    protected int numBatched = 0;

    /**
     * For each request column, the reader to read this column. This is NULL if this column
     * is missing from the file, in which case we populate the attribute with NULL.
     */
    protected VectorizedColumnReaderWrapper[] columnReaders;

    /**
     * The number of rows that have been returned.
     */
    protected long rowsReturned;

    /**
     * The number of rows that have been reading, including the current in flight row group.
     */
    protected long totalCountLoadedSoFar = 0;

    /**
     * For each column, true if the column is missing in the file and we'll instead return NULLs.
     */
    protected boolean[] missingColumns;

    /**
     * columnBatch object that is used for batch decoding. This is created on first use and triggers
     * batched decoding. It is not valid to interleave calls to the batched interface with the row
     * by row RecordReader APIs.
     * This is only enabled with additional flags for development. This is still a work in progress
     * and currently unsupported cases will fail with potentially difficult to diagnose errors.
     * This should be only turned on for development to work on this feature.
     * <p>
     * When this is set, the code will branch early on in the RecordReader APIs. There is no shared
     * code between the path that uses the MR decoders and the vectorized ones.
     * <p>
     * TODOs:
     * - Implement v2 page formats (just make sure we create the correct decoders).
     */
    protected ColumnarBatch columnarBatch;

    /**
     * If true, this class returns batches instead of rows.
     */
    protected boolean returnColumnarBatch;

    /**
     * The default config on whether columnarBatch should be offheap.
     */
    protected static final MemoryMode DEFAULT_MEMORY_MODE = MemoryMode.ON_HEAP;


    public VectorizedOapRecordReader(
                        Path file,
                        Configuration configuration,
                        ParquetMetadata footer) {
        this.file = file;
        this.configuration = configuration;
        this.footer = footer;
    }

    /**
     * Implementation of RecordReader API.
     */
    @Override
    public void initialize() throws IOException, InterruptedException {
        super.initialize();
        initializeInternal();
    }

    @Override
    public void close() throws IOException {
        if (columnarBatch != null) {
            columnarBatch.close();
            columnarBatch = null;
        }
        super.close();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        resultBatch();

        if (returnColumnarBatch) return nextBatch();

        if (batchIdx >= numBatched) {
            if (!nextBatch()) return false;
        }
        ++batchIdx;
        return true;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        if (returnColumnarBatch) return columnarBatch;
        return columnarBatch.getRow(batchIdx - 1);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) rowsReturned / totalRowCount;
    }

    /**
     * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
     * This object is reused. Calling this enables the vectorized reader. This should be called
     * before any calls to nextKeyValue/nextBatch.
     */

    // Creates a columnar batch that includes the schema from the data files and the additional
    // partition columns appended to the end of the batch.
    // For example, if the data contains two columns, with 2 partition columns:
    // Columns 0,1: data columns
    // Column 2: partitionValues[0]
    // Column 3: partitionValues[1]
    public void initBatch(MemoryMode memMode, StructType partitionColumns,
                          InternalRow partitionValues) {
        StructType batchSchema = new StructType();
        for (StructField f: sparkSchema.fields()) {
            batchSchema = batchSchema.add(f);
        }
        if (partitionColumns != null) {
            for (StructField f : partitionColumns.fields()) {
                batchSchema = batchSchema.add(f);
            }
        }

        columnarBatch = ColumnarBatch.allocate(batchSchema, memMode);
        if (partitionColumns != null) {
            int partitionIdx = sparkSchema.fields().length;
            for (int i = 0; i < partitionColumns.fields().length; i++) {
                ColumnVectorUtils.populate(columnarBatch.column(i + partitionIdx), partitionValues, i);
                columnarBatch.column(i + partitionIdx).setIsConstant();
            }
        }

        // Initialize missing columns with nulls.
        for (int i = 0; i < missingColumns.length; i++) {
            if (missingColumns[i]) {
                columnarBatch.column(i).putNulls(0, columnarBatch.capacity());
                columnarBatch.column(i).setIsConstant();
            }
        }
    }

    public void initBatch() {
        initBatch(DEFAULT_MEMORY_MODE, null, null);
    }

    public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
        initBatch(DEFAULT_MEMORY_MODE, partitionColumns, partitionValues);
    }

    public ColumnarBatch resultBatch() {
        if (columnarBatch == null) initBatch();
        return columnarBatch;
    }

    /*
     * Can be called before any rows are returned to enable returning columnar batches directly.
     */
    public void enableReturningBatches() {
        returnColumnarBatch = true;
    }

    /**
     * Advances to the next batch of rows. Returns false if there are no more.
     */
    public boolean nextBatch() throws IOException {
        columnarBatch.reset();
        return nextBatch0();
    }

    protected boolean nextBatch0() throws IOException {
        if (rowsReturned >= totalRowCount) return false;
        checkEndOfRowGroup();

        int num = (int) Math.min((long) columnarBatch.capacity(), totalCountLoadedSoFar - rowsReturned);
        for (int i = 0; i < columnReaders.length; ++i) {
            if (columnReaders[i] == null) continue;
            columnReaders[i].readBatch(num, columnarBatch.column(i));
        }
        rowsReturned += num;
        columnarBatch.setNumRows(num);
        numBatched = num;
        batchIdx = 0;
        return true;
    }

    protected void initializeInternal() throws IOException, UnsupportedOperationException {
        missingColumns = new boolean[requestedSchema.getFieldCount()];
        for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
            Type t = requestedSchema.getFields().get(i);
            if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
                throw new UnsupportedOperationException("Complex types not supported.");
            }

            String[] colPath = requestedSchema.getPaths().get(i);
            if (fileSchema.containsPath(colPath)) {
                ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
                if (!fd.equals(requestedSchema.getColumns().get(i))) {
                    throw new UnsupportedOperationException("Schema evolution not supported.");
                }
                missingColumns[i] = false;
            } else {
                if (requestedSchema.getColumns().get(i).getMaxDefinitionLevel() == 0) {
                    // Column is missing in data but the required data is non-nullable. This file is invalid.
                    throw new IOException("Required column is missing in data file. Col: " +
                            Arrays.toString(colPath));
                }
                missingColumns[i] = true;
            }
        }
    }

    protected void checkEndOfRowGroup() throws IOException {
        if (rowsReturned != totalCountLoadedSoFar) return;
        PageReadStore pages = reader.readNextRowGroup();
        if (pages == null) {
            throw new IOException("expecting more rows but reached last block. Read "
                    + rowsReturned + " out of " + totalRowCount);
        }
        List<ColumnDescriptor> columns = requestedSchema.getColumns();
        columnReaders = new VectorizedColumnReaderWrapper[columns.size()];
        for (int i = 0; i < columns.size(); ++i) {
            if (missingColumns[i]) continue;
            columnReaders[i] = new VectorizedColumnReaderWrapper(
                    new VectorizedColumnReader(columns.get(i),
                    pages.getPageReader(columns.get(i))));
        }
        totalCountLoadedSoFar += pages.getRowCount();
    }
}
