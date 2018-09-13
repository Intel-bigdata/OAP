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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ParquetFooter;
import org.apache.parquet.schema.Type;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReaderWrapper;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class VectorizedOapRecordReader extends SpecificOapRecordReaderBase<Object> {

    // TODO: make this configurable.
    public static final int CAPACITY = 4 * 1024;

    /**
     * Batch of rows that we assemble and the current index we've returned. Every time this
     * batch is used up (batchIdx == numBatched), we populated the batch.
     * From VectorizedParquetRecordReader, change private to protected.
     */
    protected int batchIdx = 0;
    /**
     * From VectorizedParquetRecordReader, change private to protected
     */
    protected int numBatched = 0;

    /**
     * For each request column, the reader to read this column. This is NULL if this column
     * is missing from the file, in which case we populate the attribute with NULL.
     * From VectorizedParquetRecordReader, change private to protected,
     * wrapper VectorizedColumnReader.
     */
    protected VectorizedColumnReaderWrapper[] columnReaders;

    /**
     * The number of rows that have been returned.
     * From VectorizedParquetRecordReader, change private to protected.
     */
    protected long rowsReturned;

    /**
     * The number of rows that have been reading, including the current in flight row group.
     * From VectorizedParquetRecordReader, change private to protected.
     */
    protected long totalCountLoadedSoFar = 0;

    /**
     * For each column, true if the column is missing in the file and we'll instead return NULLs.
     * From VectorizedParquetRecordReader, change private to protected.
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
     * From VectorizedParquetRecordReader, change private to protected.
     */
    protected ColumnarBatch columnarBatch;

    protected WritableColumnVector[] columnVectors;

    /**
     * If true, this class returns batches instead of rows.
     * From VectorizedParquetRecordReader, change private to protected.
     */
    protected boolean returnColumnarBatch;

    /**
     * The default config on whether columnarBatch should be offheap.
     * From VectorizedParquetRecordReader, change private to protected.
     */
    protected static final MemoryMode DEFAULT_MEMORY_MODE = MemoryMode.ON_HEAP;

    /**
     * SpecificOapRecordReaderBase need
     * configuration and footer use by initialize method,
     * not belong to SpecificParquetRecordReaderBase
     */
    protected Configuration configuration;
    protected ParquetFooter footer;

    /**
     * VectorizedOapRecordReader Contructor
     * new method
     * @param file
     * @param configuration
     * @param footer
     */
    public VectorizedOapRecordReader(
        Path file,
        Configuration configuration,
        ParquetFooter footer) {
      this.file = file;
      this.configuration = configuration;
      this.footer = footer;
    }

    public VectorizedOapRecordReader(
        Path file,
        Configuration configuration) throws IOException{
      this.file = file;
      this.configuration = configuration;
      this.footer = OapParquetFileReader.readParquetFooter(configuration, file);
    }

    /**
     * Override initialize method, init footer if need,
     * then call super.initialize and initializeInternal
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize() throws IOException, InterruptedException {
      // no index to use, try do filterRowGroups to skip rowgroups.
      initialize(footer.toParquetMetadata(), configuration, true);
      initializeInternal();
    }

    /**
     * From VectorizedParquetRecordReader,no change.
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
      if (columnarBatch != null) {
        columnarBatch.close();
        columnarBatch = null;
      }
      super.close();
    }

    /**
     * From VectorizedParquetRecordReader,no change.
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
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

    /**
     * From VectorizedParquetRecordReader,no change.
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
      if (returnColumnarBatch) return columnarBatch;
      return columnarBatch.getRow(batchIdx - 1);
    }

    /**
     * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
     * This object is reused. Calling this enables the vectorized reader. This should be called
     * before any calls to nextKeyValue/nextBatch.
     * From VectorizedParquetRecordReader,no change.
     */

    // Creates a columnar batch that includes the schema from the data files and the additional
    // partition columns appended to the end of the batch.
    // For example, if the data contains two columns, with 2 partition columns:
    // Columns 0,1: data columns
    // Column 2: partitionValues[0]
    // Column 3: partitionValues[1]
    public void initBatch(
        MemoryMode memMode,
        StructType partitionColumns,
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

//      columnarBatch = ColumnarBatch.allocate(batchSchema, memMode);


        if (memMode == MemoryMode.OFF_HEAP) {
            columnVectors = OffHeapColumnVector.allocateColumns(CAPACITY, batchSchema);
        } else {
            columnVectors = OnHeapColumnVector.allocateColumns(CAPACITY, batchSchema);
        }
        columnarBatch = new ColumnarBatch(columnVectors);

      if (partitionColumns != null) {
        int partitionIdx = sparkSchema.fields().length;
        for (int i = 0; i < partitionColumns.fields().length; i++) {
           ColumnVectorUtils.populate(columnVectors[i + partitionIdx],
             partitionValues, i);
            columnVectors[i + partitionIdx].setIsConstant();
        }
      }

      // Initialize missing columns with nulls.
      for (int i = 0; i < missingColumns.length; i++) {
        if (missingColumns[i]) {
            columnVectors[i].putNulls(0, CAPACITY);
            columnVectors[i].setIsConstant();
        }
      }
    }

    /**
     * From VectorizedParquetRecordReader,no change.
     */
    public void initBatch() {
      initBatch(DEFAULT_MEMORY_MODE, null, null);
    }

    /**
     * From VectorizedParquetRecordReader,no change.
     * @param partitionColumns
     * @param partitionValues
     */
    public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
      initBatch(DEFAULT_MEMORY_MODE, partitionColumns, partitionValues);
    }

    /**
     * From VectorizedParquetRecordReader,no change.
     * @return
     */
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
     * From VectorizedParquetRecordReader, no change.
     */
    public boolean nextBatch() throws IOException {
        for (WritableColumnVector vector : columnVectors) {
            vector.reset();
        }
        columnarBatch.setNumRows(0);

      if (rowsReturned >= totalRowCount) return false;
      checkEndOfRowGroup();

      int num = (int) Math.min((long) CAPACITY,
        totalCountLoadedSoFar - rowsReturned);
      for (int i = 0; i < columnReaders.length; ++i) {
        if (columnReaders[i] == null) continue;
        columnReaders[i].readBatch(num, columnVectors[i]);
      }
      rowsReturned += num;
      columnarBatch.setNumRows(num);
      numBatched = num;
      batchIdx = 0;
      return true;
    }

    /**
     * From VectorizedParquetRecordReader, chanage private -> protected.
     * @throws IOException
     * @throws UnsupportedOperationException
     */
    protected void initializeInternal() throws IOException, UnsupportedOperationException {
      missingColumns = new boolean[requestedSchema.getFieldCount()];
      for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
        Type t = requestedSchema.getFields().get(i);
        if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
          throw new UnsupportedOperationException(
            "Complex types " + t.getName() + " not supported.");
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
            // Column is missing in data but the required data is non-nullable.
            // This file is invalid.
            throw new IOException("Required column is missing in data file. Col: " +
              Arrays.toString(colPath));
          }
          missingColumns[i] = true;
        }
      }
    }

    /**
     * From VectorizedParquetRecordReader, chanage private -> protected.
     * @throws IOException
     */
    protected void checkEndOfRowGroup() throws IOException {
      if (rowsReturned != totalCountLoadedSoFar) return;
      readNextRowGroup();
    }

    protected void readNextRowGroup() throws IOException {
      PageReadStore pages = reader.readNextRowGroup();
      initColumnReaders(pages);
    }

    protected void initColumnReaders(PageReadStore pages) throws IOException {
      if (pages == null) {
        throw new IOException("expecting more rows but reached last block. Read "
        + rowsReturned + " out of " + totalRowCount);
      }
      List<ColumnDescriptor> columns = requestedSchema.getColumns();
      List<Type> types = requestedSchema.asGroupType().getFields();
      columnReaders = new VectorizedColumnReaderWrapper[columns.size()];
      for (int i = 0; i < columns.size(); ++i) {
        if (missingColumns[i]) continue;
        columnReaders[i] = new VectorizedColumnReaderWrapper(
          new VectorizedColumnReader(columns.get(i), types.get(i).getOriginalType(),
          pages.getPageReader(columns.get(i)), TimeZone.getDefault()));
      }
      totalCountLoadedSoFar += pages.getRowCount();
    }
}
