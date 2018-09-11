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
package org.apache.spark.sql.vectorized;

import java.util.*;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow;

/**
 * This class wraps multiple ColumnVectors as a row-wise table. It provides a row view of this
 * batch so that Spark can access the data row by row. Instance of it is meant to be reused during
 * the entire data loading process.
 */
@InterfaceStability.Evolving
public final class ColumnarBatch {
  //filteredRows maximum capacity,from RowBasedKeyValueBatch.java,through the analysis
  private static final int DEFAULT_CAPACITY = 1 << 16;

  private int numRows;
  private final ColumnVector[] columns;

  // Staging row returned from `getRow`.
  private final MutableColumnarRow row;

  // True if the row is filtered.
  private final boolean[] filteredRows = new boolean[DEFAULT_CAPACITY];

  // Total number of rows that have been filtered.
  private int numRowsFiltered = 0;

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after
   * calling this. This must be called at the end to clean up memory allocations.
   */
  public void close() {
    for (ColumnVector c: columns) {
      c.close();
    }
  }

  // For DataSourceScanExec
  public boolean isFiltered(int rowId) {
    return filteredRows[rowId];
  }

  /**
   * Returns an iterator over the rows in this batch.
   */
  public Iterator<InternalRow> rowIterator() {
    final int maxRows = numRows;
    final MutableColumnarRow row = new MutableColumnarRow(columns);
    return new Iterator<InternalRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        return rowId < maxRows;
      }

      @Override
      public InternalRow next() {
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        row.rowId = rowId++;
        return row;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Returns an iterator over the rows in this batch. for OAP
   */
  public Iterator<InternalRow> rowOapIterator() {
    final int maxRows = numRows;
    final MutableColumnarRow row = new MutableColumnarRow(columns);
    return new Iterator<InternalRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        while (rowId < maxRows && ColumnarBatch.this.filteredRows[rowId]) {
          ++rowId;
        }
        return rowId < maxRows;
      }

      @Override
      public InternalRow next() {
        while (rowId < maxRows && ColumnarBatch.this.filteredRows[rowId]) {
          ++rowId;
        }
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        row.rowId = rowId++;
        return row;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Sets the number of rows in this batch.
   */
  public void setNumRows(int numRows) {
    this.numRows = numRows;
  }

  /**
   * Returns the number of columns that make up this batch.
   */
  public int numCols() { return columns.length; }

  /**
   * Returns the number of rows for read, including filtered rows.
   */
  public int numRows() { return numRows; }

  /**
   * Returns the column at `ordinal`.
   */
  public ColumnVector column(int ordinal) { return columns[ordinal]; }

  /**
   * Returns the row in this batch at `rowId`. Returned row is reused across calls.
   */
  public InternalRow getRow(int rowId) {
    assert(rowId >= 0 && rowId < numRows);
    row.rowId = rowId;
    return row;
  }

  /**
   * Marks this row not filtered out. This means a subsequent iteration over the rows
   * in this batch will include this row.
   * For IndexedVectorizedOapRecordReader.
   */
  public void markValid(int rowId) {
    assert(filteredRows[rowId]);
    filteredRows[rowId] = false;
    --numRowsFiltered;
  }

  /**
   * For IndexedVectorizedOapRecordReader.
   */
  public void markAllFiltered() {
    Arrays.fill(filteredRows, true);
    numRowsFiltered = numRows;
  }

  /**
   * Returns the number of valid rows.
   */
  public int numValidRows() {
    assert(numRowsFiltered <= numRows);
    return numRows - numRowsFiltered;
  }

  public ColumnarBatch(ColumnVector[] columns) {
    this.columns = columns;
    this.row = new MutableColumnarRow(columns);
  }
}
