/*******************************************************************************
 * Copyright 2020 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
 
package org.apache.spark.ml.util;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A mini-batch of data that can be converted to DALMatrix.
 */
public class DataBatch {
  
  /** The offset of each rows in the matrix */
  final long[] rowOffset;
  /** value of each non-missing entry in the matrix */
  final double[] values ;
  /** feature columns */
  final int numCols;

  DataBatch(long[] rowOffset,
            double[] values, int numCols) {
    this.rowOffset = rowOffset;
    this.values = values;
    this.numCols = numCols;
  }

  static public class BatchIterator implements Iterator<DataBatch> {
    private final Iterator<double[]> base;
    private final int batchSize;

    public BatchIterator(Iterator<double[]> base, int batchSize) {
      this.base = base;
      this.batchSize = batchSize;
    }

    @Override
    public boolean hasNext() {
      return base.hasNext();
    }

    @Override
    public DataBatch next() {
      try {
        int numRows = 0;
        int numCols  = -1;
        List<double[]> batch = new ArrayList<>(batchSize);
        while (base.hasNext() && batch.size() < batchSize) {
          double[] curValue = base.next();
          if (numCols == -1) {
            numCols = curValue.length;
          } else if (numCols != curValue.length) {
            throw new RuntimeException("Feature size is not the same");
          }
          batch.add(curValue);
		  
          numRows++;
        }

        long[] rowOffset = new long[numRows];
        double[] values = new double[numRows * numCols];

        int offset = 0;
        for (int i = 0; i < batch.size(); i++) {
          double[] curValue = batch.get(i);
          rowOffset[i] = i;
          System.arraycopy(curValue, 0, values, offset,
            curValue.length);
          offset += curValue.length;
        }

        return new DataBatch(rowOffset, values, numCols);
      } catch (RuntimeException runtimeError) {
     
        return null;
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("DataBatch.BatchIterator.remove");
    }
  }
}
