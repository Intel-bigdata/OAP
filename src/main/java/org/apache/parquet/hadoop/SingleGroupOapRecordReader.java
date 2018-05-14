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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetFooter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SingleGroupOapRecordReader extends VectorizedOapRecordReader {

    private int blockId;
    private int rowGroupCount;

    public SingleGroupOapRecordReader(
        Path file,
        Configuration configuration,
        ParquetFooter footer,
        int blockId,
        int rowGroupCount) {
      super(file, configuration, footer);
      this.blockId = blockId;
      this.rowGroupCount = rowGroupCount;
    }

    /**
     * Override initialize method, init footer if need,
     * then call super.initialize and initializeInternal
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize() throws IOException, InterruptedException {
      ParquetMetadata meta = footer.toParquetMetadata(blockId);
      // need't do filterRowGroups.
      initialize(meta, configuration, false);
      super.initializeInternal();
    }

    public void initBatch() {
      StructType batchSchema = new StructType();
      for (StructField f: sparkSchema.fields()) {
        batchSchema = batchSchema.add(f);
      }
      columnarBatch = ColumnarBatch.allocate(batchSchema, DEFAULT_MEMORY_MODE, rowGroupCount);
      // Initialize missing columns with nulls.
      for (int i = 0; i < missingColumns.length; i++) {
        if (missingColumns[i]) {
          columnarBatch.column(i).putNulls(0, columnarBatch.capacity());
          columnarBatch.column(i).setIsConstant();
        }
      }
    }
}
