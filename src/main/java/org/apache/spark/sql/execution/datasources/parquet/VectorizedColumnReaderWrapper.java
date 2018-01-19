package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;

import org.apache.spark.sql.execution.vectorized.ColumnVector;

public class VectorizedColumnReaderWrapper {

    private VectorizedColumnReader reader;

    public VectorizedColumnReaderWrapper(VectorizedColumnReader reader) {
        this.reader = reader;
    }

    public void readBatch(int total, ColumnVector column) throws IOException {
        reader.readBatch(total, column);
    }

}
