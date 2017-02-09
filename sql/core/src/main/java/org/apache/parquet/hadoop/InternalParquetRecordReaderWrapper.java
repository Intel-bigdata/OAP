package org.apache.parquet.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalParquetRecordReaderWrapper<T> implements InternalRecordReader<T> {

    private final InternalParquetRecordReader<T> internalParquetRecordReader;

    public InternalParquetRecordReaderWrapper(InternalParquetRecordReader<T> internalParquetRecordReader) {
        this.internalParquetRecordReader = internalParquetRecordReader;
    }

    @Override
    public void close() throws IOException {
        internalParquetRecordReader.close();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return internalParquetRecordReader.nextKeyValue();
    }

    @Override
    public Void getCurrentKey() throws IOException, InterruptedException {
        return internalParquetRecordReader.getCurrentKey();
    }

    @Override
    public T getCurrentValue() throws IOException, InterruptedException {
        return internalParquetRecordReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return internalParquetRecordReader.getProgress();
    }

    @Override
    public int getCurrentBlockIndex() {
        //TODO do this method
        return 0;
    }

    @Override
    public long getInternalRowId() {
        //TODO do this method
        return 0;
    }

    public void initOthers(List<List<Long>> rowIdsList) {
        // do nothing
    }

    @Override
    public void initialize(ParquetFileReader reader, Configuration configuration) throws IOException {
        this.internalParquetRecordReader.initialize(reader,configuration);
    }
}
