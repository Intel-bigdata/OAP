package org.apache.parquet.io;

public class CounterRecordReaderImpl<T> extends PositionableRecordReaderImpl<T> {

    public CounterRecordReaderImpl(RecordReader<T> recordReader, long recordCount) {
        super(recordReader, recordCount);
    }

    @Override
    protected Long nextRowId() {
        return ++currentRowId;
    }

    @Override
    public long getRecordCount() {
        return recordMaxCount;
    }

}
