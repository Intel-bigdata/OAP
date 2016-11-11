package org.apache.parquet.hadoop;

import java.util.List;

import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.api.SpinachReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.CounterRecordReaderImpl;
import org.apache.parquet.io.PositionableRecordReader;
import org.apache.parquet.io.RecordReader;

public class CounterInternalSpinachRecordReader<T> extends InternalSpinachRecordReader<T> {

    public CounterInternalSpinachRecordReader(SpinachReadSupport<T> readSupport, Filter filter) {
        super(readSupport, filter);
    }

    public CounterInternalSpinachRecordReader(SpinachReadSupport<T> readSupport) {
        super(readSupport);
    }

    @Override
    protected PositionableRecordReader<T> getPositionableRecordReader(RecordReader<T> recordReader,
            long rowCount) {
        return new CounterRecordReaderImpl<T>(recordReader, rowCount);
    }

    @Override
    protected void initOthers(List<List<Long>> rowIdsList, List<BlockMetaData> blocks) {
        for (BlockMetaData block : blocks) {
            total += block.getRowCount();
        }
    }

}
