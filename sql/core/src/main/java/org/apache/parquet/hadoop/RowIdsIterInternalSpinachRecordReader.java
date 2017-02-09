package org.apache.parquet.hadoop;

import java.util.Iterator;
import java.util.List;

import org.apache.parquet.Preconditions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.PositionableRecordReader;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.RowIdIteratorRecordReaderImpl;

public class RowIdsIterInternalSpinachRecordReader<T> extends InternalSpinachRecordReader<T> {

    protected Iterator<List<Long>> rowIdsIter = null;

    public RowIdsIterInternalSpinachRecordReader(ReadSupport<T> readSupport) {
        super(readSupport);
    }

    protected void initOthers(List<List<Long>> rowIdsList, List<BlockMetaData> blocks) {
        Preconditions.checkArgument(rowIdsList != null && !rowIdsList.isEmpty(), "RowIdsList must not empty");
        this.rowIdsIter = rowIdsList.iterator();
        for (List<Long> rowIdList : rowIdsList) {
            total += rowIdList.size();
        }
    }

    @Override
    protected PositionableRecordReader<T> getPositionableRecordReader(RecordReader<T> recordReader,
            long rowCount) {
        return new RowIdIteratorRecordReaderImpl<>(recordReader, rowIdsIter.next(), rowCount);
    }

}
