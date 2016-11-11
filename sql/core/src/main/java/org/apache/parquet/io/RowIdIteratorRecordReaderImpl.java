package org.apache.parquet.io;

import java.util.Iterator;
import java.util.List;

import org.apache.parquet.Preconditions;

public class RowIdIteratorRecordReaderImpl<T> extends PositionableRecordReaderImpl<T> {

    private Iterator<Long> rowIdIter = null;

    private final long recordCount;

    public RowIdIteratorRecordReaderImpl(RecordReader<T> recordReader, List<Long> rowIdList,
            long recordCount) {
        super(recordReader, recordCount);
        Preconditions.checkArgument(rowIdList != null && !rowIdList.isEmpty(), "rowIdList must has item.");
        this.rowIdIter = rowIdList.iterator();
        this.recordCount = rowIdList.size();
    }

    @Override
    protected Long nextRowId() {
        return rowIdIter.next();
    }

    @Override
    public long getRecordCount() {
        return recordCount;
    }

}
