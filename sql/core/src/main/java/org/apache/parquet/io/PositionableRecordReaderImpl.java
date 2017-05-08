package org.apache.parquet.io;

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.it.unimi.dsi.fastutil.longs.LongList;

public class PositionableRecordReaderImpl<T> extends RecordReaderImplementation<T> {

    private final long recordMaxCount;

    private long recordsRead = 0;

    private Long currentRowId = -1L;

    private LongList rowIdList = null;

    private int currentIndex = 0;

    public PositionableRecordReaderImpl(MessageColumnIO root,
                                        RecordMaterializer<T> recordMaterializer,
                                        ColumnReadStoreImpl columnStore,
                                        long recordCount,
                                        LongList rowIdList) {
        super(root, recordMaterializer, false, columnStore);
        Preconditions.checkNotNull(rowIdList,"rowIdList can not be null.");
        Preconditions.checkArgument(!rowIdList.isEmpty(), "rowIdList must has item.");
        this.recordMaxCount = recordCount;
        this.rowIdList = rowIdList;
    }

    public T read() {
        currentRowId = rowIdList.getLong(currentIndex);
        seek(currentRowId);

        if (recordsRead == recordMaxCount) {
            return null;
        }

        ++recordsRead;
        ++currentIndex;
        return super.read();
    }

    private void seek(long position) {

        Preconditions.checkArgument(position >= recordsRead,
                "Not support seek to backward position, recordsRead: %s want to read: %s", recordsRead, position);
        Preconditions.checkArgument(position < recordMaxCount, "Seek position must less than recordCount");

        while (recordsRead < position) {
            State currentState = getState(0);
            do {
                ColumnReader columnReader = currentState.column;

                // has value, skip it
                if (columnReader.getCurrentDefinitionLevel() >= currentState.maxDefinitionLevel) {
                    columnReader.skip();
                }
                // change r,d state
                columnReader.consume();

                int nextR =
                        currentState.maxRepetitionLevel == 0 ? 0 : columnReader.getCurrentRepetitionLevel();
                currentState = currentState.getNextState(nextR);
            } while (currentState != null);
            recordsRead++;
        }
    }

}
