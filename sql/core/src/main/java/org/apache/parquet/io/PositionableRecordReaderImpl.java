package org.apache.parquet.io;

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.io.api.RecordMaterializer;

import java.util.Iterator;
import java.util.List;

public class PositionableRecordReaderImpl<T> extends SRecordReaderImplementation<T> {

    protected final long recordMaxCount;

    private long recordsRead = 0;

    protected Long currentRowId = -1L;

    private Iterator<Long> rowIdIter = null;

    public PositionableRecordReaderImpl(SColumnMessageIO root,
                                        RecordMaterializer<T> recordMaterializer,
                                        ColumnReadStoreImpl columnStore,
                                        long recordCount,
                                        List<Long> rowIdList
    ) {
        super(root, recordMaterializer, columnStore);
        this.recordMaxCount = recordCount;
        Preconditions.checkArgument(rowIdList != null && !rowIdList.isEmpty(), "rowIdList must has item.");
        this.rowIdIter = rowIdList.iterator();
    }

    public T read() {
        currentRowId = rowIdIter.next();
        seek(currentRowId);

        if (recordsRead == recordMaxCount) {
            return null;
        }

        ++recordsRead;
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

                // currentLevel = depth + 1 at this point
                // set the current value
                if (columnReader.getCurrentDefinitionLevel() >= currentState.maxDefinitionLevel) {
                    columnReader.skip();
                }
                columnReader.consume();

                // Based on repetition level work out next state to go to
                int nextR =
                        currentState.maxRepetitionLevel == 0 ? 0 : columnReader.getCurrentRepetitionLevel();
                currentState = currentState.getNextState(nextR);
            } while (currentState != null);
            recordsRead++;
        }
    }

}
