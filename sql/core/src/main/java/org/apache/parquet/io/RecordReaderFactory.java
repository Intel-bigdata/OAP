package org.apache.parquet.io;


import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.it.unimi.dsi.fastutil.longs.LongList;

import java.util.List;

import static org.apache.parquet.Preconditions.checkNotNull;

public class RecordReaderFactory {

    public static <T> RecordReader<T> getRecordReader(MessageColumnIO root, PageReadStore columns,
                                               RecordMaterializer<T> recordMaterializer,
                                               String createdBy,
                                               LongList rowIdList) {
        checkNotNull(root, "messageColumnIO");
        checkNotNull(columns, "columns");
        checkNotNull(recordMaterializer, "recordMaterializer");

        List<PrimitiveColumnIO> leaves = root.getLeaves();

        if (leaves.isEmpty()) {
            return new EmptyRecordReader<>(recordMaterializer);
        }

        return new PositionableRecordReaderImpl<>(
                root,
                recordMaterializer,
                new ColumnReadStoreImpl(columns, recordMaterializer.getRootConverter(), root.getType(), createdBy),
                columns.getRowCount(),
                rowIdList);
    }
}
