package org.apache.parquet.column.impl;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;

public class SpinachColumnReadStoreImpl extends ColumnReadStoreImpl {

    public SpinachColumnReadStoreImpl(PageReadStore pageReadStore, GroupConverter recordConverter,
            MessageType schema) {
        super(pageReadStore, recordConverter, schema);
    }

    @Override
    public ColumnReader getColumnReader(ColumnDescriptor path) {
        try {
            return super.getColumnReader(path);
        } catch (IllegalArgumentException e) {
            return new ColumnReaderStub();
        }
    }

}
