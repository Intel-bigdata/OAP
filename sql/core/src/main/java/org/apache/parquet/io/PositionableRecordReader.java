package org.apache.parquet.io;

public interface PositionableRecordReader<T> {

    T read();

    Long getCurrentRowId();

    /**
     * Returns whether the current record should be skipped (dropped) Will be called *after* read()
     */
    boolean shouldSkipCurrentRecord();

    long getRecordCount();

}
