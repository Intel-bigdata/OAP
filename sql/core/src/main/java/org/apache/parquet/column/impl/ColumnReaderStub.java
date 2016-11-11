package org.apache.parquet.column.impl;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Binary;

public class ColumnReaderStub implements ColumnReader {

    @Override
    public long getTotalValueCount() {
        return 0;
    }

    @Override
    public void consume() {

    }

    @Override
    public int getCurrentRepetitionLevel() {
        return 0;
    }

    @Override
    public int getCurrentDefinitionLevel() {
        return 0;
    }

    @Override
    public void writeCurrentValueToConverter() {
    }

    @Override
    public void skip() {
    }

    @Override
    public int getCurrentValueDictionaryID() {
        return 0;
    }

    @Override
    public int getInteger() {
        return 0;
    }

    @Override
    public boolean getBoolean() {
        return false;
    }

    @Override
    public long getLong() {
        return 0;
    }

    @Override
    public Binary getBinary() {
        return null;
    }

    @Override
    public float getFloat() {
        return 0;
    }

    @Override
    public double getDouble() {
        return 0;
    }

    @Override
    public ColumnDescriptor getDescriptor() {
        return null;
    }

}
