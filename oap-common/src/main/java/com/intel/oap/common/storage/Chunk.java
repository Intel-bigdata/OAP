package com.intel.oap.common.storage;

public interface Chunk {
    public void writeDataToStore(Object baseObj, long baseAddress, long offset);

    void read(int offset, int size);
}
