package com.intel.oap.common.storage;

public interface Chunk {
    void writeDataToStore(Object baseObj, byte [] bytes, long baseAddress, long offset);

    void read(byte [] bytes, int offset, int size);

    void release();
}
