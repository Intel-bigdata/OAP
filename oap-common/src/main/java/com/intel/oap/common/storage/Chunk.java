package com.intel.oap.common.storage;

public interface Chunk {

    // FileTrunk should also use write() method.
    // void writeDataToStore(Object baseObj, byte [] bytes, long baseAddress, long offset);

    //
    void read(byte [] bytes, int offset, int size);

    //
    void write(byte[] value, int offset);

    void free();
}
