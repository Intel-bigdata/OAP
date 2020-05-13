package com.intel.oap.common.storage;

public class FileChunk implements Chunk {
    @Override
    public void writeDataToStore(Object baseObj, long baseAddress, long offset) {
        // TODO fileStream write to data
    }

    @Override
    public void read(int offset, int size) {

    }
}
