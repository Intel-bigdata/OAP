package com.intel.oap.common.storage;

public class PMemChunk implements Chunk {
    private long baseAddress;
    private long offset;

    public void writeDataToStore(Object baseObj, long baseAddress, long offset){
        // TODO Platform copy to the address
    }

    @Override
    public void read(int offset, int size) {
        throw new RuntimeException("Unsupported method");
    }

}
