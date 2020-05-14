package com.intel.oap.common.storage;

public class PMemChunk implements Chunk {
    private long baseAddress;
    private long offset;

    public void writeDataToStore(Object baseObj, long baseAddress, long offset){
        // TODO Platform copy to the address
        throw new RuntimeException("Unsupported method");
    }

    @Override
    public void writeDataToStore(Object baseObj, byte[] bytes, long baseAddress, long offset) {
        throw new RuntimeException("Unsupported method");
    }

    @Override
    public void read(byte[] bytes, int offset, int size) {
        throw new RuntimeException("Unsupported method");
    }

    @Override
    public void release() {
        throw new RuntimeException("Unsupported method");
    }
}
