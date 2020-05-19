package com.intel.oap.common.storage;

public class PMemChunk implements Chunk {
    private long baseAddress;
    private long offset;

    @Override
    public void write(byte[] value, int offset) {
        MemoryCopyUtil.copyMemory(value, 0, null, baseAddress + offset, value.length);
    }

    @Override
    public void read(byte[] bytes, int offset, int size) {
    }

    @Override
    public void free() {

    }

    public PMemChunk(long baseAddress, long offset) {
        this.baseAddress = baseAddress;
        this.offset = offset;
    }

    public long getBaseAddress() {
        return this.baseAddress;
    }

    public long getOffset() {
        return this.offset;
    }
//
//    @Override
//    public void read(byte[] bytes, int offset, int size) {
//        throw new RuntimeException("Unsupported method");
//    }
//
//    @Override
//    public void release() {
//        throw new RuntimeException("Unsupported method");
//    }
}
