package com.intel.oap.common.storage.memkind;

import com.intel.oap.common.storage.stream.PMemPhysicalAddress;

public class MemkindPMemPhysicalAddress implements PMemPhysicalAddress {
    private long baseAddress;
    private int offset;

    public MemkindPMemPhysicalAddress(long baseAddress, int offset) {
        this.baseAddress = baseAddress;
        this.offset = offset;
    }

    public long getBaseAddress() {
        return this.baseAddress;
    }

    public int getOffset() {
        return this.offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
