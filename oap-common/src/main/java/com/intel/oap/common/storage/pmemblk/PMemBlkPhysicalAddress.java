package com.intel.oap.common.storage.pmemblk;

import com.intel.oap.common.storage.stream.PMemPhysicalAddress;

public class PMemBlkPhysicalAddress implements PMemPhysicalAddress {

    private int index;
    private int length;

    public PMemBlkPhysicalAddress(int index, int length) {
        this.index = index;
        this.length = length;
    }

    public int getIndex() {
        return index;
    }

    public int getLength() {
        return length;
    }
}
