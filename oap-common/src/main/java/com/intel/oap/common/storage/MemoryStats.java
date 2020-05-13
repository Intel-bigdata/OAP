package com.intel.oap.common.storage;

public class MemoryStats {
    private volatile long usedSize = 0;
    private long totalSize;

    public MemoryStats(long totalSize) {
        this.totalSize = totalSize;
    }

    public synchronized void increaseSize(long size) {
        usedSize += size;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public synchronized long getUsedSize() {
        return usedSize;
    }

    public synchronized void decreaseSize(long size) {
        totalSize -= size;
    }

}
