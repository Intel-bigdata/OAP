package com.intel.oap.common.storage.stream;

public class MemoryStats {
    private volatile long usedSize = 0;
    private long totalSize;

    public MemoryStats(long totalSize) {
        this.totalSize = totalSize;
    }

    public synchronized void increaseSize(long size) {
        usedSize += size;
    }

    public synchronized long getRemainingSize() {
        return totalSize - usedSize;
    }

    public synchronized long getUsedSize() {
        return usedSize;
    }

    public synchronized void decreaseSize(long size) {
        totalSize -= size;
    }

}
