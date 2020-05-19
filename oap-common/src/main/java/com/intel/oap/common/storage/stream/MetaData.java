package com.intel.oap.common.storage.stream;

public class MetaData {
    private boolean hasDiskData = false;
    private int totalChunk = 0;

    public MetaData(boolean hasDiskData, int totalChunk){
        this.setHasDiskData(hasDiskData);
        this.setTotalChunk(totalChunk);
    }

    public boolean isHasDiskData() {
        return hasDiskData;
    }

    public void setHasDiskData(boolean hasDiskData) {
        this.hasDiskData = hasDiskData;
    }

    public int getTotalChunk() {
        return totalChunk;
    }

    public void setTotalChunk(int totalChunk) {
        this.totalChunk = totalChunk;
    }
}
