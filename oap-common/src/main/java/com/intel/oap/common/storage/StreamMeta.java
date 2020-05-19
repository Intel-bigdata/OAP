
package com.intel.oap.common.storage;

public class StreamMeta {
    // logicalID of this Stream
    public byte[] logicalID;
    // total size of bytes in this stream
    public long totalSize;
    // number of chunks in this stream
    public int numOfChunks;
    // need compose or not
    boolean isComplete;
    StreamMeta(byte[] logicalID, long totalSize, int numOfChunks, boolean isComplete) {
        this.logicalID = logicalID;
        this.totalSize = totalSize;
        this.numOfChunks = numOfChunks;
        this.isComplete = isComplete;
    }
}
