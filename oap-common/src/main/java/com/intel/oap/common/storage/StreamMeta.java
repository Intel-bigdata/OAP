
package com.intel.oap.common.storage;

public class StreamMeta {
    // total size of bytes in this stream
    public long totalSize;
    // number of chunks in this stream
    public int numOfChunks;
    // need compose or not
    boolean isComplete;
}
