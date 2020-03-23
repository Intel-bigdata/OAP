package org.apache.spark.storage.pmof;

public class PmemBuffer {
    static {
        System.load("/usr/local/lib/libjnipmdk.so");
    }
    private native long nativeNewPmemBuffer();
    private native long nativeNewPmemBufferBySize(long len);
    private native int nativeLoadPmemBuffer(long pmBuffer, long addr, int len);
    private native int nativeReadBytesFromPmemBuffer(long pmBuffer, byte[] bytes, int off, int len);
    private native int nativeWriteBytesToPmemBuffer(long pmBuffer, byte[] bytes, int off, int len);
    private native long nativeCleanPmemBuffer(long pmBuffer);
    private native int nativeGetPmemBufferRemaining(long pmBuffer);
    private native long nativeGetPmemBufferDataAddr(long pmBuffer);
    private native long nativeDeletePmemBuffer(long pmBuffer);
		
    private boolean closed = false;
    long pmBuffer;
    PmemBuffer() {
      pmBuffer = nativeNewPmemBuffer();
    }

    PmemBuffer(long len) {
      pmBuffer = nativeNewPmemBufferBySize(len);
    }

    void load(long addr, int len) {
      nativeLoadPmemBuffer(pmBuffer, addr, len);
    }

    long getNativeObject() {
      return pmBuffer;
    }

    int get(byte[] bytes, int off, int len) {
      int read_len = nativeReadBytesFromPmemBuffer(pmBuffer, bytes, off, len);
      return read_len;
    }

    int get() {
      byte[] bytes = new byte[1];
      nativeReadBytesFromPmemBuffer(pmBuffer, bytes, 0, 1);
      return (bytes[0] & 0xFF);
    }

    void put(byte[] bytes, int off, int len) {
      nativeWriteBytesToPmemBuffer(pmBuffer, bytes, off, len);
    }

    void clean() {
      nativeCleanPmemBuffer(pmBuffer);
    }

    int size() {
      return nativeGetPmemBufferRemaining(pmBuffer);
    }

    long getDirectAddr() {
      return nativeGetPmemBufferDataAddr(pmBuffer);
    }

    synchronized void close() {
        if (!closed) {
            nativeDeletePmemBuffer(pmBuffer);
            closed = true;
        }
    }
}
