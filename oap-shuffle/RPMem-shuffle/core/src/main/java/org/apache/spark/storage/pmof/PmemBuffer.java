package org.apache.spark.storage.pmof;

import org.apache.spark.jni.pmof.JniUtils;

import java.io.IOException;

public class PmemBuffer {
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
    private long len = 0;
    long pmBuffer;
    PmemBuffer() {
      pmBuffer = nativeNewPmemBuffer();
    }

    PmemBuffer(long len) throws IOException {
        JniUtils.getInstance("jnipmdk");
        this.len = len;
        NettyByteBufferPool.unpooledInc(len);
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
      NettyByteBufferPool.unpooledDec(len);
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
        clean();
        nativeDeletePmemBuffer(pmBuffer);
        closed = true;
      }
    }
}
