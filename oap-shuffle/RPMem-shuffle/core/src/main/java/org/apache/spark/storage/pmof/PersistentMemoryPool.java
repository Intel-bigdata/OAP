package org.apache.spark.storage.pmof;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.spark.jni.pmof.JniUtils;

public class PersistentMemoryPool {
    private static native long nativeOpenDevice(String path, long size);
    private static native void nativeSetBlock(long deviceHandler, String key, ByteBuffer byteBuffer, int size, boolean clean);
    private static native long[] nativeGetBlockIndex(long deviceHandler, String key);
    private static native long nativeGetBlockSize(long deviceHandler, String key);
    private static native void nativeDeleteBlock(long deviceHandler, String key);
    private static native long nativeGetRoot(long deviceHandler);
    private static native int nativeCloseDevice(long deviceHandler);
    private static native long nativeRemoveBlock(long deviceHandler, String key);
  
    private static final long DEFAULT_PMPOOL_SIZE = 0L;

    private String device;
    private long deviceHandler;

    PersistentMemoryPool(String path, long pool_size) throws IOException {
      JniUtils.getInstance("jnipmdk");
      this.device = path; 
      pool_size = pool_size == -1 ? DEFAULT_PMPOOL_SIZE : pool_size;
      this.deviceHandler = nativeOpenDevice(path, pool_size);
    }

    public void setPartition(String key, ByteBuffer byteBuffer, int size, boolean set_clean) {
      nativeSetBlock(this.deviceHandler, key, byteBuffer, size, set_clean);
    }

    public long[] getPartitionBlockInfo(String key) {
      return nativeGetBlockIndex(this.deviceHandler, key);
    }

    public long getPartitionSize(String key) {
      return nativeGetBlockSize(this.deviceHandler, key);
    }

    public void deletePartition(String key) {
      nativeDeleteBlock(this.deviceHandler, key);
    }

    public long removeBlock(String key) {
        return nativeRemoveBlock(this.deviceHandler, key);
    }

    public long getRootAddr() {
        return nativeGetRoot(this.deviceHandler);
    }

    public void close() {
      nativeCloseDevice(this.deviceHandler);
    }
}
