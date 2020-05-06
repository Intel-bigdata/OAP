package com.intel.oap.common.unsafe;

import com.intel.oap.common.util.NativeLibraryLoader;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JNI wrapper for the vmemcache library.
 */
public class VMEMCacheJNI {

    private static final Logger LOG = LoggerFactory.getLogger(VMEMCacheJNI.class);
    private static boolean initialized = false;
    public static final String LIBRARY_NAME = "vmemcachejni";

    static {
        LOG.info("Trying to load the native library from jni...");
        NativeLibraryLoader.load(LIBRARY_NAME);
    }

    public static synchronized int initialize(String path, long maxSize) {
        if (!initialized) {
            int success = init(path, maxSize);
            if (success == 0) {
                initialized = true;
            }
            return success;
        }
        return 0;
    }

    static native int init(String path, long maxSize);

    /* returns the number of bytes put */
    // this interface is used to put on-heap data to pm
    public static native int put(byte[] keyArray, ByteBuffer keyBuffer, int keyOff, int keyLen,
                                 byte[] valueArray, ByteBuffer valueBuffer,
                                 int valueOff, int valueLen);

    // this interface is used to put off-heap data to pm
    public static native int putNative(byte[] keyArray, ByteBuffer keyBuffer,
                                       int keyOff, int keyLen,
                                       long valueBaseAdj, int valueOff, int valueLen);

    /* returns the number of bytes get */
    // this interface is used to get pm data to on-heap
    public static native int get(byte[] keyArray, ByteBuffer keyBuffer, int keyOff, int keyLen,
                                 byte[] valueArray, ByteBuffer valueBuffer,
                                 int valueOff, int maxValueLen);

    // this interface is used to get pm data to off-heap
    public static native int getNative(byte[] keyArray, ByteBuffer keyBuffer,
                                       int keyOff, int keyLen,
                                       long valueBaseAdj, int valueOff, int maxValueLen);

    public static native int evict(byte[] keyArray, ByteBuffer keyBuffer, int keyOff, int keyLen);

    public static native int exist(byte[] keyArray, ByteBuffer keyBuffer, int keyOff, int keyLen);

    public static native int status(long[] statusArray);
}
