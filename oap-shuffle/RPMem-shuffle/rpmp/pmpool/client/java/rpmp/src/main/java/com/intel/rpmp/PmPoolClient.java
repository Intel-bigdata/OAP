package com.intel.rpmp;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

/**
 * PmPoolClient
 *
 */
public class PmPoolClient {
    static {
        System.loadLibrary("pmpool");
    }

    public PmPoolClient(String remote_address, String remote_port) {
        objectId = newPmPoolClient_(remote_address, remote_port);
    }

    public long alloc(long size) {
        return alloc_(size, objectId);
    }

    public int free(long address) {
        return free_(address, objectId);
    }

    public int write(long address, String data, long size) {
        return write_(address, data, size, objectId);
    }

    public long write(String data, long size) {
        return alloc_and_write_(data, size, objectId);
    }

    public long write(ByteBuffer data, long size) {
        return alloc_and_write_(data, size, objectId);
    }

    public int read(long address, long size, ByteBuffer byteBuffer) {
        return read_(address, size, byteBuffer, objectId);
    }

    public long put(String key, ByteBuffer data, long size) {
        return put(key, data, size, objectId);
    }

    public long[] getMeta(String key) {
        return getMeta(key, objectId);
    }

    public int del(String key) {
        return del(key, objectId);
    }

    public void shutdown() {
        shutdown_(objectId);
    }

    public void waitToStop() {
        waitToStop_(objectId);
    }

    public void dispose() {
        dispose_(objectId);
    }

    private ByteBuffer convertToByteBuffer(long address, int length) throws IOException {
        Class<?> classDirectByteBuffer;
        try {
            classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer");
        } catch (ClassNotFoundException e) {
            throw new IOException("java.nio.DirectByteBuffer class not found");
        }
        Constructor<?> constructor;
        try {
            constructor = classDirectByteBuffer.getDeclaredConstructor(long.class, int.class);
        } catch (NoSuchMethodException e) {
            throw new IOException("java.nio.DirectByteBuffer constructor not found");
        }
        constructor.setAccessible(true);
        ByteBuffer byteBuffer;
        try {
            byteBuffer = (ByteBuffer) constructor.newInstance(address, length);
        } catch (Exception e) {
            throw new IOException("java.nio.DirectByteBuffer exception: " + e.toString());
        }

        return byteBuffer;
    }

    private native long newPmPoolClient_(String remote_address, String remote_port);

    private native long alloc_(long size, long objectId);

    private native int free_(long address, long objectId);

    private native int write_(long address, String data, long size, long objectId);

    private native long alloc_and_write_(String data, long size, long objectId);

    private native long alloc_and_write_(ByteBuffer data, long size, long objectId);

    private native long put(String key, ByteBuffer data, long size, long objectId);

    private native long[] getMeta(String key, long objectId);

    private native int del(String key, long objectId);

    private native int read_(long address, long size, ByteBuffer byteBuffer, long objectId);

    private native void shutdown_(long objectId);

    private native void waitToStop_(long objectId);

    private native void dispose_(long objectId);

    private long objectId;
}
