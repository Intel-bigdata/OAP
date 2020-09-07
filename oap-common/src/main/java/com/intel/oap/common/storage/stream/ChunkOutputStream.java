package com.intel.oap.common.storage.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;

public class ChunkOutputStream extends OutputStream implements WritableByteChannel {

    private boolean isOpen = true;
    private ChunkWriter chunkWriter;

    public ChunkOutputStream(byte[] name, DataStore dataStore) {
        super();
        this.chunkWriter = dataStore.getChunkWriter(name);
    }

    public void write(int b) throws IOException {
        chunkWriter.write(b);
    }

    public void write(byte b[]) throws IOException {
        chunkWriter.write(b);
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this file output stream.
     *
     * @param      b     the data.
     * @param      off   the start offset in the data.
     * @param      len   the number of bytes to write.
     * @exception  IOException  if an I/O error occurs.
     */
    public void write(byte b[], int off, int len) throws IOException {
        chunkWriter.write(b, off, len);
    }


    @Override
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Closes this file output stream and releases any system resources
     * associated with this stream. This file output stream may no longer
     * be used for writing bytes.
     *
     * <p> If this stream has an associated channel then the channel is closed
     * as well.
     *
     * @exception  IOException  if an I/O error occurs.
     *
     * @revised 1.4
     * @spec JSR-51
     */
    public void close() throws IOException {
        chunkWriter.close();
        isOpen = false;
        super.close();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        byte[] bytes = new byte[remaining];
        src.get(bytes);
        write(bytes);
        return remaining;
    }
}