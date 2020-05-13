package com.intel.oap.common.storage;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ChunkOutputStream extends FileOutputStream {

    private PMemManager pMemManager;
    private ByteBuffer byteBuffer;
    private PMemChunk currentBlock;

    public ChunkOutputStream(String name, PMemManager pMemManager) throws FileNotFoundException {
        super(name);
        this.pMemManager = pMemManager;
        this.byteBuffer = ByteBuffer.allocateDirect(100); // TODO
    }

    public void write(int b) throws IOException {
        throw new RuntimeException("Unsupported Operation");
    }

    public void write(byte b[]) throws IOException {
        byteBuffer.put(b); // TODO overflow
        if(byteBuffer.remaining() == 0){
            // FIXME prototype code
//            currentBlock = pMemManager.pMemDataStore.getOutputChunkIterator().next();
            currentBlock.writeDataToStore(b, -1, b.length);
        }
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
        throw new RuntimeException("Unsupported Operation");
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
        throw new RuntimeException("Unsupported Operation");
    }

    /**
     * Returns the unique {@link java.nio.channels.FileChannel FileChannel}
     * object associated with this file output stream.
     *
     * <p> The initial {@link java.nio.channels.FileChannel#position()
     * position} of the returned channel will be equal to the
     * number of bytes written to the file so far unless this stream is in
     * append mode, in which case it will be equal to the size of the file.
     * Writing bytes to this stream will increment the channel's position
     * accordingly.  Changing the channel's position, either explicitly or by
     * writing, will change this stream's file position.
     *
     * @return  the file channel associated with this file output stream
     *
     * @since 1.4
     * @spec JSR-51
     */
    public FileChannel getChannel() {
        throw new RuntimeException("Unsupported Operation");
    }
}