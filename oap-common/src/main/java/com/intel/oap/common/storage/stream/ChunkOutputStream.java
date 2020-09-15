package com.intel.oap.common.storage.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;

public class ChunkOutputStream extends FileOutputStream {

    private static final Logger logger = LoggerFactory.getLogger(ChunkOutputStream.class);

    private ChunkWriter chunkWriter;

    public String fileName;

    public ChunkOutputStream(String name, DataStore dataStore) throws FileNotFoundException {
        super(name);
        this.fileName = name;
        this.chunkWriter = dataStore.getChunkWriter(name.getBytes());
    }

    private static HashMap<String, ChunkOutputStream> chunkOutputStreamMap = new HashMap<>();
    public static ChunkOutputStream getChunkOutputStreamInstance(String name, DataStore dataStore) {

        if (chunkOutputStreamMap == null || !chunkOutputStreamMap.containsKey(name)) {
            synchronized (ChunkOutputStream.class) {
                if (chunkOutputStreamMap == null || !chunkOutputStreamMap.containsKey(name)) {
                    try {
                        chunkOutputStreamMap.put(name, new ChunkOutputStream(name, dataStore));
                    } catch (FileNotFoundException e) {
                        logger.warn(e.toString());
                    }
                }
            }
        }
        return chunkOutputStreamMap.get(name);
    }

    public void write(byte b) throws IOException {
       chunkWriter.write(b);
    }

    public void write(byte b[]) throws IOException {
        chunkWriter.write(b, b.length);
    }

    /**
     *
     * @param b bytes will be wrote to stream
     * @param len  data length that will be wrote, from begining
     * @throws IOException
     */
    public void write(byte b[], int len) throws IOException {
        chunkWriter.write(b, len);
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this output stream.
     *
     * @param      b     the data.
     * @param      off   the start offset in the data.
     * @param      len   the number of bytes to write.
     * @exception  IOException  if an I/O error occurs.
     */
    public void write(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        write(b, len);
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
        super.close();
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

    public long position() {
        return chunkWriter.position();
    }

    public void truncate(long position) throws IOException {
        chunkWriter.truncate(position);
    }
}