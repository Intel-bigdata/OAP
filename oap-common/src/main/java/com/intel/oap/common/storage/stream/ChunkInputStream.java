package com.intel.oap.common.storage.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ChunkInputStream extends InputStream implements ReadableByteChannel {

   private boolean isOpen = true;
   protected ChunkReader chunkReader;

    public ChunkInputStream(byte[] name, DataStore dataStore) {
        super();
        this.chunkReader = dataStore.getChunkReader(name);
    }

    public int read() throws IOException {
        return chunkReader.read();
    }

    public int read(byte b[]) throws IOException {
        assert(b.length > 0);
        return chunkReader.read(b, 0, b.length);
    }

    public int read(byte b[], int off, int len) throws IOException {
        return chunkReader.read(b, off, len);
    }

    public void free() throws IOException {
        chunkReader.freeFromPMem();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int remaining = dst.remaining();
        byte[] bytes = new byte[remaining];
        read(bytes);
        dst.put(bytes);
        return remaining;
    }

    @Override
    public void close() {
        isOpen = false;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }
}
