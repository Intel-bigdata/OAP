package com.intel.oap.common.storage.stream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

public class ChunkInputStream extends FileInputStream {
   protected ChunkReader chunkReader;

    public ChunkInputStream(String name, DataStore dataStore) throws FileNotFoundException {
        super(name);
        this.chunkReader = dataStore.getChunkReader(name.getBytes());
    }

    public int read() throws IOException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public int read(byte b[]) throws IOException {
        assert(b.length > 0);
        return chunkReader.read(b);
    }

    public int read(byte b[], int off, int len) throws IOException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public long skip(long n) throws IOException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public int available() throws IOException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void free() throws IOException {
        chunkReader.freeFromPMem();
    }
}
