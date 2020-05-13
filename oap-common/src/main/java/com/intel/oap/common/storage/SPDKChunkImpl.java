package com.intel.oap.common.storage;

import java.io.*;
import java.util.Properties;

public class SPDKChunkImpl implements ChunkAPI {

    private String fileDir;
    private File file;
    private FileInputStream inputStream;
    private FileOutputStream outputStream;

    public SPDKChunkImpl(Properties properties){
        fileDir = properties.getProperty("oap.path");
    }

    public void write(byte[] id, byte[] value) {
        // TODO move initialization logic out
        if (file == null) {
            file = new File(fileDir + File.separator + new String(id));
        }
        if (outputStream == null) {
            try {
                outputStream = new FileOutputStream(file);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        try {
            outputStream.write(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean contains(byte[] id) {
        return false;
    }

    @Override
    public Chunk getChunk(byte[] id) {
        return null;
    }

    @Override
    public void putChunk(byte[] id, Chunk chunk) {

    }

    @Override
    public void release() {

    }

    @Override
    public void free(Chunk chunk) {

    }
}
