package com.intel.oap.common.storage.backup;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileChunk implements Chunk {
    private FileInputStream fileInputStream;
    private FileOutputStream fileOutputStream;
    private byte [] id;

    public FileChunk(byte [] id){
        this.id = id;
    }

    @Override
    public void write(byte[] value, int offset){
        // TODO fileStream write to data
        try {
            if(fileOutputStream == null){
                fileOutputStream = new FileOutputStream(new String(id));
            }
            fileOutputStream.write(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void free(){
        // Do nothing since it's file stream
    }

    @Override
    public void read(byte[] bytes, int offset, int size) {
        try {
            if(fileInputStream == null){
                fileInputStream = new FileInputStream(new String(id));
            }
            fileInputStream.read(bytes, offset, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void release() {
        if(fileInputStream != null){
            try {
                fileInputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if(fileOutputStream != null){
            try {
                fileOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
