package com.intel.oap.common.storage.pmemblk;

import io.pmem.pmemkv.Converter;
import io.pmem.pmemkv.Database;

import java.io.File;
import java.nio.ByteBuffer;

public class PMemKVDatabase {

    private static Database<String, String> db;

    public static Database open(String engine, String path, long size) {
        boolean forceCreate = !isCreated(path);
        db = new Database.Builder<String, String>(engine)
                .setSize(size)
                .setPath(path)
                .setForceCreate(forceCreate)
                .setKeyConverter(new StringConverter())
                .setValueConverter(new StringConverter())
                .build();
        return db;
    }

    public static void close() {
        db.stop();
    }

    private static boolean isCreated(String path) {
        File pmemkvDB = new File(path);
        return pmemkvDB.exists();
    }

}

class StringConverter implements Converter<String> {
    public ByteBuffer toByteBuffer(String entry) {
        return ByteBuffer.wrap(entry.getBytes());
    }

    public String fromByteBuffer(ByteBuffer entry) {
        if (entry.hasArray()) {
            return new String(entry.array());
        } else {
            byte[] bytes;
            bytes = new byte[entry.capacity()];
            entry.get(bytes);
            return new String(bytes);
        }
    }
}
