package com.intel.oap.common.storage.pmemblk;

import io.pmem.pmemkv.Database;

import java.io.File;

public class PMemKVDatabase {

    private static Database db;

    public static Database open(String engine, String path, long size) {
        StringBuffer dbConfig = new StringBuffer();
        dbConfig.append("{\"path\":").append("\"" + path + "\", ");
        if (isCreated(path))
            dbConfig.append("\"force_create\":0}");
        else
            dbConfig.append("\"size\":" + size + ", \"force_create\":1}");
        db = new Database(engine, dbConfig.toString());
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
