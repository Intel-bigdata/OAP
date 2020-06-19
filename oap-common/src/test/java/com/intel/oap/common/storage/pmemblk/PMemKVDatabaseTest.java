package com.intel.oap.common.storage.pmemblk;

import com.intel.oap.common.util.NativeLibraryLoader;
import io.pmem.pmemkv.Database;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assume.*;
import static org.junit.Assert.*;

public class PMemKVDatabaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(PMemKVDatabaseTest.class);

    private static long PMEMKV_SIZE = 32 * 1024 * 1024;
    private static String PATH = "/dev/shm/pmemkv_test_db";
    private static final String LIBNAME = "pmemkv-jni";
    private static final String ENGINE = "cmap";

    @Before
    public void checkIfLibPMemKVExisted() {
        Exception exception = null;
        try {
            NativeLibraryLoader.load(LIBNAME);
        } catch (Exception e) {
            LOG.warn("Fail to load " + LIBNAME);
            exception = e;
        }
        assumeTrue(exception == null);
    }

    @After
    public void tearDown() {
        File file = new File(PATH);
        if (file != null && file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testWriteKV() {
        Database db = PMemKVDatabase.open(ENGINE, PATH, PMEMKV_SIZE);
        db.put("key1", "value1");
        db.put("key2", "value2");
        db.put("key3", "value3");
        assertEquals(3, db.countAll());
        assertEquals("value1", db.getCopy("key1"));
        assertEquals("value2", db.getCopy("key2"));
        assertEquals("value3", db.getCopy("key3"));
        db.stop();

        Database dbReopened = PMemKVDatabase.open(ENGINE, PATH, PMEMKV_SIZE);
        assertEquals(3, dbReopened.countAll());
        assertEquals("value1", dbReopened.getCopy("key1"));
        assertEquals("value2", dbReopened.getCopy("key2"));
        assertEquals("value3", dbReopened.getCopy("key3"));
        dbReopened.stop();
    }
}
