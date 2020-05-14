package com.intel.oap.common.storage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class FileChunkTest {
    String fileName = "target/test.file";
    byte[] data = new byte[]{'a', 'b', 'c'};

    @Before
    public void before(){
        File testFile = new File(fileName);
        if (testFile.exists()) {
            testFile.delete();
        }
    }

    @After
    public void after(){
        File testFile = new File(fileName);
        if (testFile.exists()) {
            testFile.delete();
        }
    }

    @Test
    public void testFileChunk() {
        FileChunk outputChunk = new FileChunk(fileName.getBytes());
        outputChunk.writeDataToStore(null, data, -1, -1);
        outputChunk.release();

        FileChunk inputChunk = new FileChunk(fileName.getBytes());
        byte[] output = new byte[3];
        inputChunk.read(output, 0, 3);
        inputChunk.release();
        Assert.assertArrayEquals(output, data);
    }
}
