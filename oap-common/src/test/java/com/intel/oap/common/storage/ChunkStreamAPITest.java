package com.intel.oap.common.storage;

import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.storage.stream.ChunkInputStream;
import com.intel.oap.common.storage.stream.ChunkOutputStream;
import com.intel.oap.common.storage.stream.DataStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

public class ChunkStreamAPITest {

    DataStore dataStore;

    @Before
    public void prepare(){
        Properties p = new Properties();
        p.put("totalSize", "0");
        p.put("chunkSize", "1");
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager);
    }

    @Test
    public void testFileStreamReadWrite() throws IOException {
        String fileName = "target/test.file";
        byte[] data = new byte[]{'a', 'b', 'c'};
        ChunkOutputStream chunkoutputStream = new ChunkOutputStream(fileName, dataStore);
        chunkoutputStream.write(data);
        chunkoutputStream.close();

        ChunkInputStream chunkInputStream = new ChunkInputStream(fileName, dataStore);
        byte[] readData = new byte[3];
        chunkInputStream.read(readData);
        chunkInputStream.close();
        Assert.assertArrayEquals(data, readData);
    }
}