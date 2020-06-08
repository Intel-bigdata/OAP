package com.intel.oap.common.storage;

import com.intel.oap.common.storage.stream.*;
import com.intel.oap.common.unsafe.PersistentMemoryPlatform;
import com.intel.oap.common.util.NativeLibraryLoader;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assume.assumeTrue;

public class ChunkStreamAPITest {
    private static final Logger logger = LoggerFactory.getLogger(ChunkStreamAPITest.class);

    DataStore dataStore;
    boolean libAvailable = true;

    @Before
    public void prepare() {
        File fd = new File("target/tmp/");
        if (!fd.exists()) {
            fd.mkdirs();
        }
    }

    private boolean loadPmemLib() {
        String LIBNAME = "pmplatform";
        try {
            NativeLibraryLoader.load(LIBNAME);
        } catch (UnsatisfiedLinkError | RuntimeException e) {
            logger.warn("pmplatform lib failed to load, PMem Stream Test will be skipped.");
            libAvailable = false;
        }
        return libAvailable;
    }

    private Boolean testChunkStream(String totalSize, String chunkSize,
                                    byte[] data, int expectedTotalChunk,
                                    boolean expectedHasDiskData) throws IOException {
        Properties p = new Properties();
        p.setProperty("totalSize", totalSize);
        p.setProperty("chunkSize", chunkSize);
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager);
        String fileName = "target/test.file";
        PersistentMemoryPlatform.initialize("target/tmp/", 16 * 1024 *1024, 0);
        byte[] readData = new byte[data.length];
        ChunkOutputStream chunkoutputStream = new ChunkOutputStream(fileName, dataStore);
        chunkoutputStream.write(data);
        chunkoutputStream.close();
        ChunkInputStream chunkInputStream = new ChunkInputStream(fileName, dataStore);
        chunkInputStream.read(readData);
        chunkInputStream.close();
        MetaData metaData = pMemManager.getpMemMetaStore().getMetaFooter(fileName.getBytes());
        boolean validation = Arrays.equals(data, readData) &&
                    metaData.getTotalChunk() == expectedTotalChunk &&
                    metaData.isHasDiskData() == expectedHasDiskData;
        chunkInputStream.free();

        return  validation;
    }

    @Test
    public void testPMemStreamEmpty() throws IOException {
        Properties p = new Properties();
        p.put("totalSize", "1024");
        p.put("chunkSize", "10");
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager);
        String fileName = "target/test.file";
        ChunkOutputStream chunkOutputStream = new ChunkOutputStream(fileName, dataStore);
        chunkOutputStream.close();
        MetaData metaData = pMemManager.getpMemMetaStore().getMetaFooter(fileName.getBytes());
        assert(metaData.getTotalChunk() == 0);
        assert(metaData.isHasDiskData() == false);
        // delete generated pmem.file
        File file = new File(fileName);
        if (file != null && file.exists()) {
            assert(file.delete());
        }
    }

    @Test
    public void testFileStreamReadWrite() throws IOException {
        assumeTrue(loadPmemLib());
        byte[] data = new byte[]{'a', 'b', 'c'};
        assert(testChunkStream("0", "1024", data, 0, true));
    }

    @Test
    public void testPMemStreamSingleChunk() throws IOException {
        assumeTrue(loadPmemLib());
        byte[] data = new byte[]{'a', 'b', 'c'};
        assert(testChunkStream("1024", "6", data, 1, false));
    }

    @Test
    public void testPMemStreamSingleChunkNearBoundary() throws IOException {
        assumeTrue(loadPmemLib());
        byte[] data = new byte[]{'a', 'b', 'c', 'd', 'e', 'f'};
        assert(testChunkStream("1024", "6", data, 1, false));
    }

    @Test
    public void testPMemStreamMultiChunks() throws IOException {
        assumeTrue(loadPmemLib());
        byte[] data = new byte[]{'a', 'b', 'c', 'd', 'e', 'f', 'g'};
        assert(testChunkStream("1024", "6", data, 2, false));
    }

    @Test
    public void testPMemStreamMultiChunksNearBoundary() throws IOException {
        assumeTrue(loadPmemLib());
        byte[] data = new byte[]{'a', 'b', 'c', 'd', 'e', 'f'};
        assert(testChunkStream("1024", "3", data, 2, false));
    }

    @Test
    public void testPMemStreamSingleChunkWithFileTriggered() throws IOException {
        assumeTrue(loadPmemLib());
        byte[] data = new byte[]{'a', 'b', 'c', 'd', 'e', 'f'};
        assert(testChunkStream("4", "3", data, 1, true));
    }

    @Test
    public void testPMemStreamMultiChunksWithFileTriggered() throws IOException {
        assumeTrue(loadPmemLib());
        byte[] data = new byte[]{'a', 'b', 'c', 'd', 'e', 'f'};
        assert(testChunkStream("4", "2", data, 2, true));
    }
}