package com.intel.oap.common.storage.memkind;

import com.intel.oap.common.storage.stream.*;
import com.intel.oap.common.unsafe.PersistentMemoryPlatform;
import com.intel.oap.common.util.NativeLibraryLoader;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
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
        PersistentMemoryPlatform.initialize("target/tmp/", 16 * 1024 *1024, 0);
        return libAvailable;
    }

    private Properties getProperties(String totalSize, String chunkSize) {
        Properties p = new Properties();
        p.setProperty("totalSize", totalSize);
        p.setProperty("chunkSize", chunkSize);
        p.setProperty("metaStore", "memkind");
        p.setProperty("dataStore", "memkind");
        p.setProperty("initialPath", "target/tmp/");
        p.setProperty("initialSize", "16777216");
        return  p;
    }

    private Boolean testChunkStream(String totalSize, String chunkSize,
                                    byte[] data, int expectedTotalChunk,
                                    boolean expectedHasDiskData) throws IOException {
        Properties p = getProperties(totalSize, chunkSize);
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager, p);
        String fileName = "/tmp/test_" + new Timestamp(System.currentTimeMillis()).toString();
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
        File file = new File(fileName);
        if (file != null && file.exists()) {
            assert(file.delete());
        }
        return  validation;
    }

    @Test
    public void testPMemStreamEmpty() throws IOException {
        Properties p = getProperties("1024", "10");
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager, p);
        String fileName = "/tmp/test_empty.file";
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

    @Test
    public void testPMemStreamIncrementalWriteAndTruncate() throws IOException {
        assumeTrue(loadPmemLib());
        Properties p = getProperties("100", "4");
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager, p);
        String fileName = "/tmp/test_truncate.file";
        byte[] data = new byte[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
        // write bytes incrementally to same chunkoutputstream
        ChunkOutputStream chunkoutputStream = ChunkOutputStream.getChunkOutputStreamInstance(fileName, dataStore);
        chunkoutputStream.write(data);
        assert(chunkoutputStream.position() == 10);
        chunkoutputStream.close();
        assert(chunkoutputStream.position() == 10);
        ChunkOutputStream cos = ChunkOutputStream.getChunkOutputStreamInstance(fileName, dataStore);
        cos.write(data);
        assert(cos.position() == 20);
        cos.close();
        assert(cos.position() == 20);
        // truncate the output stream to a small position
        ChunkOutputStream truncateCos = ChunkOutputStream.getChunkOutputStreamInstance(fileName, dataStore);
        truncateCos.truncate(5);
        assert(truncateCos.position() == 5);
        byte[] readData = new byte[5];
        ChunkInputStream chunkInputStream = new ChunkInputStream(fileName, dataStore);
        chunkInputStream.read(readData);
        chunkInputStream.close();
        MetaData metaData = pMemManager.getpMemMetaStore().getMetaFooter(fileName.getBytes());
        assert(metaData.getTotalChunk() == 2);
        assert(metaData.isHasDiskData() == false);
        assert(readData.length == 5);
        File file = new File(fileName);
        if (file != null && file.exists()) {
            assert(file.delete());
        }
    }

    @Test
    public void testPMemStreamTruncateWithFile() throws IOException {
        assumeTrue(loadPmemLib());
        Properties p = getProperties("8", "4");
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager, p);
        String fileName = "/tmp/test_truncate_file.file";
        byte[] data = new byte[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
        ChunkOutputStream chunkoutputStream = ChunkOutputStream.getChunkOutputStreamInstance(fileName, dataStore);
        chunkoutputStream.write(data);
        chunkoutputStream.write(data);
        assert(chunkoutputStream.position() == 20);
        chunkoutputStream.close();
        assert(chunkoutputStream.position() == 20);
        ChunkOutputStream truncateCos = ChunkOutputStream.getChunkOutputStreamInstance(fileName, dataStore);
        truncateCos.truncate(5);
        assert(truncateCos.position() == 5);
        byte[] readData = new byte[5];
        ChunkInputStream chunkInputStream = new ChunkInputStream(fileName, dataStore);
        chunkInputStream.read(readData);
        chunkInputStream.close();
        MetaData metaData = pMemManager.getpMemMetaStore().getMetaFooter(fileName.getBytes());
        assert(metaData.getTotalChunk() == 2);
        assert(metaData.isHasDiskData() == false);
        assert(readData.length == 5);
        File file = new File(fileName);
        if (file != null && file.exists()) {
            assert(file.delete());
        }
    }
    @Test
    public void testPMemStreamReadWithSkip() throws IOException {
        assumeTrue(loadPmemLib());
        Properties p = getProperties("8", "4");
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager, p);
        String fileName = "/tmp/test_skip.file";
        byte[] data = new byte[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
        ChunkOutputStream chunkoutputStream = ChunkOutputStream.getChunkOutputStreamInstance(fileName, dataStore);
        chunkoutputStream.write(data);
        chunkoutputStream.close();
        byte[] readData = new byte[2];
        ChunkInputStream chunkInputStream = new ChunkInputStream(fileName, dataStore);
        chunkInputStream.read(readData);
        assert(Arrays.equals(readData, new byte[]{'a', 'b'}));
        chunkInputStream.skip(2);
        chunkInputStream.read(readData);
        assert(Arrays.equals(readData, new byte[]{'e', 'f'}));
        chunkInputStream.skip(3);
        byte[] finalData = new byte[1];
        chunkInputStream.read(finalData);
        assert(Arrays.equals(finalData, new byte[]{'j'}));
        File file = new File(fileName);
        if (file != null && file.exists()) {
            assert(file.delete());
        }
    }
    @Test
    public void testPMemStreamFree() throws IOException {
        assumeTrue(loadPmemLib());
        Properties p = getProperties("8", "4");
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager, p);
        String fileName = "/tmp/test_skip_read.file";
        byte[] data = new byte[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
        ChunkOutputStream chunkoutputStream = ChunkOutputStream.getChunkOutputStreamInstance(fileName, dataStore);
        chunkoutputStream.write(data);
        chunkoutputStream.close();
        byte[] readData = new byte[10];
        ChunkInputStream chunkInputStream = ChunkInputStream.getChunkInputStreamInstance(fileName, dataStore);
        chunkInputStream.read(readData);
        ChunkInputStream cis = ChunkInputStream.getChunkInputStreamInstance(fileName, dataStore);
        cis.free();
        File file = new File(fileName);
        if (file != null && file.exists()) {
            assert(file.delete());
        }
    }
    @Test
    public void testPMemStreamBytesNotFullyWrite() throws IOException {
        assumeTrue(loadPmemLib());
        Properties p = getProperties("12", "6");
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager, p);
        String fileName = "/tmp/test_not_fully.file";
        byte[] data = new byte[]{'a', 'b', 'c', 'd', 'e', 'f'};
        ChunkOutputStream chunkoutputStream = ChunkOutputStream.getChunkOutputStreamInstance(fileName, dataStore);
        chunkoutputStream.write(data, 0, 5);
        chunkoutputStream.write(data, 0, 2);
        chunkoutputStream.write(data, 0, 4);
        chunkoutputStream.close();
        byte[] readData = new byte[11];
        ChunkInputStream chunkInputStream = ChunkInputStream.getChunkInputStreamInstance(fileName, dataStore);
        chunkInputStream.read(readData);
        ChunkInputStream cis = ChunkInputStream.getChunkInputStreamInstance(fileName, dataStore);
        cis.free();
        File file = new File(fileName);
        if (file != null && file.exists()) {
            assert(file.delete());
        }
        assert(Arrays.equals(readData, new byte[]{'a', 'b', 'c', 'd', 'e', 'a', 'b', 'a', 'b', 'c', 'd'}));
    }
}