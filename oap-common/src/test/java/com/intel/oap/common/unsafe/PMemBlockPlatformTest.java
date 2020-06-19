package com.intel.oap.common.unsafe;

import org.junit.*;

import java.io.File;
import java.util.Random;

import static org.junit.Assume.*;
import static org.junit.Assert.*;

public class PMemBlockPlatformTest {

    private static int ELEMENT_SIZE = 1024;
    private static long POOL_SIZE = 32 * 1024 * 1024;
    private static String PATH = "/dev/shm/PMemBlockPlatformTest_blk_file";

    private final Random random = new Random();

    @Before
    public void checkIfLibPMemExisted() {
        assumeTrue(PMemBlockPlatform.isPMemBlkAvailable());
    }

    @BeforeClass
    public static void setUp() {
        PMemBlockPlatform.create(PATH, ELEMENT_SIZE, POOL_SIZE);
    }

    @After
    public void tearDown() {
        File file = new File(PATH);
        if (file != null && file.exists()) {
            file.delete();
        }
    }

    @AfterClass
    public static void close() {
        PMemBlockPlatform.close();
    }

    private byte[] writeBlock(int index) {
        byte[] bytesToWrite = new byte[ELEMENT_SIZE];
        random.nextBytes(bytesToWrite);
        PMemBlockPlatform.write(bytesToWrite, index);
        return bytesToWrite;
    }

    private byte[] readBlock(int index) {
        byte[] bytesFromRead = new byte[ELEMENT_SIZE];
        PMemBlockPlatform.read(bytesFromRead, index);
        return bytesFromRead;
    }

    @Test
    public void testWriteSingleBlock() {
        byte[] writtenBlock = writeBlock(0);
        byte[] readBlock = readBlock(0);
        assertArrayEquals(writtenBlock, readBlock);
    }

    @Test
    public void testWriteMultipleBlock() {
        for (int i = 0; i < 32; i++) {
            byte[] writtenBlock = writeBlock(i);
            byte[] readBlock = readBlock(i);
            assertArrayEquals(writtenBlock, readBlock);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testWriteBlockExceedMaximum() {
        int maxNum = PMemBlockPlatform.getBlockNum();
        writeBlock(maxNum);
    }

}
