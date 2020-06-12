package com.intel.rpmp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * You need to start rpmp service before running the following tests.
 */
public class PmPoolClientTest
{
    @Before
    public void setup() {
        pmPoolClient = new PmPoolClient("172.168.0.40", "12346");
    }

    @After
    public void tear() {
        pmPoolClient.shutdown();
        pmPoolClient.waitToStop();
        pmPoolClient.dispose();
    }

    @Test
    public void remoteAlloc() {
        for (int i = 0; i < 100; i++) {
            long address = pmPoolClient.alloc(4096);
            assertTrue(address > 0);
        }
    }

    @Test
    public void remoteWrite() {
        Random rand = new Random();
        for (int i = 0; i < 100; i++) {
            long address = pmPoolClient.alloc(rand.nextInt((1024*1024*8)));
            assertTrue(address > 0);
            String data = "hello";
            assertEquals(0, pmPoolClient.write(address, data, data.length()));
        }
    }


    @Test
    public void remoteRead()
    {
        long address = pmPoolClient.alloc(4096);
        assertTrue(address > 0);
        String data = "hello";
        assertEquals(0, pmPoolClient.write(address, data, data.length()));
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
        ByteBuffer testBuffer = ByteBuffer.allocateDirect(4096);
        for (int i = 0; i < 5; i++) {
            testBuffer.put(data.getBytes()[i]);
        }
        testBuffer.flip();
        assertEquals(0, pmPoolClient.read(address, 5, byteBuffer));
        for (int i = 0; i < 5; i++) {
            assertEquals(true, (char)byteBuffer.get() == (char)testBuffer.get());
        }
    }

    @Test
    public void remoteAllocAndWrite() {
        for (int i = 0; i < 100; i++) {
            String data = "hello";
            assertTrue(pmPoolClient.write(data, data.length()) > 0);
        }
    }

    public void remoteAllocAndWriteThenRead()
    {
        String data = "hello";
        long address = pmPoolClient.write(data, data.length());
        assertTrue(address > 0);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
        ByteBuffer testBuffer = ByteBuffer.allocateDirect(4096);
        for (int i = 0; i < 5; i++) {
            testBuffer.put(data.getBytes()[i]);
        }
        testBuffer.flip();
        assertEquals(0, pmPoolClient.read(address, 5, byteBuffer));
        for (int i = 0; i < 5; i++) {
            assertEquals(true, (char)byteBuffer.get() == (char)testBuffer.get());
        }
    }

    private PmPoolClient pmPoolClient;
}
