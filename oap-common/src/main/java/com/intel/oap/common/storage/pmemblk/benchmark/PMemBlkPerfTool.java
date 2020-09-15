package com.intel.oap.common.storage.pmemblk.benchmark;

import com.intel.oap.common.storage.pmemblk.PMemBlkChunkReader;
import com.intel.oap.common.storage.pmemblk.PMemBlkChunkWriter;
import com.intel.oap.common.storage.stream.*;
import com.intel.oap.common.unsafe.PMemBlockPlatform;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PMemBlkPerfTool {


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("totalSize", args[0]);
        properties.setProperty("chunkSize", args[1]);
        properties.setProperty("metaStore", args[2]);
        properties.setProperty("storetype", args[3]);
        properties.setProperty("pmemkv_engine", "cmap");
        properties.setProperty("pmemkv_path", args[4]);
        properties.setProperty("pmemkv_size", args[5]);
        PMemManager pMemManager = new PMemManager(properties);
        ConcurrentHashMap<String, Byte[]> logicIds = new ConcurrentHashMap<>();

        long totalSize = Long.parseLong(args[0]);
        int chunkSize = Integer.parseInt(args[1]);
        int threadNum = Integer.parseInt(args[6]);
        int executeNumPerThread = (int) (totalSize / chunkSize / threadNum);

        PMemBlockPlatform.create(args[7], chunkSize, totalSize);

        ExecutorService executorWrite = Executors.newFixedThreadPool(threadNum);
        long startTimeForWrite = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            executorWrite.execute(new WriteThread(logicIds, pMemManager, executeNumPerThread));
        }
        executorWrite.shutdown();
        while (!executorWrite.isTerminated()) {
        }
        long endTimeForWrite = System.currentTimeMillis();
        System.out.println("Write Perf: " + totalSize / 1024 / 1024 + "mb, " + (endTimeForWrite - startTimeForWrite) + "ms");

        ExecutorService executorRead = Executors.newFixedThreadPool(threadNum);
        long startTimeForRead = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            executorRead.execute(new ReadThread(logicIds, pMemManager, executeNumPerThread));
        }
        executorRead.shutdown();
        while (!executorRead.isTerminated()) {
        }
        long endTimeForRead = System.currentTimeMillis();
        System.out.println("Read Perf: " + totalSize / 1024 / 1024 + "mb," + (endTimeForRead - startTimeForRead) + "ms");

        File pmemkvFile = new File(args[4]);
        pmemkvFile.delete();
        File pmemblkFile = new File(args[7]);
        pmemblkFile.delete();
    }

}

class WriteThread implements Runnable {

    private int executeNumPerThread;
    private ChunkWriter chunkWriter;
    private Random random = new Random();
    private byte[] bytes;

    public WriteThread(ConcurrentHashMap hashMap,
                       PMemManager pMemManager,
                       int executeNumPerThread) {
        this.executeNumPerThread = executeNumPerThread;
        byte[] logicId = new byte[]{(byte) Thread.currentThread().getId()};
        hashMap.put(Thread.currentThread().getName(), logicId);
        this.chunkWriter = new PMemBlkChunkWriter(logicId, pMemManager);
        bytes = new byte[pMemManager.getChunkSize()];
        random.nextBytes(bytes);
    }

    @Override
    public void run() {
        for (int i = 0; i < executeNumPerThread; i++) {
            try {
                chunkWriter.write(bytes, bytes.length);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

class ReadThread implements Runnable {

    private PMemManager pMemManager;
    private int executeNumPerThread;
    private ChunkReader chunkReader;

    public ReadThread(ConcurrentHashMap hashMap,
                      PMemManager pMemManager,
                      int executeNumPerThread) {
        this.pMemManager = pMemManager;
        this.executeNumPerThread = executeNumPerThread;
        this.chunkReader = new PMemBlkChunkReader(
                (byte[]) hashMap.get(Thread.currentThread().getName()), pMemManager);
    }

    @Override
    public void run() {
        for (int i = 0; i < executeNumPerThread; i++) {
            try {
                chunkReader.read(new byte[pMemManager.getChunkSize()]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
