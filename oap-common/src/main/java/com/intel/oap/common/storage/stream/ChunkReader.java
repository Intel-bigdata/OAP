package com.intel.oap.common.storage.stream;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ChunkReader {
    protected PMemManager pMemManager;
    protected byte[] logicalID;
    private int chunkID = 0;
    private int chunkSize = 0;
    private ByteBuffer remainingBuffer;
    private MetaData metaData;
    private FileInputStream inputStream = null;

    public ChunkReader(byte[] logicalID, PMemManager pMemManager){
        this.logicalID = logicalID;
        this.pMemManager = pMemManager;
        this.chunkSize = pMemManager.getChunkSize();
        this.remainingBuffer = ByteBuffer.wrap(new byte[chunkSize]);
        remainingBuffer.flip();
        this.metaData = pMemManager.getpMemMetaStore().getMetaFooter(logicalID);
    }

    public int read() throws IOException {
        while (true) {
            if (remainingBuffer.hasRemaining()) {
                return remainingBuffer.get();
            }
            remainingBuffer.clear();
            int size = loadData();
            remainingBuffer.limit(size);
            remainingBuffer.flip();

            if (!remainingBuffer.hasRemaining()) {
                return -1;
            }
        }
    }

    public int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }

    public int read(byte b[], int off, int len) throws IOException {
        int read = 0;
        while (len > 0) {
            if (remainingBuffer.hasRemaining()) {
                int length = Math.min(len, remainingBuffer.remaining());
                remainingBuffer.get(b, off, length);
                off += length;
                len -= length;
                read += length;
            } else {
                remainingBuffer.clear();
//                remainingBuffer.limit(chunkSize);
                int size = loadData();
                remainingBuffer.limit(size);
                if (remainingBuffer.position() == 0) {
                    break;
                }
                remainingBuffer.flip();
            }
        }
        return read == 0 ? -1 : read;
    }

    private int loadData() throws IOException {
        int size = 0;
        if (chunkID == metaData.getTotalChunk() && metaData.isHasDiskData()) {
            size = readFromDisk(remainingBuffer);
        } else {
            PMemPhysicalAddress id = pMemManager.getpMemMetaStore().getPhysicalAddressByID(logicalID, chunkID);
            if (id != null) {
                chunkID++;
                size = readFromPMem(id, remainingBuffer);
            } else {
                size = 0;
            }
        }
        return size;
    }

    private int readFromDisk(ByteBuffer remainingBuffer) throws IOException {
        if (inputStream == null){
            inputStream = new FileInputStream("/tmp/helloworld");
        }
        byte b[] = new byte[pMemManager.getChunkSize()];
        int size = 0;
        if (inputStream.available() != 0) {
            size = inputStream.read(b);
            for (int j = 0; j < size; j++) {
                remainingBuffer.put(b[j]);
            }
        }
        return size;
    }

    /**
     * read data from a address mappted to physical ID
     * @param id
     * @param data
     * @return
     */
    protected abstract int readFromPMem(PMemPhysicalAddress id, ByteBuffer data);

    protected abstract void freeFromPMem();
}
