package com.intel.oap.common.storage.stream;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ChunkReader {
    protected PMemManager pMemManager;
    protected byte[] logicalID;
    private int chunkID = 0;
    private ByteBuffer remainingBuffer;
    private MetaData metaData;
    private FileInputStream inputStream = null;

    public ChunkReader(byte[] logicalID, PMemManager pMemManager){
        this.logicalID = logicalID;
        this.pMemManager = pMemManager;
        this.remainingBuffer = ByteBuffer.wrap(new byte[pMemManager.getChunkSize()]);
        this.metaData = pMemManager.getpMemMetaStore().getMetaFooter(logicalID);
    }

    public int read(byte b[]) throws IOException {
        int i = 0;
        remainingBuffer.clear();
        int remainingSize = loadData();
        remainingBuffer.flip();
        while (remainingSize > 0 && i < b.length) {
            b[i] = remainingBuffer.get();
            i++;
            remainingSize--;
            // when current chunk finished read but byte array is not full-filled, switch to next chunk
            if (remainingSize == 0 && i < b.length) {
                remainingBuffer.clear();
                remainingSize = loadData();
                remainingBuffer.flip();
            }
        }

        return i;
    }

    private int loadData() throws IOException {
        int size = 0;
        if (chunkID == metaData.getTotalChunk() && metaData.isHasDiskData()) {
            size = readFromDisk(remainingBuffer);
        } else {
            PMemPhysicalAddress id = pMemManager.getpMemMetaStore().getPhysicalAddressByID(logicalID, chunkID);
            chunkID++;
            size = readFromPMem(id, remainingBuffer);
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
