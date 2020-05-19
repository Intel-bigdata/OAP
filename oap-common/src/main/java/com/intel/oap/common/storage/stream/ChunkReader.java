package com.intel.oap.common.storage.stream;

import com.intel.oap.common.storage.PMemManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ChunkReader {
    private PMemManager pMemManager;
    private byte[] logicalID;
    private int chunkID = 0;
    private ByteBuffer remainingBuffer;
    private MetaData metaData;
    private FileInputStream inputStream = null;

    public ChunkReader(byte[] logicalID, PMemManager pMemManager){
        this.logicalID = logicalID;
        this.pMemManager = pMemManager;
        this.remainingBuffer = ByteBuffer.allocateDirect(pMemManager.getChunkSize());
        this.metaData = pMemManager.getpMemMetaStore().getMetaFooter(logicalID);
    }

    public int read(byte b[]) throws IOException {
        int i = 0;
        while(i < b.length){
            if(remainingBuffer.remaining() < 0){
                loadToByteBuffer();
            }
            b[i] = remainingBuffer.get();
            i++;
        }
        return i;
    }

    private void loadToByteBuffer() throws IOException {
        if(chunkID == metaData.getTotalChunk() && metaData.isHasDiskData()){
            readFromDisk(remainingBuffer);
        }
        PMemID id = pMemManager.getpMemMetaStore().getPMemIDByLogicalID(logicalID, chunkID);
        readFromPMem(id, remainingBuffer);
    }

    private void readFromDisk(ByteBuffer remainingBuffer) throws IOException {
        inputStream = new FileInputStream("/tmp/helloworld");
        byte b[] = new byte[pMemManager.getChunkSize()];
        while (inputStream.available() != 0 && remainingBuffer.position() != remainingBuffer.limit()) {
            int size = inputStream.read(b);
            for (int j = 0; j < size; j++) {
                remainingBuffer.put(b[j]);
            }
        }
    }

    abstract int readFromPMem(PMemID id, ByteBuffer data);
}
