package com.intel.oap.common.storage.pmemblk;

import com.intel.oap.common.storage.stream.ChunkReader;
import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import com.intel.oap.common.unsafe.PMemBlockPlatform;

import java.nio.ByteBuffer;

public class PMemBlkChunkReader extends ChunkReader {

    private int chunkSize;

    public PMemBlkChunkReader(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
        this.chunkSize = pMemManager.getChunkSize();
    }

    protected int readFromPMem(PMemPhysicalAddress pMemPhysicalAddress, ByteBuffer data) {
        PMemBlkPhysicalAddress pMemBlkPhysicalAddress = (PMemBlkPhysicalAddress) pMemPhysicalAddress;
        int index = pMemBlkPhysicalAddress.getIndex();
        byte[] buffer = new byte[chunkSize];
        PMemBlockPlatform.read(buffer, index);
        data.put(buffer);
        return chunkSize;
    }

    @Override
    protected void freeFromPMem() {
        
    }

    @Override
    protected int getOffsetOfChunk(PMemPhysicalAddress pMemPhysicalAddress) {
        return  ((PMemBlkPhysicalAddress) pMemPhysicalAddress).getLength();
    }

    @Override
    protected byte[] readFromPMem(PMemPhysicalAddress pMemPhysicalAddress, int offsetInChunk, int remainingRequest){
        ByteBuffer data = ByteBuffer.wrap(new byte[remainingRequest -offsetInChunk]);
        readFromPMem(pMemPhysicalAddress, data);
        return data.array();
    }
}
