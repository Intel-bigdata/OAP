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

    @Override
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
}
