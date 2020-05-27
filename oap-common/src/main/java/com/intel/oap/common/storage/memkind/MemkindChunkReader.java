package com.intel.oap.common.storage.memkind;

import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.storage.stream.ChunkReader;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;

import java.nio.ByteBuffer;

public class MemkindChunkReader extends ChunkReader {
    public MemkindChunkReader(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
    }

    @Override
    protected int readFromPMem(PMemPhysicalAddress id, ByteBuffer data) {
        return 0;
    }
}
