package com.intel.oap.common.storage.stream;

import com.intel.oap.common.storage.PMemManager;

import java.nio.ByteBuffer;

public class MemkindChunkReader extends ChunkReader {
    public MemkindChunkReader(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
    }

    @Override
    int readFromPMem(PMemID id, ByteBuffer data) {
        return 0;
    }
}
