package com.intel.oap.common.storage.stream;

import com.intel.oap.common.storage.backup.PMemManager;

import java.nio.ByteBuffer;

public class MemkindChunkWriter extends ChunkWriter {
    public MemkindChunkWriter(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
    }

    @Override
    PMemID writeInternal(ByteBuffer byteBuffer) {
        //TODO
        return null;
    }

    @Override
    void closeInternal() {
        //TODO
    }
}
