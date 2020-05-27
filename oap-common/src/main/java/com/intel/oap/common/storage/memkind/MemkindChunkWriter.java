package com.intel.oap.common.storage.memkind;

import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.storage.stream.ChunkWriter;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;

import java.nio.ByteBuffer;

public class MemkindChunkWriter extends ChunkWriter {
    public MemkindChunkWriter(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
    }

    @Override
    protected PMemPhysicalAddress writeInternal(ByteBuffer byteBuffer) {
        //TODO
        return null;
    }

    @Override
    protected void closeInternal() {
        //TODO
    }
}
