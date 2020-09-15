package com.intel.oap.common.storage.pmemblk;

import com.intel.oap.common.storage.stream.ChunkWriter;
import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import com.intel.oap.common.unsafe.PMemBlockPlatform;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PMemBlkChunkWriter extends ChunkWriter {

    private PMemBlkMetaStore pMemMetaStore;

    public PMemBlkChunkWriter(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
        this.pMemMetaStore = (PMemBlkMetaStore) pMemManager.getpMemMetaStore();
    }

    @Override
    protected PMemPhysicalAddress writeInternal(ByteBuffer byteBuffer) {
        int length = byteBuffer.position();
        int index = pMemMetaStore.nextPMemBlockIndex();
        PMemBlockPlatform.write(byteBuffer.array(), index);
        return new PMemBlkPhysicalAddress(index, length);
    }

    @Override
    protected void closeInternal() {
    }


    @Override
    protected void truncate(long size) throws IOException {
    }

    @Override
    public long position() {
        return 0L;
    }
}
