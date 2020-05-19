package com.intel.oap.common.storage.chunk;

import com.intel.oap.common.storage.meta.Chunk;
import com.intel.oap.common.storage.meta.PMemChunk;
import com.intel.oap.common.unsafe.PersistentMemoryPlatform;

public class MemKindChunkAPIImpl implements ChunkAPI {
    @Override
    public PMemChunk allocate(byte[] id, long length) {
        try {
            // allocate address from PersistentMemoryPlatform
            long baseAddr = PersistentMemoryPlatform.allocateVolatileMemory(length);
            PMemChunk memKindChunk = new PMemChunk(baseAddr, length);
            return  memKindChunk;
        } catch (OutOfMemoryError e) {
            e.printStackTrace();
            // throw new OutOfMemoryError("out of memory!");
            return null;
        }
    }

    @Override
    public void free(Chunk chunk) {
        assert(chunk instanceof PMemChunk);
        PersistentMemoryPlatform.freeMemory(((PMemChunk) chunk).getBaseAddress());
    }
}
