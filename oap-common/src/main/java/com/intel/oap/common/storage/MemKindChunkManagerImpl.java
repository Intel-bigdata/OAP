package com.intel.oap.common.storage;

public class MemKindChunkManagerImpl implements ChunkManager {
    @Override
    public boolean contains(byte[] id) {
        return PMemManager.getInstance().pMemDataStore.chunkMap.contains(id);
    }

    @Override
    public void putChunk(byte[] id, Chunk chunk) {
        assert(!contains(id));
        PMemManager.getInstance().pMemDataStore.chunkMap.put(id, chunk);
    }

    @Override
    public Chunk getChunk(byte[] id) {
        assert(contains(id));
        return PMemManager.getInstance().pMemDataStore.chunkMap.get(id);
    }

    @Override
    public void remove(byte[] id) {

    }


}
