package com.intel.oap.common.storage.chunk;

import com.intel.oap.common.storage.meta.Chunk;

public interface ChunkAPI {
    /**
     * allocate memory from pmem
     * @param id
     * @return address
     */
    public Chunk allocate(byte[] id, long length);

    /**
     * free pmem space when chunk life cycle end
     * @param chunk
     */
    public void free(Chunk chunk);
}
