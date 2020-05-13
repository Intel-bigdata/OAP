package com.intel.oap.common.storage;

/**
 * expose methods for high level stream to write/read data from different backend
 */
public interface ChunkAPI {

    /**
     * check whether this chunk exists FIXME do we need this?
     * @param id trunkID
     * @return
     */
    boolean contains(byte[] id);

    /**
     *
     * @param id trunkID
     * @return base address of trunk with id
     */
    public Chunk getChunk(byte[] id);


    /**
     * update pMemManager.pMemDataStore with <trunkID, pMemBlock>   ?????
     * @param id
     * @param chunk
     */
    public void putChunk(byte[] id, Chunk chunk);


    /**
     * action when read done of current Chunk
     */
    public void release();

    /**
     * free pmem space when chunk life cycle end
     * @param chunk
     */
    public void free(Chunk chunk);
}