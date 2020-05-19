package com.intel.oap.common.storage;

/**
 * expose methods for high level stream to write/read data from different backend
 */
public interface ChunkManager {
//    /**
//     * allocate memory from pmem
//     * @param id
//     * @return address
//     */
//    public Chunk allocate(byte[] id, long length);

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
     * remove from map
     */
    public void remove(byte[] id);

//    /**
//     * free pmem space when chunk life cycle end
//     * @param chunk
//     */
//    public void free(Chunk chunk);
}