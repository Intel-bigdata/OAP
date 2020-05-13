package com.intel.oap.common.storage;

import java.util.Iterator;

//FIXME should new this by parameter instead of passing in by Spark
/**
 * store memta info such as map between chunkID and physical baseAddr in pmem.
 * provide methods to get chunks iterator with logicalID provided.
 */
public abstract class PMemDataStore {
    byte[] id;
    ChunkAPI impl;
    ChunkAPI fallback;
    FileChunk fileChunk;
    MemoryStats stats;

    public PMemDataStore(byte [] id, MemoryStats stats){
        this.id = id;
        this.stats = stats;
        fileChunk = new FileChunk();
    }

    public Iterator<Chunk> getInputChunkIterator(){
        return new Iterator<Chunk>() {
            long chuckID = 0;

            @Override
            public boolean hasNext() {
                throw new RuntimeException("Unsupported operation");
            }

            @Override
            public Chunk next() {
                chuckID++;
//                byte[] physicalID = Bytes.concat(PMemDataStore.LongToBytes(chuckID), id);
//                Chunk chuck = impl.getChunk(physicalID);
//                if (chuck == null) {
//                    return fileChunk;
//                }
                return null;
            }
        };
    }

    /**
     * provide trunk for output stream write, need update metadata for
     * this stream, like chunkID++, totalsize, etc. need implement methods next()
     * @param id
     * @param chunkSize
     * @return
     */
    public Iterator<Chunk> getOutputChunkIterator() {
        return null;
    }

    public abstract byte [] getPhysicalIDbyLogicalID(byte[] id);


}