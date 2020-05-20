package com.intel.oap.common.storage.stream;

import com.intel.oap.common.storage.memkind.MemkindChunkReader;
import com.intel.oap.common.storage.memkind.MemkindChunkWriter;

//FIXME should new this by parameter instead of passing in by Spark
/**
 * store memta info such as map between chunkID and physical baseAddr in pmem.
 * provide methods to get chunks iterator with logicalID provided.
 */
public class DataStore {
    private PMemManager pMemManager;

    public DataStore(PMemManager pMemManager){
        this.pMemManager = pMemManager;
    }

    public ChunkWriter getChunkWriter(byte[] logicalID){
        //FIXME
        return new MemkindChunkWriter(logicalID, pMemManager);
    }

    public ChunkReader getChunkReader(byte[] logicalID){
        //FIXME
        return new MemkindChunkReader(logicalID, pMemManager);
    }
}