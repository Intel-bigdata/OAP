package com.intel.oap.common.storage.stream;

import com.intel.oap.common.storage.memkind.MemkindChunkReader;
import com.intel.oap.common.storage.memkind.MemkindChunkWriter;
import com.intel.oap.common.storage.pmemblk.PMemBlkChunkReader;
import com.intel.oap.common.storage.pmemblk.PMemBlkChunkWriter;

import java.util.Properties;

//FIXME should new this by parameter instead of passing in by Spark

/**
 * store memta info such as map between chunkID and physical baseAddr in pmem.
 * provide methods to get chunks iterator with logicalID provided.
 */
public class DataStore {
    private PMemManager pMemManager;
    private StoreType storeType;

    public DataStore(PMemManager pMemManager, Properties properties) {
        this.pMemManager = pMemManager;
        storeType = StoreType.valueOf(properties.getProperty("dataStore").toUpperCase());
    }

    public DataStore(PMemManager pMemManager, String dataStore) {
        this.pMemManager = pMemManager;
        storeType = StoreType.valueOf(dataStore.toUpperCase());
    }

    public ChunkWriter getChunkWriter(byte[] logicalID) {
        ChunkWriter chunkWriter;
        switch (storeType) {
            case MEMKIND:
                chunkWriter = new MemkindChunkWriter(logicalID, pMemManager);
                break;
            case LIBPMEM:
                chunkWriter = new PMemBlkChunkWriter(logicalID, pMemManager);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + storeType);
        }
        return chunkWriter;
    }

    public ChunkReader getChunkReader(byte[] logicalID) {
        ChunkReader chunkReader;
        switch (storeType) {
            case MEMKIND:
                chunkReader = new MemkindChunkReader(logicalID, pMemManager);
                break;
            case LIBPMEM:
                chunkReader = new PMemBlkChunkReader(logicalID, pMemManager);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + storeType);
        }
        return chunkReader;
    }
}