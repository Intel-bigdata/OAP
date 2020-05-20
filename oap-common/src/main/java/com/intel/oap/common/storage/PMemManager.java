package com.intel.oap.common.storage;

import com.intel.oap.common.storage.stream.PMemMetaStore;

import java.util.Properties;

public class PMemManager {
    private MemoryStats stats;

    private PMemMetaStore pMemMetaStore;

    public MemoryStats getStats() {
        return stats;
    }

    public void setStats(MemoryStats stats) {
        this.stats = stats;
    }

    public PMemMetaStore getpMemMetaStore() {
        return pMemMetaStore;
    }

    public void setpMemMetaStore(PMemMetaStore pMemMetaStore) {
        this.pMemMetaStore = pMemMetaStore;
    }

    private static class PMemManagerInstance{
        private static final PMemManager instance = new PMemManager();
    }

    private PMemManager(){
        setStats(new MemoryStats(100));
//        pMemDataStore = new MemKindDataStoreImpl(stats);
    }

    public PMemManager(Properties properties){
        //FIXME
        long totalSize = Long.valueOf(properties.getProperty("totalsize"));
        stats = new MemoryStats(totalSize);
        pMemMetaStore = new Me
    }

    public void close(){
//        pMemMetaStore.release();
    }

    public static PMemManager getInstance(){
        return PMemManagerInstance.instance;
    }


    public int getChunkSize(){
        return 10; //TODO get from configuration
    }

}
