package com.intel.oap.common.storage.stream;

import com.intel.oap.common.storage.memkind.MemkindMetaStore;

import java.util.Properties;

public class PMemManager {
    private MemoryStats stats;

    private PMemMetaStore pMemMetaStore;

    private int chunkSize;

    public MemoryStats getStats() {
        return stats;
    }

    public void setStats(MemoryStats stats) {
        this.stats = stats;
    }

    public PMemMetaStore getpMemMetaStore() {
        return pMemMetaStore;
    }

    private static class PMemManagerInstance{
        private static final PMemManager instance = new PMemManager();
    }

    private PMemManager(){
        setStats(new MemoryStats(100));
//        pMemDataStore = new MemKindDataStoreImpl(stats);
    }

    public PMemManager(Properties properties){
        //FIXME how to get?
        chunkSize = Integer.valueOf(properties.getProperty("chunkSize"));
        long totalSize = Long.valueOf(properties.getProperty("totalSize"));
        stats = new MemoryStats(totalSize);
        pMemMetaStore = new MemkindMetaStore();
    }

    public void close(){
//        pMemMetaStore.release();
    }

    public static PMemManager getInstance(){
        return PMemManagerInstance.instance;
    }


    public int getChunkSize(){
        return chunkSize; //TODO get from configuration
    }

}
