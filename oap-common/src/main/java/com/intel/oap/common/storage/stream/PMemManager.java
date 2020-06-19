package com.intel.oap.common.storage.stream;

import com.intel.oap.common.storage.memkind.MemkindMetaStore;
import com.intel.oap.common.storage.pmemblk.PMemBlkMetaStore;

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
        String metaStore = properties.getProperty("metaStore");
        stats = new MemoryStats(totalSize);

        switch (metaStore) {
            case "memkind":
                pMemMetaStore = new MemkindMetaStore();
                break;
            case "pmemblk":
                pMemMetaStore = new PMemBlkMetaStore(properties);
                break;
        }
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
