package com.intel.oap.common.storage.stream;

import com.intel.oap.common.storage.memkind.MemkindMetaStore;
import com.intel.oap.common.storage.pmemblk.PMemBlkMetaStore;
import com.intel.oap.common.unsafe.PersistentMemoryPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;

public class PMemManager {
    private static final Logger logger = LoggerFactory.getLogger(PMemManager.class);
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

    public PMemManager(Properties properties){
        //FIXME how to get?
        chunkSize = Integer.valueOf(properties.getProperty("chunkSize"));
        long totalSize = Long.valueOf(properties.getProperty("totalSize"));
        String metaStore = properties.getProperty("metaStore");
        String initialPath = properties.getProperty("initialPath");
        long initialSize = Long.valueOf(properties.getProperty("initialSize"));
        logger.info(chunkSize + " " + totalSize + " " + metaStore + " " + initialPath);
        PersistentMemoryPlatform.initialize(initialPath, initialSize , 0);
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

    public int getChunkSize(){
        return chunkSize; //TODO get from configuration
    }

}
