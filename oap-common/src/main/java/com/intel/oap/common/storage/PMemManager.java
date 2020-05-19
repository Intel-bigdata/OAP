package com.intel.oap.common.storage;

public class PMemManager {
    MemoryStats stats;

    PMemDataStore pMemDataStore;

    private static class PMemManagerInstance{
        private static final PMemManager instance = new PMemManager();
    }

    private PMemManager(){
        stats = new MemoryStats(100);
        pMemDataStore = new MemKindPMemDataStoreImpl(stats);
    }

    public static PMemManager getInstance(){
        return PMemManagerInstance.instance;
    }
}
