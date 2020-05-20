package com.intel.oap.common.storage.stream;

import java.util.concurrent.ConcurrentHashMap;

//TODO design point, how to store data on PMEM
public class PMDKMetaStore implements PMemMetaStore {
    ConcurrentHashMap<String, PMemID> concurrentHashMap = new ConcurrentHashMap();

    @Override
    public PMemID getPMemIDByLogicalID(byte[] id, int chunkID) {
        return null;
    }

    @Override
    public PMemID putMetaFooter(byte[] id, MetaData metaData) {
        return null;
    }

    @Override
    public void putPMemID(byte[] id, int chunkID, PMemID pMemID) {

    }

    @Override
    public MetaData getMetaFooter(byte[] id) {
        return null;
    }
}
