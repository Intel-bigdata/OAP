package com.intel.oap.common.storage.memkind;

import com.intel.oap.common.storage.stream.MetaData;
import com.intel.oap.common.storage.stream.PMemID;
import com.intel.oap.common.storage.stream.PMemMetaStore;

import java.util.concurrent.ConcurrentHashMap;

//TODO design point, how to store data on PMEM
public class MemkindMetaStore implements PMemMetaStore {
    ConcurrentHashMap<String, PMemID> PMemHashMap = new ConcurrentHashMap();
    ConcurrentHashMap<String, MetaData> metaHashMap = new ConcurrentHashMap();

    @Override
    public PMemID getPMemIDByLogicalID(byte[] id, int chunkID) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(chunkID).append(new String(id));
        return PMemHashMap.get(keyBuilder.toString());
    }

    @Override
    public void putMetaFooter(byte[] id, MetaData metaData) {
        metaHashMap.put(new String(id), metaData);
    }

    @Override
    public void putPMemID(byte[] id, int chunkID, PMemID pMemID) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(chunkID).append(new String(id));
        PMemHashMap.put(keyBuilder.toString(), pMemID);
    }

    @Override
    public MetaData getMetaFooter(byte[] id) {
        return metaHashMap.get(new String(id));
    }
}
