package com.intel.oap.common.storage.memkind;

import com.intel.oap.common.storage.stream.MetaData;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import com.intel.oap.common.storage.stream.PMemMetaStore;

import java.util.concurrent.ConcurrentHashMap;

//TODO design point, how to store data on PMEM
public class MemkindMetaStore implements PMemMetaStore {
    ConcurrentHashMap<String, MemkindPMemPhysicalAddress> pMemHashMap = new ConcurrentHashMap();
    ConcurrentHashMap<String, MetaData> metaHashMap = new ConcurrentHashMap();

    @Override
    public PMemPhysicalAddress getPhysicalAddressByID(byte[] id, int chunkID) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(chunkID).append(new String(id));
        return pMemHashMap.get(keyBuilder.toString());
    }

    @Override
    public void putMetaFooter(byte[] id, MetaData metaData) {
        metaHashMap.put(new String(id), metaData);
    }

    @Override
    public void removeMetaFooter(byte[] id) {
        metaHashMap.remove(new String(id));
    }
    @Override
    public void putPhysicalAddress(byte[] id, int chunkID, PMemPhysicalAddress pMemPhysicalAddress) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(chunkID).append(new String(id));
        pMemHashMap.put(keyBuilder.toString(), (MemkindPMemPhysicalAddress) pMemPhysicalAddress);
    }

    @Override
    public void removePhysicalAddress(byte[] id, int chunkID) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(chunkID).append(new String(id));
        pMemHashMap.remove(keyBuilder.toString());
    }

    @Override
    public MetaData getMetaFooter(byte[] id) {
        return metaHashMap.get(new String(id));
    }
}
