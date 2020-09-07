package com.intel.oap.common.storage.pmemblk;

import com.intel.oap.common.storage.stream.MetaData;
import com.intel.oap.common.storage.stream.PMemMetaStore;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import io.pmem.pmemkv.Database;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class PMemBlkMetaStore implements PMemMetaStore {

    private AtomicInteger index = new AtomicInteger(0);

    Database<String, String> pmemkvDB;

    public PMemBlkMetaStore(Properties properties) {
        String pmemkvEngine = properties.getProperty("pmemkv_engine");
        String pmemkvPath = properties.getProperty("pmemkv_path");
        String pmemkvSize = properties.getProperty("pmemkv_size");
        pmemkvDB = PMemKVDatabase.open(pmemkvEngine, pmemkvPath, Long.parseLong(pmemkvSize));
        String currentPMemBlkIndex = pmemkvDB.getCopy("currentPMemBlkIndex");
        if (currentPMemBlkIndex == null)
            currentPMemBlkIndex = "0";
        index.set(Integer.parseInt(currentPMemBlkIndex));
    }

    @Override
    public PMemPhysicalAddress getPhysicalAddressByID(byte[] id, int chunkID) {
        String idStr = new String(id);
        String indexStr = pmemkvDB.getCopy(idStr + "_" + chunkID + "_index");
        String lengthStr = pmemkvDB.getCopy(idStr + "_" + chunkID + "_length");
        if (indexStr == null || lengthStr == null)
            return null;
        return new PMemBlkPhysicalAddress(Integer.parseInt(indexStr), Integer.parseInt(lengthStr));
    }

    @Override
    public void putMetaFooter(byte[] id, MetaData metaData) {
        String idStr = new String(id);
        pmemkvDB.put(idStr + "_hasDiskData", String.valueOf(metaData.isHasDiskData()));
        pmemkvDB.put(idStr + "_totalChunk", String.valueOf(metaData.getTotalChunk()));
        pmemkvDB.put("currentPMemBlkIndex", String.valueOf(index.intValue()));
    }

    @Override
    public void removeMetaFooter(byte[] id) {
        String idStr = new String(id);
        pmemkvDB.remove(idStr + "_hasDiskData");
        pmemkvDB.remove(idStr + "_totalChunk");
    }

    @Override
    public void putPhysicalAddress(byte[] id, int chunkID, PMemPhysicalAddress pMemPhysicalAddress) {
        String idStr = new String(id);
        PMemBlkPhysicalAddress pMemBlkPhysicalAddress = (PMemBlkPhysicalAddress) pMemPhysicalAddress;
        int index = pMemBlkPhysicalAddress.getIndex();
        int length = pMemBlkPhysicalAddress.getLength();
        pmemkvDB.put(idStr + "_" + chunkID + "_index", String.valueOf(index));
        pmemkvDB.put(idStr + "_" + chunkID + "_length", String.valueOf(length));
    }

    @Override
    public void removePhysicalAddress(byte[] id, int chunkID) {
        String idStr = new String(id);
        pmemkvDB.remove(idStr + "_" + chunkID);
    }

    @Override
    public MetaData getMetaFooter(byte[] id) {
        String idStr = new String(id);
        String hasDiskDataStr = pmemkvDB.getCopy(idStr + "_hasDiskData");
        String totalChunkStr = pmemkvDB.getCopy(idStr + "_totalChunk");
        if (hasDiskDataStr == null) hasDiskDataStr = "false";
        if (totalChunkStr == null) totalChunkStr = "0";
        return new MetaData(Boolean.parseBoolean(hasDiskDataStr), Integer.parseInt(totalChunkStr));
    }

    public int nextPMemBlockIndex() {
        return index.getAndIncrement();
    }

}
