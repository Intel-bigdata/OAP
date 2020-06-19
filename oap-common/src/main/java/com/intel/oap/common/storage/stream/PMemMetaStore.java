package com.intel.oap.common.storage.stream;

public interface PMemMetaStore {

     PMemPhysicalAddress getPhysicalAddressByID(byte[] id, int chunkID);

     void putMetaFooter(byte[] id, MetaData metaData);

     void removeMetaFooter(byte[] id);

     void putPhysicalAddress(byte[] id, int chunkID, PMemPhysicalAddress pMemPhysicalAddress);

     void removePhysicalAddress(byte[] id, int chunkID);

     MetaData getMetaFooter(byte[] id);
}
