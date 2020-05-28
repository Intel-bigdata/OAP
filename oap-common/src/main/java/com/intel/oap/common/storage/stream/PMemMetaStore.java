package com.intel.oap.common.storage.stream;

public interface PMemMetaStore {

     PMemPhysicalAddress getPhysicalAddressByID(byte[] id, int chunkID);

     void putMetaFooter(byte[] id, MetaData metaData);

     void putPhysicalAddress(byte[]id, int chunkID, PMemPhysicalAddress pMemPhysicalAddress);

     MetaData getMetaFooter(byte[] id);
}
