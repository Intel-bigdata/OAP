package com.intel.oap.common.storage.stream;

public interface PMemMetaStore {
     PMemPhysicalAddress getPMemIDByLogicalID(byte[] id, int chunkID);

     void putMetaFooter(byte[] id, MetaData metaData);

     void putPMemID(byte[]id, int chunkID, PMemPhysicalAddress pMemPhysicalAddress);

     MetaData getMetaFooter(byte[] id);
}
