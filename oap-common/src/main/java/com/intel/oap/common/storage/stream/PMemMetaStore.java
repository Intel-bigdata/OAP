package com.intel.oap.common.storage.stream;


public interface PMemMetaStore {
     PMemID getPMemIDByLogicalID(byte[] id, int chunkID);

     PMemID putMetaFooter(byte[] id, MetaData metaData);

     void putPMemID(byte[]id, int chunkID, PMemID pMemID);

     MetaData getMetaFooter(byte[] id);
}
