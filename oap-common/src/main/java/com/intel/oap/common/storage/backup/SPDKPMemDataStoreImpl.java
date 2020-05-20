package com.intel.oap.common.storage.backup;

import com.intel.oap.common.storage.stream.MemoryStats;

import java.util.Iterator;

@Deprecated
public class SPDKPMemDataStoreImpl {
    public SPDKPMemDataStoreImpl(byte[] id, MemoryStats stats) {
//        super(id, stats);
    }

    public Iterator<Chunk> getOutputChunkIterator(byte[] logicalID) {
        return null;
    }

    public void freeChunks(byte[] logicalID) {

    }

    public byte[] getPhysicalIDbyLogicalID(byte[] logicalID, long currentTrunkID) {
        return new byte[0];
    }
}
