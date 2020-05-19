package com.intel.oap.common.storage;

import com.intel.oap.common.storage.meta.Chunk;
import com.intel.oap.common.storage.stream.DataStore;

import java.util.Iterator;

@Deprecated
public class SPDKPMemDataStoreImpl extends DataStore {
    public SPDKPMemDataStoreImpl(byte[] id, MemoryStats stats) {
        super(id, stats);
    }

    @Override
    public Iterator<Chunk> getOutputChunkIterator(byte[] logicalID) {
        return null;
    }

    @Override
    public void freeChunks(byte[] logicalID) {

    }

    @Override
    public byte[] getPhysicalIDbyLogicalID(byte[] logicalID, long currentTrunkID) {
        return new byte[0];
    }
}
