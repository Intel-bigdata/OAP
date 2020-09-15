package com.intel.oap.common.storage.memkind;

import com.intel.oap.common.storage.stream.ChunkReader;
import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.storage.stream.PMemMetaStore;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import com.intel.oap.common.unsafe.PersistentMemoryPlatform;
import com.intel.oap.common.util.MemCopyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Unsafe;

import java.io.File;

public class MemkindChunkReader extends ChunkReader {
    private static final Logger logger = LoggerFactory.getLogger(MemkindChunkReader.class);
    private PMemMetaStore pMemMetaStore;
    public MemkindChunkReader(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
        this.pMemMetaStore = pMemManager.getpMemMetaStore();
        this.metaData = pMemMetaStore.getMetaFooter(logicalID);
    }

    // read from specific chunk at starting point offsetInChunk
    @Override
    protected byte[] readFromPMem(PMemPhysicalAddress pMemPhysicalAddress, int offsetInChunk, int remainingRequest) {
        MemkindPMemPhysicalAddress memkindPMemPhysicalAddress = (MemkindPMemPhysicalAddress) pMemPhysicalAddress;
        long baseAddress = memkindPMemPhysicalAddress.getBaseAddress();
        int length = memkindPMemPhysicalAddress.getOffset() - offsetInChunk;
        if(length <= 0) {
            return null;
        }
        int readBytes = Math.min(length, remainingRequest);
        byte[] buffer = new byte[readBytes];
        MemCopyUtil.copyMemory(null, baseAddress + offsetInChunk, buffer, Unsafe.ARRAY_BYTE_BASE_OFFSET, readBytes);
        return buffer;
    }

    @Override
    protected int getOffsetOfChunk(PMemPhysicalAddress pMemPhysicalAddress) {
        return  ((MemkindPMemPhysicalAddress) pMemPhysicalAddress).getOffset();
    }

    @Override
    protected void freeFromPMem() {
        // TODO: may need refactor based on up-level usage
        if(metaData != null) {
            int currentTrunkID = 0;
            while(currentTrunkID < metaData.getTotalChunk()) {
                MemkindPMemPhysicalAddress pMemPhysicalAddress = (MemkindPMemPhysicalAddress) pMemMetaStore.getPhysicalAddressByID(logicalID, currentTrunkID);
                PersistentMemoryPlatform.freeMemory(pMemPhysicalAddress.getBaseAddress());
                pMemManager.getStats().decreaseSize(pMemPhysicalAddress.getOffset());
                pMemMetaStore.removePhysicalAddress(logicalID, currentTrunkID);
                currentTrunkID++;
            }
            // delete file
            File file = new File(new String(logicalID));
            if (file != null && file.exists()) {
                if (!file.delete()) {
                    logger.error("Was unable to delete file {}", file.getAbsolutePath());
                }
            }
            pMemMetaStore.removeMetaFooter(logicalID);
            metaData = null;
        }
    }
}
