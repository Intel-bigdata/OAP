package com.intel.oap.common.storage.memkind;

import com.intel.oap.common.storage.stream.ChunkReader;
import com.intel.oap.common.storage.stream.MetaData;
import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.storage.stream.PMemMetaStore;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import com.intel.oap.common.unsafe.PersistentMemoryPlatform;
import com.intel.oap.common.util.MemCopyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.File;
import java.nio.ByteBuffer;

public class MemkindChunkReader extends ChunkReader {
    private static final Logger logger = LoggerFactory.getLogger(MemkindChunkReader.class);
    private PMemMetaStore pMemMetaStore;
    public MemkindChunkReader(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
        this.pMemMetaStore = pMemManager.getpMemMetaStore();
    }

    @Override
    protected int readFromPMem(PMemPhysicalAddress pMemPhysicalAddress, ByteBuffer data) {
        MemkindPMemPhysicalAddress memkindPMemPhysicalAddress = (MemkindPMemPhysicalAddress) pMemPhysicalAddress;
        long baseAddress = memkindPMemPhysicalAddress.getBaseAddress();
        int length = memkindPMemPhysicalAddress.getOffset();
        byte[] buffer = new byte[length];
        MemCopyUtil.copyMemory(null, baseAddress, buffer, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);
        data.put(buffer);
        return length;
    }

    @Override
    protected void freeFromPMem() {
        // TODO: may need refactor based on up-level usage
        // get metaFooter for this logicalID
        MetaData metaData = pMemMetaStore.getMetaFooter(logicalID);
        int currentTrunkID = 0;
        while(currentTrunkID < metaData.getTotalChunk()) {
            PMemPhysicalAddress pMemPhysicalAddress = pMemMetaStore.getPhysicalAddressByID(logicalID, currentTrunkID);
            PersistentMemoryPlatform.freeMemory(((MemkindPMemPhysicalAddress) pMemPhysicalAddress).getBaseAddress());
            pMemMetaStore.removePhysicalAddress(logicalID, currentTrunkID, pMemPhysicalAddress);
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
    }
}
