package com.intel.oap.common.storage.memkind;

import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.storage.stream.ChunkWriter;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import com.intel.oap.common.unsafe.PersistentMemoryPlatform;
import com.intel.oap.common.util.MemCopyUtil;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

public class MemkindChunkWriter extends ChunkWriter {
    public MemkindChunkWriter(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
    }

    @Override
    protected PMemPhysicalAddress writeInternal(ByteBuffer byteBuffer) {
        int dataSizeInByte = byteBuffer.position();
        long baseAddr = PersistentMemoryPlatform.allocateVolatileMemory(dataSizeInByte);
        MemkindPMemPhysicalAddress pMemPhysicalAddress = new MemkindPMemPhysicalAddress(baseAddr, dataSizeInByte);
        // write the byte buffer to PMem
        MemCopyUtil.copyMemory(byteBuffer.array(), Unsafe.ARRAY_BYTE_BASE_OFFSET, null, baseAddr, dataSizeInByte);
        return  pMemPhysicalAddress;
    }

    @Override
    protected void closeInternal() {
        //do nothing here for MemKind
    }
}
