package com.intel.oap.common.storage.memkind;

import com.intel.oap.common.storage.stream.ChunkWriter;
import com.intel.oap.common.storage.stream.MetaData;
import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import com.intel.oap.common.unsafe.PersistentMemoryPlatform;
import com.intel.oap.common.util.MemCopyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MemkindChunkWriter extends ChunkWriter {
    public MemkindChunkWriter(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
    }
    private static final Logger logger = LoggerFactory.getLogger(MemkindChunkWriter.class);
    @Override
    protected PMemPhysicalAddress writeInternal(ByteBuffer byteBuffer) {
        int dataSizeInByte = byteBuffer.position();
        long baseAddr = PersistentMemoryPlatform.allocateVolatileMemory(dataSizeInByte);
        MemkindPMemPhysicalAddress pMemPhysicalAddress = new MemkindPMemPhysicalAddress(baseAddr, dataSizeInByte);
        // write the byte buffer to PMem
        MemCopyUtil.copyMemory(byteBuffer.array(), Unsafe.ARRAY_BYTE_BASE_OFFSET, null, baseAddr, dataSizeInByte);
        return  pMemPhysicalAddress;
    }

    /**
     * Truncates this stream to the given size.
     * Stream length: <PMemChunks> ---- <File> ---- <RemainingBuffer>, assum the stream is in open status
     *
     * <p> If the given size is less than the stream's current size then the stream
     * is truncated, discarding any bytes beyond the new end of the stream.  If
     * the given size is greater than or equal to the stream's current size then
     * the stream is not modified.
     * </p>
     *
     * @param  truncatePosition
     *         The new size, a non-negative byte count
     *
     * @throws  IllegalArgumentException
     *          If the new size is negative
     *
     * @throws IOException
     *          If some other I/O error occurs
     */
    protected void truncate(long truncatePosition) throws IOException {
        if(truncatePosition < 0) {
            throw new IllegalArgumentException("Negative size");
        }
        long discardedSize = position() + remainingBuffer.position() - truncatePosition;
        if(discardedSize <= 0) {
            logger.info("truncate position is beyond data size, won't discard any bytes in PMem/File");
            return;
        }
        int currentRemainingBufferPosition = remainingBuffer.position();
        // if only need to discard bytes in remainingBuffer
        if (discardedSize <= currentRemainingBufferPosition) {
            remainingBuffer.position(currentRemainingBufferPosition - (int)discardedSize);
            // no need to update metadata
            return;
        }
        // discard bytes in remaining buffer first
        remainingBuffer.clear();
        if(fallbackTriggered) {
            // if file already used for storing data, discard proper amount from it
            File file = new File(new String(logicalID));
            assert (file != null && file.exists());
            if(outputStream == null) {
                outputStream = new FileOutputStream(new String(logicalID), true);
            }
            long remainingDiscardSize = discardedSize - (file.length() +
                    currentRemainingBufferPosition);
            if (remainingDiscardSize <= 0) {
                // if only need to discard part of the file
                outputStream.getChannel().truncate(file.length() +
                        currentRemainingBufferPosition - discardedSize);
                // no need to update metadata
            }
            if (remainingDiscardSize > 0) {
                // if discard whole file and need to discard more, close the outputStream
                // and set it to null since ChunkOutputStream will open new one if need
                outputStream.getChannel().truncate(0);
                outputStream.close();
                outputStream = null;
                fallbackTriggered = false;
                // discard more PMem chunks
                discardChunks(remainingDiscardSize);
            }
        } else {
            // if no file triggered, discard from PMem chunks directly
            // remainingDiscardSize must be > 0
            long remainingDiscardSize = discardedSize - currentRemainingBufferPosition;
            discardChunks(remainingDiscardSize);
        }
        return;
    }

    private void discardChunks(long size) {
        assert(size > 0);
        while(size > 0) {
            // in this writer, chunkID indicates the chunk that will be wrote, but for truncate,
            // we should discard from chunkID-- which actually finished wrote.
            chunkID--;
            MemkindPMemPhysicalAddress pMemPhysicalAddress = (MemkindPMemPhysicalAddress)
                    pMemMetaStore.getPhysicalAddressByID(logicalID, chunkID);
            // discard whole chunk if truncated size bigger than it
            if(size >= pMemPhysicalAddress.getOffset()) {
                PersistentMemoryPlatform.freeMemory(pMemPhysicalAddress.getBaseAddress());
                pMemMetaStore.removePhysicalAddress(logicalID, chunkID);
                size -= pMemPhysicalAddress.getOffset();
            } else {
                // otherwise, only discard part of the chunk and update the chunk's offset in meta
                int originOffset = pMemPhysicalAddress.getOffset();
                pMemPhysicalAddress.setOffset(originOffset - (int)size);
                pMemMetaStore.putPhysicalAddress(logicalID, chunkID, pMemPhysicalAddress);
                size -= originOffset;
                // increase chunkID since later bytes should wrote to next chunk
                chunkID ++;
            }
        }
        pMemMetaStore.putMetaFooter(logicalID, new MetaData(fallbackTriggered, chunkID));
    }

    /**
     * length = length of bytes in (remainingBuffer + PMemChunks + file)
     * @return position, actual length of current stream
     */
    public long position()
    {
        int size = remainingBuffer.position();
        int currentTrunkID = 0;
        while(currentTrunkID < chunkID) {
            MemkindPMemPhysicalAddress pMemPhysicalAddress = (MemkindPMemPhysicalAddress) pMemMetaStore.getPhysicalAddressByID(logicalID, currentTrunkID);
            size += pMemPhysicalAddress.getOffset();
            currentTrunkID++;
        }
        // delete file
        File file = new File(new String(logicalID));
        if (file != null && file.exists()) {
            size += file.length();
        }
        return size;
    }
}
