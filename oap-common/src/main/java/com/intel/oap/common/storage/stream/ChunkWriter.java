package com.intel.oap.common.storage.stream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ChunkWriter {
    protected PMemManager pMemManager;
    protected PMemMetaStore pMemMetaStore;
    protected byte[] logicalID;
    protected int chunkID = 0;
    protected ByteBuffer remainingBuffer;
    protected boolean fallbackTriggered = false;
    protected FileOutputStream outputStream = null;

    public ChunkWriter(byte[] logicalID, PMemManager pMemManager) {
        this.logicalID = logicalID;
        this.pMemManager = pMemManager;
        this.pMemMetaStore = pMemManager.getpMemMetaStore();
        remainingBuffer = ByteBuffer.wrap(new byte[pMemManager.getChunkSize()]);
    }

    public abstract long position();

    public void write(byte b) throws IOException {
        remainingBuffer.put(b);
        if (remainingBuffer.position() == pMemManager.getChunkSize()) {
            flushBufferByChunk(remainingBuffer);
            remainingBuffer.clear();
        }
    }

    public void write(byte[] bytes, int len) throws IOException {
        // FIXME optimize this by avoiding one-by-one add. A new data structure can used like simple array
        if (bytes == null || bytes.length == 0) {
            return;
        }
        // if remaining buffer is empty and the bytes length is equal to chunkSize, directly flush it
        // without copying to remaining buffer.
        if (bytes.length == pMemManager.getChunkSize()) {
            if (remainingBuffer.position() > 0) {
                flushBufferByChunk(remainingBuffer);
                remainingBuffer.clear();
            }
            ByteBuffer wrapBuffer = ByteBuffer.wrap(bytes);
            wrapBuffer.position(len);
            flushBufferByChunk(wrapBuffer);
        } else {
            int i = 0, j = remainingBuffer.position();
            while (i < len) {
                if (j == pMemManager.getChunkSize()) {
                    j = 0;
                    // Flush buffer through chunk writer
                    flushBufferByChunk(remainingBuffer);
                    // clear content of remainingBuffer
                    remainingBuffer.clear();
                }
                remainingBuffer.put(bytes[i]);
                i++;
                j++;
            }
            if (j == pMemManager.getChunkSize()) {
                // Flush buffer through chunk writer
                flushBufferByChunk(remainingBuffer);
                remainingBuffer.clear();
            }
        }

    }

    private void flushBufferByChunk(ByteBuffer byteBuffer) throws IOException {
        int dataSizeInByte = byteBuffer.position();
        if (!fallbackTriggered && pMemManager.getStats().getRemainingSize() >= dataSizeInByte) {
            try {
                PMemPhysicalAddress id = writeInternal(byteBuffer);
                pMemManager.getStats().increaseSize(dataSizeInByte);
                pMemMetaStore.putPhysicalAddress(logicalID, chunkID, id);
                chunkID++;
            } catch (RuntimeException re) {
                // TODO Log Warning
                fallbackTriggered = true;
                flushToDisk(byteBuffer);
            }
        } else {
            flushToDisk(byteBuffer);
        }
    }

    private void flushToDisk(ByteBuffer byteBuffer) throws IOException {
        if (outputStream == null) {
            //FIXME
            outputStream = new FileOutputStream(new String(logicalID), true);
            fallbackTriggered = true;
        }
        outputStream.write(byteBuffer.array(), 0, byteBuffer.position());
        outputStream.flush();
        byteBuffer.clear();
    }

    public void close() throws IOException {
        // if remaining buffer has valid elements, write them to output stream
        if(remainingBuffer.position() > 0){
            flushBufferByChunk(remainingBuffer);
            remainingBuffer.clear();
        }
        pMemMetaStore.putMetaFooter(logicalID, new MetaData(fallbackTriggered, chunkID));

        closeInternal();
    }

    protected abstract PMemPhysicalAddress writeInternal(ByteBuffer byteBuffer);

    /**
     * Do some clean up work if needed.
     */
    protected void closeInternal() throws IOException {
        if(outputStream != null) {
            outputStream.close();
            outputStream = null;
        }
    }

    protected abstract void truncate(long size) throws IOException;

}
