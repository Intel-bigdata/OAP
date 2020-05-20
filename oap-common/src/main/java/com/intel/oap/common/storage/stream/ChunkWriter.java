package com.intel.oap.common.storage.stream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ChunkWriter {
    private PMemManager pMemManager;
    private byte[] logicalID;
    private int chunkID = 0;
    private ByteBuffer remainingBuffer;
    private boolean fallbackTriggered = false;
    private FileOutputStream outputStream = null;

    public ChunkWriter(byte[] logicalID, PMemManager pMemManager) {
        this.logicalID = logicalID;
        this.pMemManager = pMemManager;
//        this.remainingBuffer = ByteBuffer.allocateDirect(pMemManager.getChunkSize());
        remainingBuffer = ByteBuffer.wrap(new byte[pMemManager.getChunkSize()]);
    }


    public void write(byte[] bytes) throws IOException {
        // FIXME optimize this by avoiding one-by-one add. A new data structure can used like simple array
        if (bytes == null || bytes.length == 0) {
            return;
        }
        int i = 0, j = 0;
        while (i < bytes.length) {
            if (j == pMemManager.getChunkSize()) {
                j = 0;
                // Flush buffer through chunk writer
                flushBufferByChunk(remainingBuffer);
            }
            remainingBuffer.put(bytes[i]);
            i++;
            j++;
        }
        if(j == pMemManager.getChunkSize()){
            // Flush buffer through chunk writer
            flushBufferByChunk(remainingBuffer);
        }
    }

    private void flushBufferByChunk(ByteBuffer byteBuffer) throws IOException {
        int dataSizeInByte = byteBuffer.position();
        if (!fallbackTriggered && pMemManager.getStats().getRemainingSize() > dataSizeInByte) {
            try {
                PMemID id = writeInternal(byteBuffer);
                chunkID++;
                pMemManager.getStats().increaseSize(dataSizeInByte);
                pMemManager.getpMemMetaStore().putPMemID(logicalID, chunkID, id);
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
            outputStream = new FileOutputStream("/tmp/helloworld");
            fallbackTriggered = true;
        }
        outputStream.write(byteBuffer.array());
        byteBuffer.clear();
    }

    public void close() throws IOException {
        if(remainingBuffer.hasRemaining()){
            flushBufferByChunk(remainingBuffer);
        }
        pMemManager.getpMemMetaStore().putMetaFooter(logicalID, new MetaData(fallbackTriggered, chunkID));

        closeInternal();
    }

    protected abstract PMemID writeInternal(ByteBuffer byteBuffer);

    /**
     * Do some clean up work if needed.
     */
    protected abstract void closeInternal();
}
