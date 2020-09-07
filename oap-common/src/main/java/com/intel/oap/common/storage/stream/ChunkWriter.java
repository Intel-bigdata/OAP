package com.intel.oap.common.storage.stream;

import sun.nio.ch.DirectBuffer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ChunkWriter {
    protected PMemManager pMemManager;
    protected byte[] logicalID;
    protected int chunkID = 0;
    private ByteBuffer remainingBuffer;
    private boolean fallbackTriggered = false;
    private FileOutputStream outputStream = null;

    public ChunkWriter(byte[] logicalID, PMemManager pMemManager) {
        this.logicalID = logicalID;
        this.pMemManager = pMemManager;
//        this.remainingBuffer = ByteBuffer.allocateDirect(pMemManager.getChunkSize());
        remainingBuffer = ByteBuffer.wrap(new byte[pMemManager.getChunkSize()]);
    }

    public void write(final byte[] bytes, int off, int len) throws IOException {
        while (len > 0) {
            int length = Math.min(len, remainingBuffer.remaining());
            remainingBuffer.put(bytes, off, length);
            if (!remainingBuffer.hasRemaining()) {
                flushBufferByChunk(remainingBuffer);
            }
            len -= length;
            off += length;
        }
        // TODO： do not flush immediately to avoid wasting space of last chunk
        if (remainingBuffer.position() > 0) {
            flushBufferByChunk(remainingBuffer);
        }
    }

    public void write(final byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    public void write(final int b) throws IOException {
        write(new byte[] { (byte)b}, 0, 1);
    }

    private void flushBufferByChunk(ByteBuffer byteBuffer) throws IOException {
        int dataSizeInByte = byteBuffer.position();
        if (!fallbackTriggered && pMemManager.getStats().getRemainingSize() >= dataSizeInByte) {
            try {
                PMemPhysicalAddress id = writeInternal(byteBuffer);
                pMemManager.getStats().increaseSize(dataSizeInByte);
                pMemManager.getpMemMetaStore().putPhysicalAddress(logicalID, chunkID, id);
                chunkID++;
            } catch (RuntimeException re) {
                // TODO Log Warning
                fallbackTriggered = true;
                flushToDisk(byteBuffer);
            }
        } else {
            flushToDisk(byteBuffer);
        }
        byteBuffer.clear();
    }

    private void flushToDisk(ByteBuffer byteBuffer) throws IOException {
        if (outputStream == null) {
            //FIXME
            outputStream = new FileOutputStream("/tmp/helloworld");
            fallbackTriggered = true;
        }
        outputStream.write(byteBuffer.array());
    }

    public void close() throws IOException {
        // if remaining buffer has valid elements, write them to output stream
        if(remainingBuffer.position() > 0){
            flushBufferByChunk(remainingBuffer);
        }
        pMemManager.getpMemMetaStore().putMetaFooter(logicalID, new MetaData(fallbackTriggered, chunkID));

        closeInternal();
    }

    protected abstract PMemPhysicalAddress writeInternal(ByteBuffer byteBuffer);

    /**
     * Do some clean up work if needed.
     */
    protected abstract void closeInternal();

}
