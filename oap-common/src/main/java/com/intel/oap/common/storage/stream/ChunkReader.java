package com.intel.oap.common.storage.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public abstract class ChunkReader {
    private static final Logger logger = LoggerFactory.getLogger(ChunkReader.class);
    protected PMemManager pMemManager;
    protected byte[] logicalID;
    // current position determined by chunkID and offsetInChunk
    private int chunkID = 0;
    private int offsetInChunk = 0;
    private ByteBuffer remainingBuffer;
    protected MetaData metaData;
    private FileChannel fileChannel = null;

    public ChunkReader(byte[] logicalID, PMemManager pMemManager){
        this.logicalID = logicalID;
        this.pMemManager = pMemManager;
        this.remainingBuffer = ByteBuffer.wrap(new byte[pMemManager.getChunkSize()]);
        remainingBuffer.flip();
        this.metaData = pMemManager.getpMemMetaStore().getMetaFooter(logicalID);
    }

    /**
     * load <chunkSize> bytes from current position
     * @return data size read from PMem + Disk
     * @throws IOException
     */
    private int loadData() throws IOException {
        int totalRequest = remainingBuffer.capacity();
        int size = 0;
        while(size < totalRequest) {
            // if there are still pmem trunks, read from chunk
            while(chunkID < metaData.getTotalChunk()) {
                PMemPhysicalAddress id = pMemManager.getpMemMetaStore().getPhysicalAddressByID(logicalID, chunkID);
                // read expected data from PMem chunk as well as origin offset of PMem chunk
                byte[] data= readFromPMem(id, offsetInChunk, totalRequest - size);
                remainingBuffer.put(data);
                int originChunkOffset = getOffsetOfChunk(id);
                // update position(chunkID, offsetInChunk)
                int readedDataSize = data.length;
                int offset = offsetInChunk + readedDataSize;
                if(offset < originChunkOffset) {
                    offsetInChunk += readedDataSize;
                } else {
                    offsetInChunk = 0;
                    chunkID++;
                }
                size += readedDataSize;
                if(size == totalRequest)
                    return size;
            }
            // PMem chunks are all read and no disk file
            if(!metaData.isHasDiskData()) {
                return size == 0? -1: size;
            }
            // read from disk file
            if(fileChannel == null) {
                fileChannel = FileChannel.open(
                        new File(new String(logicalID)).toPath(), StandardOpenOption.READ);
                logger.debug("Read data from real file.");
            }
            long startPosition = fileChannel.position();
            if(startPosition < fileChannel.size()) {
                int remainingRequest = (int) Math.min(totalRequest - size, fileChannel.size() - startPosition);
                ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[remainingRequest]);
                // append bytes read from file to remainingBuffer
                fileChannel.read(byteBuffer);
                byte[] fromFileBytes = new byte[remainingRequest];
                byteBuffer.flip();
                byteBuffer.get(fromFileBytes, 0, remainingRequest);
                remainingBuffer.put(fromFileBytes);
                size += remainingRequest;
                // file finished read
                if (fileChannel.position() == fileChannel.size())
                    return size;
            }
            // Stream is already finished read
            else {
                return -1;
            }
        }
        return size;
    }

    private boolean refill() throws IOException {
        if(!remainingBuffer.hasRemaining()) {
            remainingBuffer.clear();
            int nRead = 0;
            while (nRead == 0) {
                nRead = loadData();
            }
            remainingBuffer.flip();
            if (nRead < 0) {
                return false;
            }
        }
        return true;
    }

    public synchronized int read() throws IOException {
        if (!refill()) {
            return -1;
        }
        return remainingBuffer.get() & 0xFF;
    }

    public synchronized int read(byte[] b, int offset, int len) throws IOException {
        if (offset < 0 || len < 0 || offset + len < 0 || offset + len > b.length) {
            throw new IndexOutOfBoundsException();
        }
        if (!refill()) {
            return -1;
        }
        len = Math.min(len, remainingBuffer.remaining());
        remainingBuffer.get(b, offset, len);
        return len;
    }

    public synchronized int readFully(byte[] b, int off, int len) throws IOException {
        if (len < 0)
            throw new IndexOutOfBoundsException();
        int n = 0;
        while (n < len) {
            int count = read(b, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
        return n;
    }

    public synchronized int available() throws IOException {
        return remainingBuffer.remaining();
    }


    public synchronized long skip(long n) throws IOException {
        if (n <= 0L) {
            return 0L;
        }
        if (remainingBuffer.remaining() >= n) {
            // The buffered content is enough to skip
            remainingBuffer.position(remainingBuffer.position() + (int) n);
            return n;
        }
        long skippedFromBuffer = remainingBuffer.remaining();
        long toSkipFromPMemStream = n - skippedFromBuffer;
        // Discard everything we have read in the buffer.
        remainingBuffer.clear();
        remainingBuffer.flip();
        return skippedFromBuffer + skipFromPMemStream(toSkipFromPMemStream);
    }

    private long skipFromPMemStream(long n) throws IOException {
        long totalExpectedSkip = n;
        while(chunkID < metaData.getTotalChunk()) {
            int offset = getOffsetOfChunk(pMemManager.getpMemMetaStore().getPhysicalAddressByID(logicalID, chunkID));
            int canSkip = offset - offsetInChunk;
            if(canSkip > n) {
                offsetInChunk += n;
                return totalExpectedSkip;
            }
            if(canSkip < n) {
                offsetInChunk = 0;
                chunkID++;
            }
            n -= canSkip;
        }
        if(metaData.isHasDiskData() == false) {
            return totalExpectedSkip - n;
        }
        if(fileChannel == null) {
            fileChannel = FileChannel.open(new File(new String(logicalID)).toPath(), StandardOpenOption.READ);
        }
        long currentFilePosition = fileChannel.position();
        long size = fileChannel.size();
        if (n > size - currentFilePosition) {
            fileChannel.position(size);
            return totalExpectedSkip - n + size - currentFilePosition;
        } else {
            fileChannel.position(currentFilePosition + n);
            return totalExpectedSkip;
        }
    }

    public synchronized void close() throws IOException {
        fileChannel.close();
    }

    @Override
    protected void finalize() throws IOException {
        close();
    }

    public int read(byte b[]) throws IOException {
        return readFully(b, 0, b.length);
    }

    protected abstract byte[] readFromPMem(PMemPhysicalAddress id, int offsetInChunk, int remainingRequest);

    protected abstract void freeFromPMem();

    /**
     * This function should can de got directly from interface PMemPhysicalAddress, but
     * currently it's blank since hard to unify semantics for PMemBlk and MemKind, so use
     * this method as a temp workaround.
     * @param pMemPhysicalAddress
     * @return
     */
    protected abstract int getOffsetOfChunk(PMemPhysicalAddress pMemPhysicalAddress);
}
