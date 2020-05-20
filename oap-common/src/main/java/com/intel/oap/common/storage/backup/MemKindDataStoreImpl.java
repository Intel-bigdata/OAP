package com.intel.oap.common.storage.backup;

import com.intel.oap.common.storage.stream.MemoryStats;

import java.util.Iterator;

public class MemKindDataStoreImpl {
    //get this value from conf
    private final long chunkSize = 100;

    public MemKindDataStoreImpl(MemoryStats stats){
        super();
//        this.chunkAPI = new MemKindChunkAPIImpl();
//        this.chunkManager = new MemKindChunkManagerImpl();
//        this.chunkMap = new ConcurrentHashMap<>();
//        this.streamMetaMap = new ConcurrentHashMap<>();
//        this.stats = stats;
    }

    public Iterator<Chunk> getOutputChunkIterator(byte[] logicalID) {
//        return new Iterator<Chunk>() {
//            long totalSize = 0;
//            boolean isComplete = false;
//            int currentTrunkID = 0;
//            @Override
//            public boolean hasNext() {
//                throw new RuntimeException("Unsupported operation");
//            }
//            @Override
//            public Chunk next() {
//                byte[] physicalID = getPhysicalIDbyLogicalID(logicalID, currentTrunkID);
//                Chunk chunk = chunkAPI.allocate(physicalID, chunkSize);
//                currentTrunkID += 1;
//                totalSize += chunkSize;
//                // if allocate fails, return a file chunk
//                if(chunk == null) {
//                    totalSize -= chunkSize;
//                    currentTrunkID -= 1;
//                    chunk = new FileChunk(physicalID);
//                    isComplete = true;
//                }
//                chunkManager.putChunk(physicalID, chunk);
//                return chunk;
//            }
//            public void close() {
//                stats.increaseSize(totalSize);
//                streamMetaMap.put(logicalID, new StreamMeta(logicalID, totalSize, currentTrunkID, isComplete));
//            }
//        };
        return null;
    }

    public Iterator<Chunk> getInputChunkIterator(byte[] logicalID) {
//        return new Iterator<Chunk>() {
//            int currentTrunkID = -1;
//            StreamMeta streamMeta = streamMetaMap.get(logicalID);
//            @Override
//            public boolean hasNext() {
//                if(currentTrunkID + 1 < streamMeta.numOfChunks && chunkMap.get(getPhysicalIDbyLogicalID(logicalID, currentTrunkID + 1)) != null)
//                    return true;
//                else if(currentTrunkID + 1 == streamMeta.numOfChunks && streamMeta.isComplete == false)
//                    return true;
//                else
//                    return false;
//            }
//
//            @Override
//            public Chunk next() {
//                currentTrunkID += 1;
//                return chunkMap.get(getPhysicalIDbyLogicalID(logicalID, currentTrunkID));
//            }
//        };
        return null;
    }

    public byte[] getPhysicalIDbyLogicalID(byte[] logicalID, long currentTrunkID) {
        return (logicalID + "_" + currentTrunkID).getBytes();
    }

    //free ?
    public void freeChunks(byte[] logicalID){
//        Iterator<Chunk> chunks = getInputChunkIterator(logicalID);
//        Chunk currentChunk;
//        int currentChunkNo = 0;
//        while(chunks.hasNext())
//        {
//            currentChunk = chunks.next();
//            chunkManager.remove(getPhysicalIDbyLogicalID(logicalID, currentChunkNo));
//            currentChunk.free();
//            currentChunkNo += 1;
//        }
//        //remove this logical metadata from streamMeta
//        streamMetaMap.remove(logicalID);
    }
}
