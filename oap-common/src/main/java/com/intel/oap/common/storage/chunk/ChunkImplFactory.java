package com.intel.oap.common.storage.chunk;

import com.intel.oap.common.storage.ChunkManager;
import com.intel.oap.common.storage.meta.ChunkType;
import com.intel.oap.common.storage.FileChunkImpl;

import java.util.Properties;

public class ChunkImplFactory {
    public static ChunkManager getChunkImplByType(Properties properties) {
        ChunkType type = ChunkType.valueOf(properties.getProperty("oap.chunk.type"));
        switch (type) {
            case PMDK:
            case PLASMA:
            case MEMKIND:
            case SPDK:
                return new FileChunkImpl(properties);
            default:
                throw new RuntimeException("Unsupported Chunk Impl");
        }
    }
}
