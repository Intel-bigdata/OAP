package com.intel.oap.common.storage;

import java.util.Properties;

public class ChunkImplFactory {
    public static ChunkAPI getChunkImplByType(Properties properties) {
        ChunkType type = ChunkType.valueOf(properties.getProperty("oap.chunk.type"));
        switch (type) {
            case PMDK:
            case PLASMA:
            case MEMKIND:
            case SPDK:
                return new SPDKChunkImpl(properties);
            default:
                throw new RuntimeException("Unsupported Chunk Impl");
        }
    }
}
