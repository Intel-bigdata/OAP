package com.intel.oap.common.storage;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class ChunkInputStream extends FileInputStream {
    PMemDataStore pMemDataStore = null;

    public ChunkInputStream(String name) throws FileNotFoundException {
        super(name);
    }

}
