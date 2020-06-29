package com.intel.oap.common.storage.benchmark;

import com.intel.oap.common.unsafe.PersistentMemoryPlatform;

import java.io.File;
import java.util.Arrays;

public class JmhTestUtil {
    public static void PMemInitialize() {
        File fd = new File("/mnt/pmem0");
        if (!fd.exists()) {
            fd.mkdirs();
        }
        PersistentMemoryPlatform.initialize("/mnt/pmem0", (long) 400 * 1024 * 1024 * 1024, 0);
    }
    public static byte[] PMemTestDataParation() {
        byte[] data = new byte[4*1024*1024];
        byte b = 97;
        Arrays.fill(data, b);
        return data;
    }
}
