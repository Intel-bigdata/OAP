package com.intel.oap.common.unsafe;

import com.intel.oap.common.util.NativeLibraryLoader;

public class PMemMemoryMapper {

    private static final String LIBNAME = "pmemmemorymapper";
    static {
        NativeLibraryLoader.load(LIBNAME);
    }

    /**
     * Create a file and memory map it.
     * pmem_map_file(path, PMEM_LEN, PMEM_FILE_CREATE,0666, &mapped_len, &is_pmem)
     *
     * @param fileName
     * @param fileLength
     * @return pmem address
     */
    public static native long pmemMapFile(String fileName, long fileLength);

    public static native void pmemMemcpy(long pmemAddress, byte[] src, long length);

    /**
     * Flush at final
     */
    public static native void pmemDrain();

    /**
     * Unmap the file
     *
     * @param address pmem address
     * @param length  mapped length
     */
    public static native void pmemUnmap(long address, long length);

}
