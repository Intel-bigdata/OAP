/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.common.unsafe;

import com.intel.oap.common.util.NativeLibraryLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PMemBlockPlatform {

    private static final Logger LOG = LoggerFactory.getLogger(PMemBlockPlatform.class);
    private static final String LIBNAME = "pmblkplatform";
    private static boolean isAvailable = true;

    static {
        NativeLibraryLoader.load(LIBNAME);
    }

    public static boolean isPMemBlkAvailable() {
        try {
            NativeLibraryLoader.load(LIBNAME);
        } catch (Exception e) {
            LOG.error("Failed to load " + LIBNAME);
            isAvailable = false;
        }
        return isAvailable;
    }

    public static native void create(String path, long elementSize, long poolSize);

    public static native void write(byte[] buffer, int index);

    public static native void read(byte[] buffer, int index);

    public static native void clear(int index);

    public static native void close();

    public static native int getBlockNum();

}
