/*******************************************************************************
 * Copyright 2020 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

// Based on oneDAL Java com.intel.daal.utils.libUtils code

package org.apache.spark.ml.util;

import java.io.*;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LibLoader {
    private static final String LIBRARY_PATH_IN_JAR = "/lib";
    private final static String subDir = "MLlibDAL_" + new Date().getTime();

    private static final Logger logger = Logger.getLogger(LibLoader.class.getName());
    private static final Level logLevel = Level.INFO;

    /**
     * Get temp dir for exacting lib files
     * @return path of temp dir
     */
    public static String getTempSubDir() {
        String tempSubDirectory = System.getProperty("java.io.tmpdir") + "/" + subDir + LIBRARY_PATH_IN_JAR;
        return tempSubDirectory;
    }

    /**
     * Load MLlibDAL lib, it depends TBB libs that are loaded by oneDAL,
     * so this function should be called after oneDAL loadLibrary
     */
    public static synchronized void loadLibrary() throws IOException {
        // Load oneCCL libs in dependency order
        loadFromJar(subDir, "libpmi.so.1");
        loadFromJar(subDir, "libresizable_pmi.so.1");
        loadFromJar(subDir, "libfabric.so.1");
        loadFromJar(subDir, "libsockets-fi.so");
        loadFromJar(subDir, "libccl_atl_ofi.so");
        // Load for JNI
        loadFromJar(subDir, "libMLlibDAL.so");
    }

    /**
     * Load lib as resource
     *
     * @param path sub folder (in temporary folder) name
     * @param name library name
     */
    private static void loadFromJar(String path, String fullName) throws IOException {
        File fileOut = createTempFile(path, fullName);
        if (fileOut == null) {
            logger.log(logLevel, "DONE: Loading library as resource.");
            return;
        }

        InputStream streamIn = LibLoader.class.getResourceAsStream(LIBRARY_PATH_IN_JAR + "/" + fullName);
        if (streamIn == null) {
            throw new IOException("Error: No resource found.");
        }

        try (OutputStream streamOut = new FileOutputStream(fileOut)) {
            logger.log(logLevel, "Writing resource to temp file.");

            byte[] buffer = new byte[32768];
            while (true) {
                int read = streamIn.read(buffer);
                if (read < 0) {
                    break;
                }
                streamOut.write(buffer, 0, read);
            }

            streamOut.flush();
        } catch (IOException e) {
            throw new IOException("Error:  I/O error occurs from/to temp file.");
        } finally {
            streamIn.close();
        }

        logger.log(logLevel, fileOut.toString());
        System.load(fileOut.toString());
        logger.log(logLevel, "DONE: Loading library as resource.");
    }

    /**
     * Create temporary file
     *
     * @param name           library name
     * @param tempSubDirName sub folder (in temporary folder) name
     * @return temporary file handler. null if file exist already.
     */
    private static File createTempFile(String tempSubDirName, String name) throws IOException {
        File tempSubDirectory = new File(System.getProperty("java.io.tmpdir") + "/" + tempSubDirName + LIBRARY_PATH_IN_JAR);

        if (!tempSubDirectory.exists()) {
            tempSubDirectory.mkdirs();
            // Check existance again, don't use return bool of mkdirs
            if (!tempSubDirectory.exists()) {
                throw new IOException("Error: Can`t create folder for temp file.");
            }
        }

        String tempFileName = tempSubDirectory + "/" + name;
        File tempFile = new File(tempFileName);

        if (tempFile == null) {
            throw new IOException("Error: Can`t create temp file.");
        }

        if (tempFile.exists()) {
            return null;
        }

        return tempFile;
    }

}
/**
 * @}
 */
