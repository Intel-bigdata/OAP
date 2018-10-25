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

package org.apache.spark.util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A native library loader which used to load native library.
 */
public class NativeLibraryLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeLibraryLoader.class);

  private static Map<String, Boolean> loadRecord = new HashMap<>();

  private static String osName() {
    String os = System.getProperty("os.name").toLowerCase().replace(' ', '_');
    //TODO: improve this
    if (os.startsWith("linux")){
      return "linux";
    } else if (os.startsWith("mac")) {
      return "mac";
    } else {
      throw new UnsupportedOperationException("The platform: " + os + "is not supported.");
    }
  }

  private static String osArch() {
    String arch = System.getProperty("os.arch");
    if (arch.contains("64")) {
      arch = "64";
    } else {
      arch = "32";
    }
    return arch;
  }

  private static String resourceName(final String libname) {
    return "/" + osName() + "/" + osArch() + "/lib/" + System.mapLibraryName(libname);
  }

  /**
   * Loading the native library with given name. The name should not include the prefix and suffix.
   */
  public static synchronized void load(final String libname) {
    if (loadRecord.containsKey(libname) && loadRecord.get(libname)) {
      return;
    }

    try {
      System.loadLibrary(libname);
    } catch (UnsatisfiedLinkError e) {
      LOGGER.warn("Loading library: {} from system libraries failed, trying to load it from " +
        "package", libname);
      loadFromPackage(libname);
    }

    loadRecord.put(libname, true);
  }

  private static void loadFromPackage(final String libname) {
    InputStream is = null;
    OutputStream out = null;
    File tmpLib = null;
    try {
      is = NativeLibraryLoader.class.getResourceAsStream(resourceName(libname));
      if (is == null) {
        String errorMsg = "Unsupported OS/arch, cannot find " + resourceName(libname) + " or " +
          "load " + libname + " from system libraries. Please try building from source the jar" +
          " or providing " + libname + " in you system.";
        throw new RuntimeException(errorMsg);
      }

      tmpLib = File.createTempFile(libname, ".tmp", null);

      out = new FileOutputStream(tmpLib);
      byte[] buf = new byte[4096];
      while (true) {
        int read = is.read(buf);
        if (read == -1) {
          break;
        }
        out.write(buf, 0, read);
      }

      out.flush();
      out.close();
      out = null;

      System.load(tmpLib.getAbsolutePath());
    } catch (IOException | UnsatisfiedLinkError e) {
      throw new RuntimeException("Can't load library: " + libname + " from jar.", e);
    } finally {
      safeClose(is, "Close resource input stream failed. Ignore it.");
      safeClose(out, "Close tmp lib output stream failed. Ignore it.");

      if (tmpLib != null && !tmpLib.delete()) {
        LOGGER.warn("Delete tmp lib: {} failed, ignore it.", tmpLib.getAbsolutePath());
      }
    }
  }

  private static void safeClose(Closeable closeable, String warnMsg) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        LOGGER.warn(warnMsg, e.fillInStackTrace());
      }
    }
  }
}
