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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * A native library loader which used to load persistent memory native library.
 */
public class NativeLibraryLoader {
  private static final Logger logger = LoggerFactory.getLogger(NativeLibraryLoader.class);
  private static final String libname = "pmplatform";
  private static boolean loaded = false;

  private static String osName() {
    String os = System.getProperty("os.name").toLowerCase().replace(' ', '_');
    if (os.startsWith("win")){
      return "win";
    } else if (os.startsWith("mac")) {
      return "darwin";
    } else {
      return os;
    }
  }

  private static String osArch() {
    return System.getProperty("os.arch");
  }

  private static String resourceName() {
    return "/" + osName() + "/" + osArch() + "/lib/" + System.mapLibraryName(libname);
  }

  public static synchronized void load() {
    load(null);
  }

  public static synchronized void load(final File tempFolder) {
    if (loaded) {
      return;
    }

    try {
      System.loadLibrary(libname);
    } catch (UnsatisfiedLinkError e) {
      logger.warn("Loading library: {} from system libraries failed, trying to load it from " +
        "jar", libname);
      InputStream is = null;
      OutputStream out = null;
      File tmpLib = null;
      try {
        is = NativeLibraryLoader.class.getResourceAsStream(resourceName());
        if (is == null) {
          String errorMsg = "Unsupported OS/arch, cannot find " + resourceName() + " or load " +
            libname + " from system libraries. Please try building from source the jar or " +
            "providing " + libname + " in you system.";
          logger.error(errorMsg);
          throw new RuntimeException(errorMsg);
        }

        tmpLib = File.createTempFile(libname, ".tmp", tempFolder);

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
        loaded = true;
      } catch (IOException ioe) {
        logger.error("Can't load library: {} from jar.", libname);
        throw new RuntimeException(ioe);
      } finally {
        if (is != null) {
          closeWithWarn(is, "Close resource input stream failed. Ignore it.");
        }

        if (out != null) {
          closeWithWarn(out, "Close tmp lib output stream failed. Ignore it.");
        }

        if (tmpLib != null && !tmpLib.delete()) {
          logger.warn("Delete tmp lib: {} failed, ignore it.", tmpLib.getAbsolutePath());
        }
      }
    }
  }

  private static void closeWithWarn(Closeable closeable, String warnMsg) {
    try {
      closeable.close();
    } catch (IOException e) {
      logger.warn(warnMsg);
    }
  }
}
