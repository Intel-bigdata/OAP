package org.apache.spark.jni.pmof;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class for JNI related operations. */
public class JniUtils {
  private static boolean isLoaded = false;
  private static Map<String,JniUtils> instanceMap = new HashMap<String,JniUtils>(); 
  private static final Logger LOG = LoggerFactory.getLogger(JniUtils.class);

  public static JniUtils getInstance(String libName) throws IOException {
    synchronized (JniUtils.class) {
      if (!instanceMap.containsKey(libName)) {
        try {
          instanceMap.put(libName, new JniUtils(libName));
        } catch (IllegalAccessException ex) {
          throw new IOException("IllegalAccess");
        }
      }
    }
    return instanceMap.get(libName);
  }

  private JniUtils(String libName) throws IOException, IllegalAccessException {
    try {
      loadLibraryFromJar(libName);
    } catch (IOException ex) {
      System.loadLibrary(libName);
    }
  }

  static void loadLibraryFromJar(String libName) throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      if (!isLoaded) {
        final String libraryToLoad = System.mapLibraryName(libName);
        final File libraryFile =
            moveFileFromJarToTemp(System.getProperty("java.io.tmpdir"), libraryToLoad);
        LOG.info("library path is " + libraryFile.getAbsolutePath());
        System.load(libraryFile.getAbsolutePath());
        isLoaded = true;
      }
    }
  }

  private static File moveFileFromJarToTemp(final String tmpDir, String libraryToLoad)
      throws IOException {
    final File temp = File.createTempFile(tmpDir, libraryToLoad);
    try (final InputStream is =
        JniUtils.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }
    return temp;
  }
}
