/*
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
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <dlfcn.h>
#include <jni.h>
#include <stddef.h>
#include <unistd.h>
#include <assert.h>

#include "utils.h"

#include "com_intel_compression_core_IntelCompressionCodecNative.h"

#define ZSTD_LIBRARY "libzstd.so"

static size_t (*dlsym_ZSTD_compress)(void* dst, size_t dstCapacity,
    const void* src, size_t srcSize,
    int compressionLevel);
static size_t (*dlsym_ZSTD_decompress)(void* dst, size_t dstCapacity,
    const void* src, size_t compressedSize);
static char* (*dlsym_ZSTD_versionString)();

static int compressed_level = 3;
static int symbol_loaded = 0;

static void zstd_load_symbol(JNIEnv *env)
{
  if (!symbol_loaded)
  {
    symbol_loaded = 1;
    // Load libzstd.so
    void *lib = dlopen(ZSTD_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
    if (!lib) {
      char msg[128];
      snprintf(msg, 128, "%s (%s)!", "Cannot load "ZSTD_LIBRARY , dlerror());
      THROW(env, "java/lang/UnsatisfiedLinkError1", msg);
      return;
    }

    dlerror(); // Clear any existing error
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_compress, env, lib, "ZSTD_compress");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_decompress, env, lib, "ZSTD_decompress");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_versionString, env, lib, "ZSTD_versionString");

    fprintf(stderr, "zstd_load_symbol: load the symbol successfully\n");
  }
}

/*
 * Class:     com_intel_compression_core_IntelCompressionCodecNative
 * Method:    zstd_compress_init
 * Signature: (I)V
 */
JNIEXPORT void JNICALL
Java_com_intel_compression_core_IntelCompressionCodecNative_zstd_1compress_1init
    (JNIEnv *env, jclass cls, int level) {
  compressed_level = level;
  if(compressed_level < 1||compressed_level > 22) {
    fprintf(stderr, "ZSTD_init, invalid compresse level %d, set to default level:3\n",
      compressed_level);
    compressed_level = 3;
  }

  fprintf(stderr, "ZSTD_compress_init: set to compress level:%d\n", compressed_level);

  zstd_load_symbol(env);

  return;
}

/*
 * Class:     com_intel_compression_core_IntelCompressionCodecNative
 * Method:    zstd_decompress_init
 * Signature: (I)V
 */
JNIEXPORT void JNICALL
Java_com_intel_compression_core_IntelCompressionCodecNative_zstd_1decompress_1init
    (JNIEnv *env, jclass cls, int arg) {

  zstd_load_symbol(env);

  return;
}

/*
 * Class:     com_intel_compression_core_IntelCompressionCodecNative
 * Method:    zstd_compress
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_compression_core_IntelCompressionCodecNative_zstd_1compress
    (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen,
    jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen) {

  const unsigned char* in;
  unsigned char* out;

  if (srcArray != NULL) {
    in = (const unsigned char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (const unsigned char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  }
  if (in == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Out of memory");
    return 0;
  }

  if (destArray != NULL) {
    out = (unsigned char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (unsigned char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  }
  if (out == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Out of memory");
    return 0;
  }

  size_t compressed = dlsym_ZSTD_compress(out + destOff, (size_t)maxDestLen,
      in + srcOff, (size_t)srcLen, compressed_level);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, (void *)in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;
}

/*
 * Class:     com_intel_compression_core_IntelCompressionCodecNative
 * Method:    zstd_decompress
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_compression_core_IntelCompressionCodecNative_zstd_1decompress
    (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen,
    jbyteArray destArray, jobject destBuffer, jint destOff, jint destLen) {

  const unsigned char* in;
  unsigned char* out;

  if (srcArray != NULL) {
    in = (const unsigned char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (const unsigned char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  }
  if (in == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Out of memory");
    return 0;
  }

  if (destArray != NULL) {
    out = (unsigned char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (unsigned char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  }
  if (out == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Out of memory");
    return 0;
  }

  size_t decompressed = dlsym_ZSTD_decompress(out + destOff, destLen,
      in + srcOff, srcLen);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, (void *)in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return decompressed;
}

/*
 * Class:     com_intel_compression_core_IntelCompressionCodecNative
 * Method:    zstd_getLibraryName
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
Java_com_intel_compression_core_IntelCompressionCodecNative_zstd_1getLibraryName
    (JNIEnv *env, jclass class) {
  char *version = dlsym_ZSTD_versionString();
  return (*env)->NewStringUTF(env, version);
}
