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

#define _GNU_SOURCE

#include <jni.h>

#include <stdio.h>
#include <errno.h>

#include <libvmemcache.h>

/*
 * Size of the smallest memory block in the cache.
 * For optimal copy performance and reduced metadata
 * footprint, this should be larger or equal to ECC
 * unit size of the underlying hardware.
 */
#define CACHE_EXTENT_SIZE 512

/* A helper macro to 'throw' a java exception. */
#define THROW(env, exception_name, message) \
{ \
    jclass ecls = (*env)->FindClass(env, exception_name); \
    if (ecls) { \
        (*env)->ThrowNew(env, ecls, message); \
        (*env)->DeleteLocalRef(env, ecls); \
    } \
}

typedef unsigned long long stat_t;

static VMEMcache *g_cache = NULL;

static void check(JNIEnv *env)
{
  if (g_cache == NULL)
  {
    THROW(env, "java/lang/RuntimeException", "Oops... you should call init first!");
  }
}

/*com.intel.oap.common.unsafe
 * Class:     com_intel_oap_common_unsafe_VMEMCacheJNI
 * Method:    init
 * Signature: (Ljava/lang/String;J)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_oap_common_unsafe_VMEMCacheJNI_init(
    JNIEnv *env, jclass cls, jstring path, jlong maxSize)
{
  const char* pathString = (*env)->GetStringUTFChars(env, path, NULL);
  g_cache = vmemcache_new();
  if (g_cache == NULL)
  {
    char msg[128];
    snprintf(msg, 128, "vmemcache_new failed: %s", vmemcache_errormsg());
    THROW(env, "java/lang/RuntimeException", msg);
    return -1;
  }
  vmemcache_set_extent_size(g_cache, CACHE_EXTENT_SIZE);
  vmemcache_set_size(g_cache, maxSize);
  if (vmemcache_add(g_cache, pathString) != 0) {
    char msg[128];
    snprintf(msg, 128, "vmemcache_add failed: %s(%s)", vmemcache_errormsg(), pathString);
    THROW(env, "java/lang/RuntimeException", msg);
    return -1;
  }
  (*env)->ReleaseStringUTFChars(env, path, pathString);

  return 0;
}

/*
 * Class:     com_intel_oap_common_unsafe_VMEMCacheJNI
 * Method:    putNative
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_oap_common_unsafe_VMEMCacheJNI_putNative(
    JNIEnv *env, jclass cls, jbyteArray keyArray, jobject keyBuffer, jint keyOff, jint keyLen,
    jlong valueBaseAddr, jint valueOff, jint valueLen)
{
  const char* key;
  const char* value;

  check(env);

  if (keyArray != NULL) {
    key = (const char*)(*env)->GetPrimitiveArrayCritical(env, keyArray, 0);
  } else {
    key = (const char*) (*env)->GetDirectBufferAddress(env, keyBuffer);
  }
  if (key == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Can't get key buffer");
    return -1;
  }
  key += keyOff;

  if(valueBaseAddr == NULL)
    return -1;

   value = (char*)valueBaseAddr;

  int put = vmemcache_put(g_cache, key, keyLen, value, valueLen);
  if (put) {
    //TODO: workaround to avoid throw exception when put the same key multi times.
    char msg[256];
    snprintf(msg, sizeof(msg), "vmemcache_put key:%s error:%s", key, vmemcache_errormsg());
    //THROW(env, "java/lang/RuntimeException", msg);
    fprintf(stderr, msg);
  }

  if (keyArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, keyArray, (void *)key, 0);
  }
   return put;
}

/*
 * Class:     com_intel_oap_common_unsafe_VMEMCacheJNI
 * Method:    put
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_oap_common_unsafe_VMEMCacheJNI_put(
    JNIEnv *env, jclass cls, jbyteArray keyArray, jobject keyBuffer, jint keyOff, jint keyLen,
    jbyteArray valueArray, jobject valueBuffer, jint valueOff, jint valueLen)
{
  const char* key;
  const char* value;

  check(env);

  if (keyArray != NULL) {
    key = (const char*)(*env)->GetPrimitiveArrayCritical(env, keyArray, 0);
  } else {
    key = (const char*) (*env)->GetDirectBufferAddress(env, keyBuffer);
  }
  if (key == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Can't get key buffer");
    return -1;
  }
  key += keyOff;

  if (valueArray != NULL) {
    value = (char*)(*env)->GetPrimitiveArrayCritical(env, valueArray, 0);
  } else {
    value = (char*) (*env)->GetDirectBufferAddress(env, valueBuffer);
  }
  if (value == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Can't get value buffer");
    return -1;
  }
  value += valueOff;

  int put = vmemcache_put(g_cache, key, keyLen, value, valueLen);
  if (put) {
    //TODO: workaround to avoid throw exception when put the same key multi times.
    char msg[256];
    snprintf(msg, sizeof(msg), "vmemcache_put key:%s error:%s", key, vmemcache_errormsg());
    //THROW(env, "java/lang/RuntimeException", msg);
    fprintf(stderr, msg);
  }

  if (keyArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, keyArray, (void *)key, 0);
  }
  if (valueArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, valueArray, (void *)value, 0);
  }

  return put;
}

/*
 * Class:     com_intel_oap_common_unsafe_VMEMCacheJNI
 * Method:    get
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_oap_common_unsafe_VMEMCacheJNI_get(
    JNIEnv *env, jclass cls, jbyteArray keyArray, jobject keyBuffer, jint keyOff, jint keyLen,
    jbyteArray valueArray, jobject valueBuffer, jint valueOff, jint maxValueLen)
{
  const char* key;
  void* value;
  size_t valueLen;

  check(env);

  if (keyArray != NULL) {
    key = (const char*)(*env)->GetPrimitiveArrayCritical(env, keyArray, 0);
  } else {
    key = (void*) (*env)->GetDirectBufferAddress(env, keyBuffer);
  }
  if (key == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Can't get key buffer");
    return -1;
  }
  key += keyOff;

  if (valueArray != NULL) {
    value = (char*)(*env)->GetPrimitiveArrayCritical(env, valueArray, 0);
  } else {
    value = (char*) (*env)->GetDirectBufferAddress(env, valueBuffer);
  }
  if (value == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Can't get value buffer");
    return -1;
  }
  value += valueOff;

  ssize_t ret = vmemcache_get(g_cache, key, keyLen, value, maxValueLen, 0, &valueLen);
  if (ret == -1 && errno != ENOENT) {
    char msg[128];
    snprintf(msg, 128, "vmemcache_get failed: %s", vmemcache_errormsg());
    THROW(env, "java/lang/RuntimeException", msg);
    return -1;
  }

  if (keyArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, keyArray, (void *)key, 0);
  }
  if (valueArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, valueArray, (void *)value, 0);
  }

  return ret;
}

/*
 * Class:     com_intel_oap_common_unsafe_VMEMCacheJNI
 * Method:    getNative
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_oap_common_unsafe_VMEMCacheJNI_getNative(
    JNIEnv *env, jclass cls, jbyteArray keyArray, jobject keyBuffer, jint keyOff, jint keyLen,
    jlong valueBaseObj, jint valueOff, jint maxValueLen)
{
  const char* key;
  void* value;
  size_t valueLen;

  check(env);

  if (keyArray != NULL) {
    key = (const char*)(*env)->GetPrimitiveArrayCritical(env, keyArray, 0);
  } else {
    key = (void*) (*env)->GetDirectBufferAddress(env, keyBuffer);
  }
  if (key == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Can't get key buffer");
    return -1;
  }
  key += keyOff;

  if(valueBaseObj == NULL)
    return -1;
  value = (char *)valueBaseObj;

  ssize_t ret = vmemcache_get(g_cache, key, keyLen, value, maxValueLen, valueOff, &valueLen);
  if (ret == -1 && errno != ENOENT) {
    char msg[128];
    snprintf(msg, 128, "vmemcache_get failed: %s", vmemcache_errormsg());
    THROW(env, "java/lang/RuntimeException", msg);
    return -1;
  }

   if (keyArray != NULL) {
     (*env)->ReleasePrimitiveArrayCritical(env, keyArray, (void *)key, 0);
   }

  return ret;
}

/*
 * Class:     com_intel_oap_common_unsafe_VMEMCacheJNI
 * Method:    evict
 * Signature: ([BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_oap_common_unsafe_VMEMCacheJNI_evict(
    JNIEnv *env, jclass cls, jbyteArray keyArray, jobject keyBuffer, jint keyOff, jint keyLen)
{
  const char* key;

  check(env);

  if (keyArray != NULL) {
    key = (const char*)(*env)->GetPrimitiveArrayCritical(env, keyArray, 0);
  } else {
    key = (const char*) (*env)->GetDirectBufferAddress(env, keyBuffer);
  }
  if (key == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Can't get key buffer");
    return -1;
  }
  key += keyOff;

  if (vmemcache_evict(g_cache, key, keyLen)) {
    char msg[128];
    snprintf(msg, 128, "vmemcache_evict failed: %s", vmemcache_errormsg());
    THROW(env, "java/lang/RuntimeException", msg);
    return -1;
  }

  if (keyArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, keyArray, (void *)key, 0);
  }

  return 0;
}

/*
 * Class:     com_intel_oap_common_unsafe_VMEMCacheJNI
 * Method:    exist
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_oap_common_unsafe_VMEMCacheJNI_exist(
    JNIEnv *env, jclass cls, jbyteArray keyArray, jobject keyBuffer, jint keyOff, jint keyLen)
{
  const char* key;
  size_t valueLen;

  check(env);

  if (keyArray != NULL) {
    key = (const char*)(*env)->GetPrimitiveArrayCritical(env, keyArray, 0);
  } else {
    key = (const char*) (*env)->GetDirectBufferAddress(env, keyBuffer);
  }
  if (key == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Can't get key buffer");
    return -1;
  }
  key += keyOff;

  int ret = vmemcache_exists(g_cache, key, keyLen, &valueLen);

  if (keyArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, keyArray, (void *)key, 0);
  }

  return (ret == 1) ? valueLen : 0;
}

/*
 * Class:     com_intel_oap_common_unsafe_VMEMCacheJNI
 * Method:    status
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_oap_common_unsafe_VMEMCacheJNI_status(
    JNIEnv *env, jclass cls, jlongArray statusArray)
{
  stat_t stat;
  int ret;
  int64_t * status;

  if (statusArray != NULL) {
    status = (int64_t*)(*env)->GetPrimitiveArrayCritical(env, statusArray, 0);
  }
  if(status == NULL) {
    return -1;
  }

  // evict count
  ret = vmemcache_get_stat(g_cache, VMEMCACHE_STAT_EVICT,
        			&stat, sizeof(stat));
  if(ret == -1) {
    status[0] = 0;
    char msg[128];
    snprintf(msg, 128, "vmemcache_status failed: %s", vmemcache_errormsg());
    THROW(env, "java/lang/RuntimeException", msg);
    return -1;
  } else {
    status[0] = (int64_t)stat;
  }

  // entries count
  ret = vmemcache_get_stat(g_cache, VMEMCACHE_STAT_ENTRIES,
        			&stat, sizeof(stat));
  if(ret == -1) {
    status[1] = 0;
    char msg[128];
    snprintf(msg, 128, "vmemcache_status failed: %s", vmemcache_errormsg());
    THROW(env, "java/lang/RuntimeException", msg);
    return -1;
  } else {
    status[1] = (int64_t)stat;
  }

  // pool size
  ret = vmemcache_get_stat(g_cache, VMEMCACHE_STAT_POOL_SIZE_USED,
        			&stat, sizeof(stat));
  if(ret == -1) {
    status[2] = 0;
    char msg[128];
    snprintf(msg, 128, "vmemcache_status failed: %s", vmemcache_errormsg());
    THROW(env, "java/lang/RuntimeException", msg);
    return -1;
  } else {
    status[2] = (int64_t)stat;
  }

  if (statusArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, statusArray, (void *)status, 0);
  }

  return 0;
}
