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

#include <memkind.h>
#include <cstring>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cassert>
#include <stdexcept>
#include "com_intel_oap_common_unsafe_PersistentMemoryPlatform.h"

using memkind = struct memkind;
memkind *pmemkind = NULL;
struct memkind_config *pmemkind_config;

// copied form openjdk: http://hg.openjdk.java.net/jdk8/jdk8/hotspot/file/87ee5ee27509/src/share/vm/prims/unsafe.cpp
inline void* addr_from_java(jlong addr) {
  // This assert fails in a variety of ways on 32-bit systems.
  // It is impossible to predict whether native code that converts
  // pointers to longs will sign-extend or zero-extend the addresses.
  //assert(addr == (uintptr_t)addr, "must not be odd high bits");
  return (void*)(uintptr_t)addr;
}

inline jlong addr_to_java(void* p) {
  //assert(p == (void*)(uintptr_t)p, "must not be odd high bits");
  assert(p == (void*)(uintptr_t)p);
  return (uintptr_t)p;
}

inline void check(JNIEnv *env) {
  if (NULL == pmemkind) {
    jclass exceptionCls = env->FindClass("java/lang/RuntimeException");
    env->ThrowNew(exceptionCls, "Persistent memory should be initialized first!");
  }
}

JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PersistentMemoryPlatform_initializeKmem
  (JNIEnv *, jclass) {
  pmemkind = MEMKIND_DAX_KMEM;
}

JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PersistentMemoryPlatform_initializeNative
  (JNIEnv *env, jclass clazz, jstring path, jlong size, jint pattern) {
  // str should not be null, we should checked in java code
  const char* str = env->GetStringUTFChars(path, NULL);
  size_t sz = (size_t)size;
  // Initialize persistent memory
  int pattern_c = (int)pattern;
  int error;

  if (pattern_c == 0) {
    error = memkind_create_pmem(str, sz, &pmemkind);
  } else {
    pmemkind_config = memkind_config_new();
    memkind_config_set_path(pmemkind_config, str);
    memkind_config_set_size(pmemkind_config, sz);
    memkind_config_set_memory_usage_policy(pmemkind_config, MEMKIND_MEM_USAGE_POLICY_CONSERVATIVE);
    error = memkind_create_pmem_with_config(pmemkind_config, &pmemkind);
  }

  if (error) {
    jclass exceptionCls = env->FindClass("java/lang/Exception");
    env->ThrowNew(exceptionCls,
      "Persistent initialize failed! Please check the path permission.");
  }

  env->ReleaseStringUTFChars(path, str);
}

JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PersistentMemoryPlatform_setNUMANode
  (JNIEnv *env, jclass, jstring dax_node, jstring regular_node) {
  const char* dax_node_str = env->GetStringUTFChars(dax_node, NULL);
  const char* regular_node_str = env->GetStringUTFChars(regular_node, NULL);

  setenv("MEMKIND_REGULAR_NODES", regular_node_str, 1);
  setenv("MEMKIND_DAX_KMEM_NODES", dax_node_str, 1);
  env->ReleaseStringUTFChars(regular_node, regular_node_str);
  env->ReleaseStringUTFChars(dax_node, dax_node_str);
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_common_unsafe_PersistentMemoryPlatform_allocateVolatileMemory
  (JNIEnv *env, jclass clazz, jlong size) {
  check(env);

  size_t sz = (size_t)size;
  void *p = memkind_malloc(pmemkind, sz);
  if (p == NULL) {
    jclass errorCls = env->FindClass("java/lang/OutOfMemoryError");
    std::string errorMsg;
    errorMsg.append("Don't have enough memory, please consider decrease the persistent ");
    errorMsg.append("memory usable ratio. The requested size: ");
    errorMsg.append(std::to_string(sz));
    env->ThrowNew(errorCls, errorMsg.c_str());
  }

  return addr_to_java(p);
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_common_unsafe_PersistentMemoryPlatform_getOccupiedSize
  (JNIEnv *env, jclass clazz, jlong address) {
  check(env);
  void *p = addr_from_java(address);
  return memkind_malloc_usable_size(pmemkind, p);
}

JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PersistentMemoryPlatform_freeMemory
  (JNIEnv *env, jclass clazz, jlong address) {
  check(env);
  memkind_free(pmemkind, addr_from_java(address));
}

JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PersistentMemoryPlatform_copyMemory
  (JNIEnv *env, jclass clazz, jlong destination, jlong source, jlong size) {
  size_t sz = (size_t)size;
  void *dest = addr_from_java(destination);
  void *src = addr_from_java(source);
  std::memcpy(dest, src, sz);
}
