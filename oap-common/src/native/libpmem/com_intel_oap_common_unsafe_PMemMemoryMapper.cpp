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

#include <libpmem.h>
#include <cstring>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cassert>
#include <stdexcept>
#include "com_intel_oap_common_unsafe_PMemMemoryMapper.h"

// copied form openjdk: http://hg.openjdk.java.net/jdk8/jdk8/hotspot/file/87ee5ee27509/src/share/vm/prims/unsafe.cpp
inline void* addr_from_java(jlong addr) {
  // This assert fails in a variety of ways on 32-bit systems.
  // It is impossible to predict whether native code that converts
  // pointers to longs will sign-extend or zero-extend the addresses.
  //assert(addr == (uintptr_t)addr, "must not be odd high bits");
  return (void*)(uintptr_t)addr;
}

inline jlong addr_to_java(void* p) {
  assert(p == (void*)(uintptr_t)p);
  return (uintptr_t)p;
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_common_unsafe_PMemMemoryMapper_pmemMapFile
  (JNIEnv *env, jclass clazz, jstring fileName, jlong fileLength) {
    const char* path = NULL;
    void* pmemaddr = NULL;
    size_t mapped_len = 0;
    int is_pmem = 0;

    path = env->GetStringUTFChars(fileName, NULL);
    pmemaddr = pmem_map_file(path, fileLength,
                             PMEM_FILE_CREATE|PMEM_FILE_EXCL,
                             0666, &mapped_len, &is_pmem);

    env->ReleaseStringUTFChars(fileName, path);
    return addr_to_java(pmemaddr);
}

JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PMemMemoryMapper_pmemMemcpy
  (JNIEnv *env, jclass clazz, jlong pmemAddress, jbyteArray src, jlong length) {
    jbyte* srcBuf = env->GetByteArrayElements(src, 0);
    pmem_memcpy_nodrain(addr_from_java(pmemAddress), srcBuf, length);
    env->ReleaseByteArrayElements(src, srcBuf, 0);
}

JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PMemMemoryMapper_pmemDrain
  (JNIEnv *env, jclass clazz) {
    pmem_drain();
}

JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PMemMemoryMapper_pmemUnmap
  (JNIEnv *env, jclass clazz, jlong pmemAddress, jlong length) {
    pmem_unmap(addr_from_java(pmemAddress), length);
}

