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

#include <libpmemblk.h>
#include <cstring>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cassert>
#include <stdexcept>
#include "com_intel_oap_common_unsafe_PMemBlockPlatform.h"

PMEMblkpool *pbp = NULL;

/*
 * Class:     com_intel_oap_common_unsafe_PMemBlockPlatform
 * Method:    create
 * Signature: (Ljava/lang/String;JJ)V
 */
JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PMemBlockPlatform_create
  (JNIEnv *env, jclass clazz, jstring path, jlong element_size, jlong pool_size) {

  const char* s_path = env->GetStringUTFChars(path, NULL);
  size_t sz_element_size = (size_t) element_size;
  size_t sz_pool_size = (size_t) pool_size;

  pbp = pmemblk_create(s_path, sz_element_size, sz_pool_size, 0666);
  if (pbp == NULL)
    pbp = pmemblk_open(s_path, element_size);

  if (pbp == NULL) {
    jclass exceptionCls = env->FindClass("java/lang/RuntimeException");
    std::string errorMsg;
    errorMsg.append("Fail to create pmem block pool on ");
    errorMsg.append(s_path);
    env->ThrowNew(exceptionCls, errorMsg.c_str());
  }
  env->ReleaseStringUTFChars(path, s_path);
}

/*
 * Class:     com_intel_oap_common_unsafe_PMemBlockPlatform
 * Method:    write
 * Signature: (Ljava/nio/ByteBuffer;I)V
 */
JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PMemBlockPlatform_write
  (JNIEnv *env, jclass clazz, jbyteArray jbuf, jint jindex) {

  jbyte* buf = env->GetByteArrayElements(jbuf, 0);
  int index = (int) jindex;

  if(pmemblk_write(pbp, buf, index) < 0) {
    jclass exceptionCls = env->FindClass("java/lang/RuntimeException");
    std::string errorMsg;
    errorMsg.append("Fail to write pmem block on ");
    errorMsg.append(std::to_string(index));
    env->ThrowNew(exceptionCls, errorMsg.c_str());
  }
  env->ReleaseByteArrayElements(jbuf, buf, 0);
}

/*
 * Class:     com_intel_oap_common_unsafe_PMemBlockPlatform
 * Method:    read
 * Signature: (Ljava/nio/ByteBuffer;I)V
 */
JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PMemBlockPlatform_read
  (JNIEnv *env, jclass clazz, jbyteArray jbuf, jint jindex) {

  jbyte* buf = env->GetByteArrayElements(jbuf, 0);
  int index = (int) jindex;

  if(pmemblk_read(pbp, buf, index) < 0) {
    jclass exceptionCls = env->FindClass("java/lang/RuntimeException");
    std::string errorMsg;
    errorMsg.append("Fail to read pmem block on ");
    errorMsg.append(std::to_string(index));
    env->ThrowNew(exceptionCls, errorMsg.c_str());
  }
  env->ReleaseByteArrayElements(jbuf, buf, 0);
}

/*
 * Class:     com_intel_oap_common_unsafe_PMemBlockPlatform
 * Method:    clear
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PMemBlockPlatform_clear
  (JNIEnv *env, jclass clazz, jint jindex) {

  int index = (int) jindex;

  if (pmemblk_set_zero(pbp, index) < 0) {
    jclass exceptionCls = env->FindClass("java/lang/RuntimeException");
    std::string errorMsg;
    errorMsg.append("Fail to set zero on ");
    errorMsg.append(std::to_string(index));
    env->ThrowNew(exceptionCls, errorMsg.c_str());
  }
}

/*
 * Class:     com_intel_oap_common_unsafe_PMemBlockPlatform
 * Method:    close
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_intel_oap_common_unsafe_PMemBlockPlatform_close
  (JNIEnv *env, jclass clazz) {

  pmemblk_close(pbp);
}

/*
 * Class:     com_intel_oap_common_unsafe_PMemBlockPlatform
 * Method:    getBlockNum
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_com_intel_oap_common_unsafe_PMemBlockPlatform_getBlockNum
  (JNIEnv *env, jclass clazz) {

  return pmemblk_nblock(pbp);
}
