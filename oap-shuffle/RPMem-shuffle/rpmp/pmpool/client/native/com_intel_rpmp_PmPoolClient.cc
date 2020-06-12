/*
 * Filename:
 * /mnt/spark-pmof/tool/rpmp/pmpool/client/native/PmPoolClientNative.cc Path:
 * /mnt/spark-pmof/tool/rpmp/pmpool/client/native Created Date: Monday, February
 * 24th 2020, 9:23:22 pm Author: root
 *
 * Copyright (c) 2020 Intel
 */
#include <memory>

#include "pmpool/client/PmPoolClient.h"
#include "pmpool/client/native/com_intel_rpmp_PmPoolClient.h"

JNIEXPORT jlong JNICALL Java_com_intel_rpmp_PmPoolClient_newPmPoolClient_1(
    JNIEnv *env, jobject obj, jstring address, jstring port) {
  const char *remote_address = env->GetStringUTFChars(address, 0);
  const char *remote_port = env->GetStringUTFChars(port, 0);

  PmPoolClient *client = new PmPoolClient(remote_address, remote_port);
  client->begin_tx();
  client->init();
  client->end_tx();

  env->ReleaseStringUTFChars(address, remote_address);
  env->ReleaseStringUTFChars(port, remote_port);

  return reinterpret_cast<uint64_t>(client);
}

JNIEXPORT jlong JNICALL Java_com_intel_rpmp_PmPoolClient_alloc_1(
    JNIEnv *env, jobject obj, jlong size, jlong objectId) {
  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->begin_tx();
  uint64_t address = client->alloc(size);
  client->end_tx();
  return address;
}

JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_free_1(JNIEnv *env,
                                                               jobject obj,
                                                               jlong address,
                                                               jlong objectId) {
  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->begin_tx();
  int success = client->free(address);
  client->end_tx();
  return success;
}

JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_write_1(
    JNIEnv *env, jobject obj, jlong address, jstring data, jlong size,
    jlong objectId) {
  const char *raw_data = env->GetStringUTFChars(data, 0);

  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->begin_tx();
  int success = client->write(address, raw_data, size);
  client->end_tx();

  env->ReleaseStringUTFChars(data, raw_data);

  return success;
}
JNIEXPORT jlong JNICALL
Java_com_intel_rpmp_PmPoolClient_alloc_1and_1write_1__Ljava_lang_String_2JJ(
    JNIEnv *env, jobject obj, jstring data, jlong size, jlong objectId) {
  const char *raw_data = env->GetStringUTFChars(data, 0);

  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->begin_tx();
  uint64_t address = client->write(raw_data, size);
  client->end_tx();

  env->ReleaseStringUTFChars(data, raw_data);
  return address;
}

JNIEXPORT jlong JNICALL
Java_com_intel_rpmp_PmPoolClient_alloc_1and_1write_1__Ljava_nio_ByteBuffer_2JJ(
    JNIEnv *env, jobject obj, jobject data, jlong size, jlong objectId) {
  char *raw_data = static_cast<char *>((*env).GetDirectBufferAddress(data));
  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->begin_tx();
  uint64_t address = client->write(raw_data, size);
  client->end_tx();
}

JNIEXPORT jlong JNICALL
Java_com_intel_rpmp_PmPoolClient_put(JNIEnv *env, jobject obj, jstring key,
                                     jobject data, jlong size, jlong objectId) {
  char *raw_data = static_cast<char *>((*env).GetDirectBufferAddress(data));
  const char *raw_key = env->GetStringUTFChars(key, 0);
  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->begin_tx();
  auto address = client->put(raw_key, raw_data, size);
  client->end_tx();
  env->ReleaseStringUTFChars(key, raw_key);
  return address;
}

JNIEXPORT jlongArray JNICALL Java_com_intel_rpmp_PmPoolClient_getMeta(
    JNIEnv *env, jobject obj, jstring key, jlong objectId) {
  const char *raw_key = env->GetStringUTFChars(key, 0);
  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->begin_tx();
  auto bml = client->get(raw_key);
  client->end_tx();
  env->ReleaseStringUTFChars(key, raw_key);
  int longCArraySize = bml.size() * 2;
  jlongArray longJavaArray = env->NewLongArray(longCArraySize);
  uint64_t *longCArray =
      static_cast<uint64_t *>(std::malloc(longCArraySize * sizeof(uint64_t)));
  if (longJavaArray == nullptr) {
    return nullptr;
  }
  int i = 0;
  for (auto bm : bml) {
    longCArray[i++] = bm.address;
    longCArray[i++] = bm.size;
  }
  env->SetLongArrayRegion(longJavaArray, 0, longCArraySize,
                          reinterpret_cast<jlong *>(longCArray));
  std::free(longCArray);
  env->ReleaseStringUTFChars(key, raw_key);
  return longJavaArray;
}

JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_del(JNIEnv *env,
                                                            jobject obj,
                                                            jstring key,
                                                            jlong objectId) {
  const char *raw_key = env->GetStringUTFChars(key, 0);
  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->begin_tx();
  int res = client->del(raw_key);
  client->end_tx();
  env->ReleaseStringUTFChars(key, raw_key);
  return res;
}

JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_read_1(
    JNIEnv *env, jobject obj, jlong address, jlong size, jobject data,
    jlong objectId) {
  char *raw_data = static_cast<char *>((*env).GetDirectBufferAddress(data));
  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->begin_tx();
  int success = client->read(address, raw_data, size);
  client->end_tx();
  return success;
}

JNIEXPORT void JNICALL Java_com_intel_rpmp_PmPoolClient_shutdown_1(
    JNIEnv *env, jobject obj, jlong objectId) {
  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->shutdown();
}

JNIEXPORT void JNICALL Java_com_intel_rpmp_PmPoolClient_waitToStop_1(
    JNIEnv *env, jobject obj, jlong objectId) {
  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  client->wait();
}

JNIEXPORT void JNICALL Java_com_intel_rpmp_PmPoolClient_dispose_1(
    JNIEnv *env, jobject obj, jlong objectId) {
  PmPoolClient *client = reinterpret_cast<PmPoolClient *>(objectId);
  delete client;
}
