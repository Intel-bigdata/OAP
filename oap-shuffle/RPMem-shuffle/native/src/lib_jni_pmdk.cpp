#include "lib_jni_pmdk.h"
#include "PmemBuffer.h"
#include "pmemkv.h"

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeOpenDevice
  (JNIEnv *env, jclass obj, jstring path, jlong size) {
  const char *CStr = env->GetStringUTFChars(path, 0);
  pmemkv* kv= new pmemkv(CStr);
  env->ReleaseStringUTFChars(path, CStr);
  return (long)kv;
}

JNIEXPORT void JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeSetBlock
  (JNIEnv *env, jclass obj, jlong kv, jstring key, jobject byteBuffer, jint dataSize, jboolean set_clean) {
  jbyte* buf = (jbyte*)(*env).GetDirectBufferAddress(byteBuffer);
  if (buf == nullptr) {
    return;
  }
  const char* CStr = env->GetStringUTFChars(key, 0);
  string key_str(CStr);
  pmemkv *pmkv = static_cast<pmemkv*>((void*)kv);
  pmkv->put(key_str, (char*)buf, dataSize);
}

JNIEXPORT jlongArray JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetBlockIndex
  (JNIEnv *env, jclass obj, jlong kv, jstring key) {
  const char *CStr = env->GetStringUTFChars(key, 0);
  string key_str(CStr);
  pmemkv *pmkv = static_cast<pmemkv*>((void*)kv);
  uint64_t size = 0;
  pmkv->get_meta_size(key_str, &size);
  struct memory_meta* mm = (struct memory_meta*)std::malloc(sizeof(struct memory_meta));
  mm->meta = (uint64_t*)std::malloc(size*2*sizeof(uint64_t));
  pmkv->get_meta(key_str, mm);
  jlongArray data = env->NewLongArray(mm->length);
  env->SetLongArrayRegion(data, 0, mm->length, (jlong*)mm->meta);
  std::free(mm->meta);
  std::free(mm);
  return data;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetBlockSize
  (JNIEnv *env, jclass obj, jlong kv, jstring key) {
  const char *CStr = env->GetStringUTFChars(key, 0);
  string key_str(CStr);
  pmemkv *pmkv = static_cast<pmemkv*>((void*)kv);
  uint64_t value_size;
  pmkv->get_value_size(key_str, &value_size);
  return value_size;
  }

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeCloseDevice
  (JNIEnv *env, jclass obj, jlong kv) {
  pmemkv *pmkv = static_cast<pmemkv*>((void*)kv);
  //pmkv->free_all();
  delete pmkv;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetRoot
  (JNIEnv *env, jclass obj, jlong kv) {
  pmemkv *pmkv = static_cast<pmemkv*>((void*)kv);
  return pmkv->get_root();
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeNewPmemBuffer
  (JNIEnv *env, jobject obj) {
  return (long)(new PmemBuffer());
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeNewPmemBufferBySize
  (JNIEnv *env, jobject obj, jlong len) {
  return (long)(new PmemBuffer(len));
}

JNIEXPORT void JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeLoadPmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer, jlong addr, jint len) {
  ((PmemBuffer*)pmBuffer)->load((char*)addr, len);
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeReadBytesFromPmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer, jbyteArray data, jint off, jint len) {
  jboolean isCopy = JNI_FALSE;
  jbyte* ret_data = env->GetByteArrayElements(data, &isCopy);
  int read_len = ((PmemBuffer*)pmBuffer)->read((char*)ret_data + off, len);
  if (isCopy == JNI_TRUE) {
    env->ReleaseByteArrayElements(data, ret_data, 0);
  }
  return read_len;
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeWriteBytesToPmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer, jbyteArray data, jint off, jint len) {
  jboolean isCopy = JNI_FALSE;
  jbyte* ret_data = env->GetByteArrayElements(data, &isCopy);
  int read_len = ((PmemBuffer*)pmBuffer)->write((char*)ret_data + off, len);
  if (isCopy == JNI_TRUE) {
    env->ReleaseByteArrayElements(data, ret_data, 0);
  }
  return read_len;
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeGetPmemBufferRemaining
  (JNIEnv *env, jobject obj, jlong pmBuffer) {
  ((PmemBuffer*)pmBuffer)->getRemaining();
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeGetPmemBufferDataAddr
  (JNIEnv *env, jobject obj, jlong pmBuffer) {
  return (long)(((PmemBuffer*)pmBuffer)->getDataAddr());
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeCleanPmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer) {
  ((PmemBuffer*)pmBuffer)->clean();
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeDeletePmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer) {
  delete (PmemBuffer*)pmBuffer;
  return 0;
}
