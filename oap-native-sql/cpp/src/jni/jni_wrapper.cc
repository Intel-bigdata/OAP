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

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/util/compression.h>
#include <jni.h>

#include <iostream>
#include <memory>
#include <string>

#include "codegen/code_generator_factory.h"
#include "codegen/common/hash_relation.h"
#include "codegen/common/result_iterator.h"
#include "data_source/parquet/adapter.h"
#include "jni/concurrent_map.h"
#include "jni/jni_common.h"
#include "proto/protobuf_utils.h"
#include "shuffle/splitter.h"

namespace types {
class ExpressionList;
}  // namespace types

static jclass arrow_record_batch_builder_class;
static jmethodID arrow_record_batch_builder_constructor;

static jclass arrow_field_node_builder_class;
static jmethodID arrow_field_node_builder_constructor;

static jclass arrowbuf_builder_class;
static jmethodID arrowbuf_builder_constructor;

static jclass split_result_class;
static jmethodID split_result_constructor;

static jclass native_memory_reservation_class;
static jclass native_direct_memory_reservation_class;
static jmethodID reserve_memory_method;
static jmethodID unreserve_memory_method;

using arrow::jni::ConcurrentMap;
static ConcurrentMap<std::shared_ptr<arrow::Buffer>> buffer_holder_;

static jint JNI_VERSION = JNI_VERSION_1_8;

using CodeGenerator = sparkcolumnarplugin::codegen::CodeGenerator;
static arrow::jni::ConcurrentMap<std::shared_ptr<CodeGenerator>> handler_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<ResultIteratorBase>>
    batch_iterator_holder_;

using sparkcolumnarplugin::shuffle::SplitOptions;
using sparkcolumnarplugin::shuffle::Splitter;
static arrow::jni::ConcurrentMap<std::shared_ptr<Splitter>> shuffle_splitter_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<arrow::Schema>>
    decompression_schema_holder_;
static arrow::jni::ConcurrentMap<arrow::MemoryPool*> memory_pool_holder;

static int64_t default_memory_pool_id;

std::shared_ptr<CodeGenerator> GetCodeGenerator(JNIEnv* env, jlong id) {
  auto handler = handler_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return handler;
}

std::shared_ptr<ResultIteratorBase> GetBatchIterator(JNIEnv* env, jlong id) {
  auto handler = batch_iterator_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return handler;
}

template <typename T>
std::shared_ptr<ResultIterator<T>> GetBatchIterator(JNIEnv* env, jlong id) {
  auto handler = GetBatchIterator(env, id);
  return std::dynamic_pointer_cast<ResultIterator<T>>(handler);
}

jobject MakeRecordBatchBuilder(JNIEnv* env, std::shared_ptr<arrow::Schema> schema,
                               std::shared_ptr<arrow::RecordBatch> record_batch) {
  jobjectArray field_array =
      env->NewObjectArray(schema->num_fields(), arrow_field_node_builder_class, nullptr);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto column = record_batch->column(i);
    auto dataArray = column->data();
    jobject field = env->NewObject(arrow_field_node_builder_class,
                                   arrow_field_node_builder_constructor, column->length(),
                                   column->null_count());
    env->SetObjectArrayElement(field_array, i, field);

    for (auto& buffer : dataArray->buffers) {
      buffers.push_back(buffer);
    }
  }

  jobjectArray arrowbuf_builder_array =
      env->NewObjectArray(buffers.size(), arrowbuf_builder_class, nullptr);

  for (size_t j = 0; j < buffers.size(); ++j) {
    auto buffer = buffers[j];
    uint8_t* data = nullptr;
    int size = 0;
    int64_t capacity = 0;
    if (buffer != nullptr) {
      data = (uint8_t*)buffer->data();
      size = (int)buffer->size();
      capacity = buffer->capacity();
    }
    jobject arrowbuf_builder =
        env->NewObject(arrowbuf_builder_class, arrowbuf_builder_constructor,
                       buffer_holder_.Insert(std::move(buffer)), data, size, capacity);
    env->SetObjectArrayElement(arrowbuf_builder_array, j, arrowbuf_builder);
  }

  // create RecordBatch
  jobject arrow_record_batch_builder = env->NewObject(
      arrow_record_batch_builder_class, arrow_record_batch_builder_constructor,
      record_batch->num_rows(), field_array, arrowbuf_builder_array);
  return arrow_record_batch_builder;
}

using FileSystem = arrow::fs::FileSystem;
using ParquetFileReader = jni::parquet::adapters::ParquetFileReader;
using ParquetFileWriter = jni::parquet::adapters::ParquetFileWriter;

static arrow::jni::ConcurrentMap<std::shared_ptr<ParquetFileReader>> reader_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<ParquetFileWriter>> writer_holder_;

std::shared_ptr<ParquetFileReader> GetFileReader(JNIEnv* env, jlong id) {
  auto reader = reader_holder_.Lookup(id);
  if (!reader) {
    std::string error_message = "invalid reader id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return reader;
}

std::shared_ptr<ParquetFileWriter> GetFileWriter(JNIEnv* env, jlong id) {
  auto writer = writer_holder_.Lookup(id);
  if (!writer) {
    std::string error_message = "invalid reader id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return writer;
}

#ifdef __cplusplus
extern "C" {
#endif


class ReserveMemory : public arrow::ReservationListener {
 public:
  ReserveMemory(JavaVM* vm, jobject memory_reservation)
      : vm_(vm), memory_reservation_(memory_reservation) {}

  arrow::Status OnReservation(int64_t size) override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      return arrow::Status::Invalid("JNIEnv was not attached to current thread");
    }
    env->CallObjectMethod(memory_reservation_, reserve_memory_method, size);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      return arrow::Status::Invalid("Memory reservation failed in Java");
    }
    return arrow::Status::OK();
  }

  arrow::Status OnRelease(int64_t size) override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      return arrow::Status::Invalid("JNIEnv was not attached to current thread");
    }
    env->CallObjectMethod(memory_reservation_, unreserve_memory_method, size);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      return arrow::Status::Invalid("Memory unreservation failed in Java");
    }
    return arrow::Status::OK();
  }

  jobject GetMemoryReservation() {
    return memory_reservation_;
  }

 private:
  JavaVM* vm_;
  jobject memory_reservation_;
};

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  io_exception_class = CreateGlobalClassReference(env, "Ljava/io/IOException;");
  unsupportedoperation_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/UnsupportedOperationException;");
  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");

  arrow_record_batch_builder_class = CreateGlobalClassReference(
      env, "Lcom/intel/oap/vectorized/ArrowRecordBatchBuilder;");
  arrow_record_batch_builder_constructor =
      GetMethodID(env, arrow_record_batch_builder_class, "<init>",
                  "(I[Lcom/intel/oap/vectorized/ArrowFieldNodeBuilder;"
                  "[Lcom/intel/oap/vectorized/ArrowBufBuilder;)V");

  arrow_field_node_builder_class =
      CreateGlobalClassReference(env, "Lcom/intel/oap/vectorized/ArrowFieldNodeBuilder;");
  arrow_field_node_builder_constructor =
      GetMethodID(env, arrow_field_node_builder_class, "<init>", "(II)V");

  arrowbuf_builder_class =
      CreateGlobalClassReference(env, "Lcom/intel/oap/vectorized/ArrowBufBuilder;");
  arrowbuf_builder_constructor =
      GetMethodID(env, arrowbuf_builder_class, "<init>", "(JJIJ)V");

  split_result_class =
      CreateGlobalClassReference(env, "Lcom/intel/oap/vectorized/SplitResult;");
  split_result_constructor = GetMethodID(env, split_result_class, "<init>", "(JJJJJ[J)V");


  native_memory_reservation_class =
      CreateGlobalClassReference(env,
                                 "Lcom/intel/oap/vectorized/NativeMemoryReservation;");
  native_direct_memory_reservation_class =
      CreateGlobalClassReference(env,
                                 "Lcom/intel/oap/vectorized/"
                                 "NativeDirectMemoryReservation;");

  reserve_memory_method =
      GetMethodID(env, native_memory_reservation_class, "reserve", "(J)V");
  unreserve_memory_method =
      GetMethodID(env, native_memory_reservation_class, "unreserve", "(J)V");

  default_memory_pool_id = memory_pool_holder.Insert(arrow::default_memory_pool());

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  env->DeleteGlobalRef(io_exception_class);
  env->DeleteGlobalRef(unsupportedoperation_exception_class);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);

  env->DeleteGlobalRef(arrow_field_node_builder_class);
  env->DeleteGlobalRef(arrowbuf_builder_class);
  env->DeleteGlobalRef(arrow_record_batch_builder_class);
  env->DeleteGlobalRef(split_result_class);

  env->DeleteGlobalRef(native_memory_reservation_class);
  env->DeleteGlobalRef(native_direct_memory_reservation_class);

  buffer_holder_.Clear();
  handler_holder_.Clear();
  batch_iterator_holder_.Clear();
  shuffle_splitter_holder_.Clear();
  decompression_schema_holder_.Clear();
  memory_pool_holder.Clear();

  default_memory_pool_id = -1L;
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_vectorized_ExpressionMemoryPool_getDefaultMemoryPool
    (JNIEnv *, jclass) {
  return default_memory_pool_id;
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_vectorized_ExpressionMemoryPool_createListenableMemoryPool
    (JNIEnv* env, jclass, jobject jlistener) {
  jobject jlistener_ref = env->NewGlobalRef(jlistener);
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    env->ThrowNew(illegal_access_exception_class, "Unable to get JavaVM instance");
    return -1;
  }
  std::shared_ptr<arrow::ReservationListener> listener =
      std::make_shared<ReserveMemory>(vm, jlistener_ref);
  auto memory_pool =
      new arrow::ReservationListenableMemoryPool(arrow::default_memory_pool(), listener);
  return memory_pool_holder.Insert(memory_pool);
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ExpressionMemoryPool_releaseMemoryPool
    (JNIEnv* env, jclass, jlong memory_pool_id) {
  arrow::ReservationListenableMemoryPool* pool =
      dynamic_cast<arrow::ReservationListenableMemoryPool*>(
          memory_pool_holder.Lookup(memory_pool_id));
  if (pool == nullptr) {
    return;
  }
  std::shared_ptr<ReserveMemory> rm =
      std::dynamic_pointer_cast<ReserveMemory>(pool->get_listener());
  if (rm == nullptr) {
    return;
  }
  env->DeleteGlobalRef(rm->GetMemoryReservation());
  memory_pool_holder.Erase(memory_pool_id);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetJavaTmpDir(
    JNIEnv* env, jobject obj, jstring pathObj) {
  jboolean ifCopy;
  auto path = env->GetStringUTFChars(pathObj, &ifCopy);
  setenv("NATIVESQL_TMP_DIR", path, 1);
  env->ReleaseStringUTFChars(pathObj, path);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetBatchSize(
    JNIEnv* env, jobject obj, jint batch_size) {
  setenv("NATIVESQL_BATCH_SIZE", std::to_string(batch_size).c_str(), 1);
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeBuild(
    JNIEnv* env, jobject obj, jbyteArray schema_arr, jbyteArray exprs_arr,
    jbyteArray res_schema_arr, jboolean return_when_finish = false) {
  arrow::Status status;

  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  gandiva::ExpressionVector expr_vector;
  gandiva::FieldVector ret_types;
  msg = MakeExprVector(env, exprs_arr, &expr_vector, &ret_types);
  if (!msg.ok()) {
    std::string error_message =
        "failed to parse expressions protobuf, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  if (res_schema_arr != nullptr) {
    std::shared_ptr<arrow::Schema> resSchema;
    msg = MakeSchema(env, res_schema_arr, &resSchema);
    if (!msg.ok()) {
      std::string error_message = "failed to readSchema, err msg is " + msg.message();
      env->ThrowNew(io_exception_class, error_message.c_str());
    }
    ret_types = resSchema->fields();
  }

#ifdef DEBUG
  for (auto expr : expr_vector) {
    std::cout << expr->ToString() << std::endl;
  }
#endif

  std::shared_ptr<CodeGenerator> handler;
  try {
    msg = sparkcolumnarplugin::codegen::CreateCodeGenerator(
        schema, expr_vector, ret_types, &handler, return_when_finish);
  } catch (const std::runtime_error& error) {
    env->ThrowNew(unsupportedoperation_exception_class, error.what());
  } catch (const std::exception& error) {
    env->ThrowNew(io_exception_class, error.what());
  }
  if (!msg.ok()) {
    std::string error_message =
        "nativeBuild: failed to create CodeGenerator, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

#ifdef DEBUG
  auto handler_holder_size = handler_holder_.Size();
  auto batch_holder_size = batch_iterator_holder_.Size();
  auto buffer_holder_size = buffer_holder_.Size();
  std::cout << "build native Evaluator " << handler->ToString()
            << "\nremain refCnt [buffer|Evaluator|batchIterator] is ["
            << buffer_holder_size << "|" << handler_holder_size << "|"
            << batch_holder_size << "]" << std::endl;
#endif

  return handler_holder_.Insert(std::move(handler));
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeBuildWithFinish(
    JNIEnv* env, jobject obj, jbyteArray schema_arr, jbyteArray exprs_arr,
    jbyteArray finish_exprs_arr) {
  arrow::Status status;

  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  gandiva::ExpressionVector expr_vector;
  gandiva::FieldVector ret_types;
  msg = MakeExprVector(env, exprs_arr, &expr_vector, &ret_types);
  if (!msg.ok()) {
    std::string error_message =
        "failed to parse expressions protobuf, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  gandiva::ExpressionVector finish_expr_vector;
  gandiva::FieldVector finish_ret_types;
  msg = MakeExprVector(env, finish_exprs_arr, &finish_expr_vector, &finish_ret_types);
  if (!msg.ok()) {
    std::string error_message =
        "failed to parse expressions protobuf, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<CodeGenerator> handler;
  try {
    msg = sparkcolumnarplugin::codegen::CreateCodeGenerator(
        schema, expr_vector, ret_types, &handler, true, finish_expr_vector);
  } catch (const std::runtime_error& error) {
    env->ThrowNew(unsupportedoperation_exception_class, error.what());
  } catch (const std::exception& error) {
    env->ThrowNew(io_exception_class, error.what());
  }
  if (!msg.ok()) {
    std::string error_message =
        "nativeBuild: failed to create CodeGenerator, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  return handler_holder_.Insert(std::move(handler));
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetReturnFields(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr) {
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  msg = handler->SetResSchema(schema);
  if (!msg.ok()) {
    std::string error_message =
        "failed to set result schema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeClose(JNIEnv* env,
                                                                        jobject obj,
                                                                        jlong id) {
  auto handler = GetCodeGenerator(env, id);
  if (handler.use_count() > 2) {
    std::cout << "evaluator ptr use count is " << handler.use_count() - 1 << std::endl;
  }
#ifdef DEBUG
  auto handler_holder_size = handler_holder_.Size();
  auto batch_holder_size = batch_iterator_holder_.Size();
  auto buffer_holder_size = buffer_holder_.Size();
  std::cout << "close native Evaluator " << handler->ToString()
            << "\nremain refCnt [buffer|Evaluator|batchIterator] is ["
            << buffer_holder_size << "|" << handler_holder_size << "|"
            << batch_holder_size << "]" << std::endl;
#endif
  handler_holder_.Erase(id);
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluate(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in);

  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  status = handler->evaluate(in, &out);

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::Schema> res_schema;
  status = handler->getResSchema(&res_schema);
  jobjectArray record_batch_builder_array =
      env->NewObjectArray(out.size(), arrow_record_batch_builder_class, nullptr);
  int i = 0;
  for (auto record_batch : out) {
    jobject record_batch_builder = MakeRecordBatchBuilder(env, res_schema, record_batch);
    env->SetObjectArrayElement(record_batch_builder_array, i++, record_batch_builder);
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return record_batch_builder_array;
}

JNIEXPORT jstring JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeGetSignature(
    JNIEnv* env, jobject obj, jlong id) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  return env->NewStringUTF((handler->GetSignature()).c_str());
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluateWithSelection(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jint selection_vector_count, jlong selection_vector_buf_addr,
    jlong selection_vector_buf_size) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in);

  // Make Array From SelectionVector
  auto selection_vector_buf = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(selection_vector_buf_addr), selection_vector_buf_size);

  auto selection_arraydata = arrow::ArrayData::Make(
      arrow::uint16(), selection_vector_count, {NULLPTR, selection_vector_buf});
  auto selection_array = arrow::MakeArray(selection_arraydata);

  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  status = handler->evaluate(selection_array, in, &out);

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::Schema> res_schema;
  status = handler->getResSchema(&res_schema);
  jobjectArray record_batch_builder_array =
      env->NewObjectArray(out.size(), arrow_record_batch_builder_class, nullptr);
  int i = 0;
  for (auto record_batch : out) {
    jobject record_batch_builder = MakeRecordBatchBuilder(env, res_schema, record_batch);
    env->SetObjectArrayElement(record_batch_builder_array, i++, record_batch_builder);
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return record_batch_builder_array;
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMember(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in);

  status = handler->SetMember(in);

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeFinish(JNIEnv* env,
                                                                         jobject obj,
                                                                         jlong id) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  status = handler->finish(&out);

  if (!status.ok()) {
    std::string error_message =
        "nativeFinish: finish failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::Schema> schema;
  status = handler->getResSchema(&schema);

  jobjectArray record_batch_builder_array =
      env->NewObjectArray(out.size(), arrow_record_batch_builder_class, nullptr);
  int i = 0;
  for (auto record_batch : out) {
    jobject record_batch_builder = MakeRecordBatchBuilder(env, schema, record_batch);
    env->SetObjectArrayElement(record_batch_builder_array, i++, record_batch_builder);
  }

  return record_batch_builder_array;
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeFinishByIterator(
    JNIEnv* env, jobject obj, jlong id) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<ResultIteratorBase> out;
  status = handler->finish(&out);
  if (!status.ok()) {
    std::string error_message =
        "nativeFinishForIterator: finish failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  return batch_iterator_holder_.Insert(std::move(out));
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetDependency(
    JNIEnv* env, jobject obj, jlong id, jlong iter_id, int index) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  auto iter = GetBatchIterator<arrow::RecordBatch>(env, iter_id);
  status = handler->SetDependency(iter, index);
  if (!status.ok()) {
    std::string error_message =
        "nativeSetDependency: finish failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
}

JNIEXPORT jboolean JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeHasNext(
    JNIEnv* env, jobject obj, jlong id) {
  auto iter = GetBatchIterator(env, id);
  return iter->HasNext();
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeNext(
    JNIEnv* env, jobject obj, jlong id) {
  arrow::Status status;
  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  std::shared_ptr<arrow::RecordBatch> out;
  if (!iter->HasNext()) return nullptr;
  status = iter->Next(&out);
  if (!status.ok()) {
    std::string error_message =
        "nativeNext: get Next() failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  return MakeRecordBatchBuilder(env, out->schema(), out);
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeProcess(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  std::shared_ptr<arrow::RecordBatch> out;
  status = iter->Process(in, &out);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcess: ResultIterator process next failed with error msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return MakeRecordBatchBuilder(env, out->schema(), out);
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeProcessWithSelection(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes, jint selection_vector_count,
    jlong selection_vector_buf_addr, jlong selection_vector_buf_size) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  // Make Array From SelectionVector
  auto selection_vector_buf = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(selection_vector_buf_addr), selection_vector_buf_size);

  auto selection_arraydata = arrow::ArrayData::Make(
      arrow::uint16(), selection_vector_count, {NULLPTR, selection_vector_buf});
  auto selection_array = arrow::MakeArray(selection_arraydata);

  std::shared_ptr<arrow::RecordBatch> out;
  status = iter->Process(in, &out, selection_array);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcess: ResultIterator process next failed with error msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return MakeRecordBatchBuilder(env, out->schema(), out);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeProcessAndCacheOne(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  status = iter->ProcessAndCacheOne(in);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcessAndCache: ResultIterator process next failed with error msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeProcessAndCacheOneWithSelection(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes, jint selection_vector_count,
    jlong selection_vector_buf_addr, jlong selection_vector_buf_size) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  // Make Array From SelectionVector
  auto selection_vector_buf = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(selection_vector_buf_addr), selection_vector_buf_size);

  auto selection_arraydata = arrow::ArrayData::Make(
      arrow::uint16(), selection_vector_count, {NULLPTR, selection_vector_buf});
  auto selection_array = arrow::MakeArray(selection_arraydata);
  status = iter->ProcessAndCacheOne(in, selection_array);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcessAndCache: ResultIterator process next failed with error msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeSetDependencies(
    JNIEnv* env, jobject this_obj, jlong id, jlongArray ids) {
  int ids_size = env->GetArrayLength(ids);
  long* ids_data = env->GetLongArrayElements(ids, 0);
  std::vector<std::shared_ptr<ResultIteratorBase>> dependent_batch_list;
  for (int i = 0; i < ids_size; i++) {
    auto handler = GetBatchIterator(env, ids_data[i]);
    dependent_batch_list.push_back(handler);
  }
  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  iter->SetDependencies(dependent_batch_list);
  env->ReleaseLongArrayElements(ids, ids_data, JNI_ABORT);
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeClose(
    JNIEnv* env, jobject this_obj, jlong id) {
#ifdef DEBUG
  auto it = batch_iterator_holder_.Lookup(id);
  if (it.use_count() > 2) {
    std::cout << it->ToString() << " ptr use count is " << it.use_count() << std::endl;
  }
#endif
  batch_iterator_holder_.Erase(id);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_AdaptorReferenceManager_nativeRelease(JNIEnv* env,
                                                                    jobject this_obj,
                                                                    jlong id) {
#ifdef DEBUG
  auto it = buffer_holder_.Lookup(id);
  if (it.use_count() > 2) {
    std::cout << "buffer ptr use count is " << it.use_count() << std::endl;
  }
#endif
  buffer_holder_.Erase(id);
}

///////////// Parquet Reader and Writer /////////////
JNIEXPORT jlong JNICALL
Java_com_intel_oap_datasource_parquet_ParquetReaderJniWrapper_nativeOpenParquetReader(
    JNIEnv* env, jobject obj, jstring path, jlong batch_size) {
  arrow::Status status;
  std::string cpath = JStringToCString(env, path);

  std::shared_ptr<FileSystem> fs;
  std::string file_name;
  fs = arrow::fs::FileSystemFromUri(cpath, &file_name).ValueOrDie();

  std::shared_ptr<arrow::io::RandomAccessFile> file;
  ARROW_ASSIGN_OR_THROW(file, fs->OpenInputFile(file_name));

  parquet::ArrowReaderProperties properties(true);
  properties.set_batch_size(batch_size);

  std::unique_ptr<ParquetFileReader> reader;
  status =
      ParquetFileReader::Open(file, arrow::default_memory_pool(), properties, &reader);
  if (!status.ok()) {
    std::string error_message = "nativeOpenParquetReader: " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
#ifdef DEBUG
  auto handler_holder_size = handler_holder_.Size();
  auto batch_holder_size = batch_iterator_holder_.Size();
  auto buffer_holder_size = buffer_holder_.Size();
  std::cout << "build native Parquet Reader "
            << "\nremain refCnt [buffer|Evaluator|batchIterator] is ["
            << buffer_holder_size << "|" << handler_holder_size << "|"
            << batch_holder_size << "]" << std::endl;
#endif
  return reader_holder_.Insert(std::shared_ptr<ParquetFileReader>(reader.release()));
}

JNIEXPORT void JNICALL
Java_com_intel_oap_datasource_parquet_ParquetReaderJniWrapper_nativeInitParquetReader(
    JNIEnv* env, jobject obj, jlong id, jintArray column_indices,
    jintArray row_group_indices) {
  // Prepare column_indices and row_group_indices from java array.
  bool column_indices_need_release = false;
  int column_indices_len = env->GetArrayLength(column_indices);
  jint* column_indices_ptr = env->GetIntArrayElements(column_indices, 0);
  std::vector<int> _column_indices(column_indices_ptr,
                                   column_indices_ptr + column_indices_len);
  column_indices_need_release = true;

  bool row_group_indices_need_release = false;
  std::vector<int> _row_group_indices = {};
  jint* row_group_indices_ptr;
  int row_group_indices_len = env->GetArrayLength(row_group_indices);
  if (row_group_indices_len != 0) {
    row_group_indices_ptr = env->GetIntArrayElements(row_group_indices, 0);
    std::vector<int> rg_indices_tmp(row_group_indices_ptr,
                                    row_group_indices_ptr + row_group_indices_len);
    _row_group_indices = rg_indices_tmp;
    row_group_indices_need_release = true;
  }

  // Call ParquetFileReader init func.
  arrow::Status status;
  auto reader = GetFileReader(env, id);
  status = reader->InitRecordBatchReader(_column_indices, _row_group_indices);
  if (!status.ok()) {
    std::string error_message =
        "nativeInitParquetReader: failed to Initialize, err msg is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  if (column_indices_need_release) {
    env->ReleaseIntArrayElements(column_indices, column_indices_ptr, JNI_ABORT);
  }
  if (row_group_indices_need_release) {
    env->ReleaseIntArrayElements(row_group_indices, row_group_indices_ptr, JNI_ABORT);
  }
}

JNIEXPORT void JNICALL
Java_com_intel_oap_datasource_parquet_ParquetReaderJniWrapper_nativeInitParquetReader2(
    JNIEnv* env, jobject obj, jlong id, jintArray column_indices, jlong start_pos,
    jlong end_pos) {
  // Prepare column_indices and row_group_indices from java array.
  bool column_indices_need_release = false;
  int column_indices_len = env->GetArrayLength(column_indices);
  jint* column_indices_ptr = env->GetIntArrayElements(column_indices, 0);
  std::vector<int> _column_indices(column_indices_ptr,
                                   column_indices_ptr + column_indices_len);
  column_indices_need_release = true;

  // Call ParquetFileReader init func.
  arrow::Status status;
  auto reader = GetFileReader(env, id);
  status = reader->InitRecordBatchReader(_column_indices, start_pos, end_pos);
  if (!status.ok()) {
    std::string error_message =
        "nativeInitParquetReader2: failed to Initialize, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  if (column_indices_need_release) {
    env->ReleaseIntArrayElements(column_indices, column_indices_ptr, JNI_ABORT);
  }
}

JNIEXPORT void JNICALL
Java_com_intel_oap_datasource_parquet_ParquetReaderJniWrapper_nativeCloseParquetReader(
    JNIEnv* env, jobject obj, jlong id) {
  reader_holder_.Erase(id);
#ifdef DEBUG
  auto handler_holder_size = handler_holder_.Size();
  auto batch_holder_size = batch_iterator_holder_.Size();
  auto buffer_holder_size = buffer_holder_.Size();
  std::cout << "close native Parquet Reader "
            << "\nremain refCnt [buffer|Evaluator|batchIterator] is ["
            << buffer_holder_size << "|" << handler_holder_size << "|"
            << batch_holder_size << "]" << std::endl;
#endif
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_datasource_parquet_ParquetReaderJniWrapper_nativeReadNext(JNIEnv* env,
                                                                             jobject obj,
                                                                             jlong id) {
  arrow::Status status;
  auto reader = GetFileReader(env, id);

  std::shared_ptr<arrow::RecordBatch> record_batch;
  status = reader->ReadNext(&record_batch);
  if (!status.ok()) {
    std::string error_message =
        "nativeReadNext: failed to read next batch, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  if (record_batch == nullptr) {
    return nullptr;
  }

  return MakeRecordBatchBuilder(env, record_batch->schema(), record_batch);
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_datasource_parquet_ParquetReaderJniWrapper_nativeGetSchema(JNIEnv* env,
                                                                              jobject obj,
                                                                              jlong id) {
  arrow::Status status;
  auto reader = GetFileReader(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = reader->ReadSchema(&schema);

  if (!status.ok()) {
    std::string error_message =
        "nativeGetSchema: failed to read schema, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jbyteArray ret = ToSchemaByteArray(env, schema);
  if (ret == nullptr) {
    std::string error_message = "nativeGetSchema: failed to convert schema to byte array";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  return ret;
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_datasource_parquet_ParquetWriterJniWrapper_nativeOpenParquetWriter(
    JNIEnv* env, jobject obj, jstring path, jbyteArray schemaBytes) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  status = MakeSchema(env, schemaBytes, &schema);
  if (!status.ok()) {
    std::string error_message =
        "nativeOpenParquetWriter: failed to readSchema, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::string cpath = JStringToCString(env, path);

  std::shared_ptr<FileSystem> fs;
  std::string file_name;

  fs = arrow::fs::FileSystemFromUri(cpath, &file_name).ValueOrDie();

  // check if directory exists
  auto dir = arrow::fs::internal::GetAbstractPathParent(file_name).first;
  arrow::fs::FileInfo info;
  info = fs->GetFileInfo("/" + dir).ValueOrDie();
  if (info.type() == arrow::fs::FileType::NotFound) {
    status = fs->CreateDir(dir);
    if (!status.ok()) {
      std::string error_message = "nativeOpenParquetWriter: " + status.message();
      env->ThrowNew(io_exception_class, error_message.c_str());
    }
  }

  std::shared_ptr<arrow::io::OutputStream> sink;
  ARROW_ASSIGN_OR_THROW(sink, fs->OpenOutputStream(file_name));

  std::unique_ptr<ParquetFileWriter> writer;
  status = ParquetFileWriter::Open(sink, arrow::default_memory_pool(), schema, &writer);
  if (!status.ok()) {
    std::string error_message = "nativeOpenParquetWriter: " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  return writer_holder_.Insert(std::shared_ptr<ParquetFileWriter>(writer.release()));
}

JNIEXPORT void JNICALL
Java_com_intel_oap_datasource_parquet_ParquetWriterJniWrapper_nativeCloseParquetWriter(
    JNIEnv* env, jobject obj, jlong id) {
  arrow::Status status;
  auto writer = GetFileWriter(env, id);
  status = writer->Flush();
  if (!status.ok()) {
    std::string error_message =
        "nativeCloseParquetWriter: failed to Flush, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  writer_holder_.Erase(id);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_datasource_parquet_ParquetWriterJniWrapper_nativeWriteNext(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray bufAddrs,
    jlongArray bufSizes) {
  // convert input data to record batch
  int in_bufs_len = env->GetArrayLength(bufAddrs);
  if (in_bufs_len != env->GetArrayLength(bufSizes)) {
    std::string error_message =
        "nativeWriteNext: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(bufAddrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(bufSizes, 0);

  arrow::Status status;
  auto writer = GetFileWriter(env, id);

  std::shared_ptr<arrow::Schema> schema;
  status = writer->GetSchema(&schema);
  if (!status.ok()) {
    std::string error_message =
        "nativeWriteNext: failed to read schema, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::RecordBatch> record_batch;
  status = MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len,
                           &record_batch);
  if (!status.ok()) {
    std::string error_message =
        "nativeWriteNext: failed to get record batch, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  status = writer->WriteNext(record_batch);
  if (!status.ok()) {
    std::string error_message =
        "nativeWriteNext: failed to write next batch, err msg is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(bufAddrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(bufSizes, in_buf_sizes, JNI_ABORT);
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_nativeMake(
    JNIEnv* env, jobject, jstring partitioning_name_jstr, jint num_partitions,
    jbyteArray schema_arr, jbyteArray expr_arr, jint buffer_size,
    jstring compression_type_jstr, jstring data_file_jstr, jint num_sub_dirs,
    jstring local_dirs_jstr) {
  if (partitioning_name_jstr == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Short partitioning name can't be null").c_str());
    return 0;
  }
  if (schema_arr == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Make splitter schema can't be null").c_str());
    return 0;
  }
  if (data_file_jstr == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Shuffle DataFile can't be null").c_str());
    return 0;
  }
  if (local_dirs_jstr == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Shuffle DataFile can't be null").c_str());
    return 0;
  }

  auto partitioning_name_c = env->GetStringUTFChars(partitioning_name_jstr, JNI_FALSE);
  auto partitioning_name = std::string(partitioning_name_c);
  env->ReleaseStringUTFChars(partitioning_name_jstr, partitioning_name_c);

  auto splitOptions = SplitOptions::Defaults();
  if (buffer_size > 0) {
    splitOptions.buffer_size = buffer_size;
  }
  if (num_sub_dirs > 0) {
    splitOptions.num_sub_dirs = num_sub_dirs;
  }

  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      splitOptions.compression_type = compression_type_result.MoveValueUnsafe();
    }
  }

  auto data_file_c = env->GetStringUTFChars(data_file_jstr, JNI_FALSE);
  splitOptions.data_file = std::string(data_file_c);
  env->ReleaseStringUTFChars(data_file_jstr, data_file_c);

  auto local_dirs = env->GetStringUTFChars(local_dirs_jstr, JNI_FALSE);
  setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs, 1);
  env->ReleaseStringUTFChars(local_dirs_jstr, local_dirs);

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  gandiva::ExpressionVector expr_vector = {};
  if (expr_arr != NULL) {
    gandiva::FieldVector ret_types;
    auto status = MakeExprVector(env, expr_arr, &expr_vector, &ret_types);
    if (!status.ok()) {
      env->ThrowNew(
          illegal_argument_exception_class,
          std::string("Failed to parse expressions protobuf, error message is " +
                      status.message())
              .c_str());
      return 0;
    }
  }

  jclass cls = env->FindClass("java/lang/Thread");
  jmethodID mid = env->GetStaticMethodID(cls, "currentThread", "()Ljava/lang/Thread;");
  jobject thread = env->CallStaticObjectMethod(cls, mid);
  if (thread == NULL) {
    std::cout << "Thread.currentThread() return NULL" << std::endl;
  } else {
    jmethodID mid_getid = env->GetMethodID(cls, "getId", "()J");
    jlong sid = env->CallLongMethod(thread, mid_getid);
    splitOptions.thread_id = (int64_t)sid;
  }

  jclass tc_cls = env->FindClass("org/apache/spark/TaskContext");
  jmethodID get_tc_mid = env->GetStaticMethodID(tc_cls, "get", "()Lorg/apache/spark/TaskContext;");
  jobject tc_obj = env->CallStaticObjectMethod(tc_cls, get_tc_mid);
  if (tc_obj == NULL) {
    std::cout << "TaskContext.get() return NULL" << std::endl;
  } else {
    jmethodID get_tsk_attmpt_mid = env->GetMethodID(tc_cls, "taskAttemptId", "()J");
    jlong attmpt_id = env->CallLongMethod(tc_obj, get_tsk_attmpt_mid);
    splitOptions.task_attempt_id = (int64_t)attmpt_id;
  }

  auto make_result = Splitter::Make(partitioning_name, std::move(schema), num_partitions,
                                    expr_vector, std::move(splitOptions));
  if (!make_result.ok()) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Failed create native shuffle splitter, error message is " +
                              make_result.status().message())
                      .c_str());
    return 0;
  }
  auto splitter = make_result.MoveValueUnsafe();

  return shuffle_splitter_holder_.Insert(std::shared_ptr<Splitter>(splitter));
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_split(
    JNIEnv* env, jobject, jlong splitter_id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes) {
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
    return;
  }
  if (buf_addrs == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native split: buf_addrs can't be null").c_str());
    return;
  }
  if (buf_sizes == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native split: buf_sizes can't be null").c_str());
    return;
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native split: length of buf_addrs and buf_sizes mismatch").c_str());
    return;
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> in;
  auto status = MakeRecordBatch(splitter->schema(), num_rows, (int64_t*)in_buf_addrs,
                                (int64_t*)in_buf_sizes, in_bufs_len, &in);

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  if (!status.ok()) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native split: make record batch failed, error message is " +
                    status.message())
            .c_str());
    return;
  }

  status = splitter->Split(*in);

  if (!status.ok()) {
    // Throw IOException
    env->ThrowNew(io_exception_class,
                  std::string("Native split: splitter split failed, error message is " +
                              status.message())
                      .c_str());
  }
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_stop(
    JNIEnv* env, jobject, jlong splitter_id) {
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
    return nullptr;
  }

  auto status = splitter->Stop();

  if (!status.ok()) {
    // Throw IOException
    env->ThrowNew(io_exception_class,
                  std::string("Native split: splitter stop failed, error message is " +
                              status.message())
                      .c_str());
    return nullptr;
  }

  const auto& partition_length = splitter->PartitionLengths();
  auto partition_length_arr = env->NewLongArray(partition_length.size());
  auto src = reinterpret_cast<const jlong*>(partition_length.data());
  env->SetLongArrayRegion(partition_length_arr, 0, partition_length.size(), src);
  jobject split_result = env->NewObject(
      split_result_class, split_result_constructor, splitter->TotalComputePidTime(),
      splitter->TotalWriteTime(), splitter->TotalSpillTime(),
      splitter->TotalBytesWritten(), splitter->TotalBytesSpilled(), partition_length_arr);

  return split_result;
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_close(
    JNIEnv* env, jobject, jlong splitter_id) {
  shuffle_splitter_holder_.Erase(splitter_id);
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_make(
    JNIEnv* env, jobject, jbyteArray schema_arr) {
  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  return decompression_schema_holder_.Insert(schema);
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_decompress(
    JNIEnv* env, jobject obj, jlong schema_holder_id, jstring compression_type_jstr,
    jint num_rows, jlongArray buf_addrs, jlongArray buf_sizes, jlongArray buf_mask) {
  auto schema = decompression_schema_holder_.Lookup(schema_holder_id);
  if (!schema) {
    std::string error_message =
        "Invalid schema holder id " + std::to_string(schema_holder_id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
    return nullptr;
  }
  if (buf_addrs == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native decompress: buf_addrs can't be null").c_str());
    return nullptr;
  }
  if (buf_sizes == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native decompress: buf_sizes can't be null").c_str());
    return nullptr;
  }
  if (buf_mask == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native decompress: buf_mask can't be null").c_str());
    return nullptr;
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native decompress: length of buf_addrs and buf_sizes mismatch")
            .c_str());
    return nullptr;
  }

  auto compression_type = arrow::Compression::UNCOMPRESSED;
  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      compression_type = compression_type_result.MoveValueUnsafe();
    }
  }

  // make buffers from raws
  auto in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  auto in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);
  auto in_buf_mask = env->GetLongArrayElements(buf_mask, JNI_FALSE);

  auto num_fields = schema->num_fields();

  std::vector<std::shared_ptr<arrow::Buffer>> input_buffers;
  input_buffers.reserve(in_bufs_len);
  for (auto field_idx = 0, buffer_idx = 0; field_idx < num_fields; ++field_idx) {
    auto field = schema->field(field_idx);
    auto num_buf = arrow::is_base_binary_like(field->type()->id()) ? 3 : 2;
    for (auto i = 0; i < num_buf; ++i, ++buffer_idx) {
      input_buffers.push_back(std::make_shared<arrow::Buffer>(
          reinterpret_cast<const uint8_t*>(in_buf_addrs[buffer_idx]),
          in_buf_sizes[buffer_idx]));
    }
  }

  // decompress buffers
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.use_threads = false;
  auto status =
      DecompressBuffers(compression_type, options, (uint8_t*)in_buf_mask, input_buffers);
  if (!status.ok()) {
    env->ThrowNew(
        io_exception_class,
        std::string("failed to decompress buffers, error message is " + status.message())
            .c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_mask, in_buf_mask, JNI_ABORT);

  // make arrays from buffers
  std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
  for (auto field_idx = 0, buffer_idx = 0; field_idx < num_fields; ++field_idx) {
    auto field = schema->field(field_idx);
    auto num_buf = arrow::is_base_binary_like(field->type()->id()) ? 3 : 2;

    std::vector<std::shared_ptr<arrow::Buffer>> bufs;
    bufs.reserve(num_buf);
    for (auto i = 0; i < num_buf; ++i, ++buffer_idx) {
      bufs.push_back(std::move(input_buffers[buffer_idx]));
    }
    arrays.push_back(arrow::ArrayData::Make(field->type(), num_rows, std::move(bufs)));
  }

  return MakeRecordBatchBuilder(
      env, schema, arrow::RecordBatch::Make(schema, num_rows, std::move(arrays)));
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_close(
    JNIEnv* env, jobject, jlong schema_holder_id) {
  decompression_schema_holder_.Erase(schema_holder_id);
}

#ifdef __cplusplus
}
#endif
