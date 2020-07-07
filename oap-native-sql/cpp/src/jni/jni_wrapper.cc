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
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/util/compression.h>
#include <jni.h>
#include <iostream>
#include <string>
#include "data_source/parquet/adapter.h"
#include "proto/protobuf_utils.h"

#include "codegen/code_generator_factory.h"
#include "codegen/common/result_iterator.h"
#include "jni/concurrent_map.h"
#include "jni/jni_common.h"
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

static jclass partition_file_info_class;
static jmethodID partition_file_info_constructor;

using arrow::jni::ConcurrentMap;
static ConcurrentMap<std::shared_ptr<arrow::Buffer>> buffer_holder_;

static jint JNI_VERSION = JNI_VERSION_1_8;

using CodeGenerator = sparkcolumnarplugin::codegen::CodeGenerator;
static arrow::jni::ConcurrentMap<std::shared_ptr<CodeGenerator>> handler_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<ResultIterator<arrow::RecordBatch>>>
    batch_iterator_holder_;

using sparkcolumnarplugin::shuffle::Splitter;
static arrow::jni::ConcurrentMap<std::shared_ptr<Splitter>> shuffle_splitter_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<arrow::Schema>>
    decompression_schema_holder_;

std::shared_ptr<CodeGenerator> GetCodeGenerator(JNIEnv* env, jlong id) {
  auto handler = handler_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return handler;
}

std::shared_ptr<ResultIterator<arrow::RecordBatch>> GetBatchIterator(JNIEnv* env,
                                                                     jlong id) {
  auto handler = batch_iterator_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return handler;
}

std::shared_ptr<Splitter> GetShuffleSplitter(JNIEnv* env, jlong id) {
  auto splitter = shuffle_splitter_holder_.Lookup(id);
  if (!splitter) {
    std::string error_message = "invalid reader id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }

  return splitter;
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

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  io_exception_class = CreateGlobalClassReference(env, "Ljava/io/IOException;");
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

  arrow_field_node_builder_class = CreateGlobalClassReference(
      env, "Lcom/intel/oap/vectorized/ArrowFieldNodeBuilder;");
  arrow_field_node_builder_constructor =
      GetMethodID(env, arrow_field_node_builder_class, "<init>", "(II)V");

  arrowbuf_builder_class = CreateGlobalClassReference(
      env, "Lcom/intel/oap/vectorized/ArrowBufBuilder;");
  arrowbuf_builder_constructor =
      GetMethodID(env, arrowbuf_builder_class, "<init>", "(JJIJ)V");

  partition_file_info_class = CreateGlobalClassReference(
      env, "Lcom/intel/oap/vectorized/PartitionFileInfo;");
  partition_file_info_constructor =
      GetMethodID(env, partition_file_info_class, "<init>", "(ILjava/lang/String;)V");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  env->DeleteGlobalRef(io_exception_class);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);

  env->DeleteGlobalRef(arrow_field_node_builder_class);
  env->DeleteGlobalRef(arrowbuf_builder_class);
  env->DeleteGlobalRef(arrow_record_batch_builder_class);
  env->DeleteGlobalRef(partition_file_info_class);

  buffer_holder_.Clear();
  handler_holder_.Clear();
  batch_iterator_holder_.Clear();
  shuffle_splitter_holder_.Clear();
  decompression_schema_holder_.Clear();
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

  for (auto expr : expr_vector) {
    std::cout << expr->ToString() << std::endl;
  }

  std::shared_ptr<CodeGenerator> handler;
  msg = sparkcolumnarplugin::codegen::CreateCodeGenerator(schema, expr_vector, ret_types,
                                                          &handler, return_when_finish);
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
  msg = sparkcolumnarplugin::codegen::CreateCodeGenerator(
      schema, expr_vector, ret_types, &handler, true, finish_expr_vector);
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
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeClose(
    JNIEnv* env, jobject obj, jlong id) {
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
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeFinish(
    JNIEnv* env, jobject obj, jlong id) {
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
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> out;
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
  auto iter = GetBatchIterator(env, iter_id);
  status = handler->SetDependency(iter, index);
  if (!status.ok()) {
    std::string error_message =
        "nativeSetDependency: finish failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeNext(JNIEnv* env,
                                                                       jobject obj,
                                                                       jlong id) {
  arrow::Status status;
  auto iter = GetBatchIterator(env, id);
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

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeProcess(
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

  auto iter = GetBatchIterator(env, id);
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

  auto iter = GetBatchIterator(env, id);
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

  auto iter = GetBatchIterator(env, id);
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

  auto iter = GetBatchIterator(env, id);
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

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeClose(JNIEnv* env,
                                                                        jobject this_obj,
                                                                        jlong id) {
#ifdef DEBUG
  auto it = batch_iterator_holder_.Lookup(id);
  if (it.use_count() > 2) {
    std::cout << it->ToString() << " ptr use count is " << it.use_count() << std::endl;
  }
#endif
  batch_iterator_holder_.Erase(id);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_AdaptorReferenceManager_nativeRelease(
    JNIEnv* env, jobject this_obj, jlong id) {
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
Java_com_intel_oap_datasource_parquet_ParquetReaderJniWrapper_nativeReadNext(
    JNIEnv* env, jobject obj, jlong id) {
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
Java_com_intel_oap_datasource_parquet_ParquetReaderJniWrapper_nativeGetSchema(
    JNIEnv* env, jobject obj, jlong id) {
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
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_make(
    JNIEnv* env, jobject, jbyteArray schema_arr, jlong buffer_size, jstring pathObj) {
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status status;

  auto joined_path = env->GetStringUTFChars(pathObj, JNI_FALSE);
  setenv("NATIVESQL_SPARK_LOCAL_DIRS", joined_path, 1);

  env->ReleaseStringUTFChars(pathObj, joined_path);

  status = MakeSchema(env, schema_arr, &schema);
  if (!status.ok()) {
    env->ThrowNew(
        io_exception_class,
        std::string("failed to readSchema, err msg is " + status.message()).c_str());
  }

  auto result = Splitter::Make(schema);
  if (!result.ok()) {
    env->ThrowNew(io_exception_class,
                  std::string("Failed create native shuffle splitter").c_str());
  }

  (*result)->set_buffer_size(buffer_size);

  return shuffle_splitter_holder_.Insert(std::shared_ptr<Splitter>(*result));
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_split(
    JNIEnv* env, jobject, jlong splitter_id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes) {
  auto splitter = GetShuffleSplitter(env, splitter_id);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "native split: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> in;
  auto status = MakeRecordBatch(splitter->schema(), num_rows, (int64_t*)in_buf_addrs,
                                (int64_t*)in_buf_sizes, in_bufs_len, &in);

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  if (!status.ok()) {
    env->ThrowNew(io_exception_class,
                  std::string("native split: make record batch failed").c_str());
  }

  status = splitter->Split(*in);

  if (!status.ok()) {
    env->ThrowNew(io_exception_class,
                  std::string("native split: splitter split failed").c_str());
  }
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_stop(
    JNIEnv* env, jobject, jlong splitter_id) {
  auto splitter = GetShuffleSplitter(env, splitter_id);
  auto status = splitter->Stop();

  if (!status.ok()) {
    env->ThrowNew(io_exception_class,
                  std::string("native split: splitter stop failed, error message is " +
                              status.message())
                      .c_str());
  }
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_setPartitionBufferSize(
    JNIEnv* env, jobject, jlong splitter_id, jlong buffer_size) {
  auto splitter = GetShuffleSplitter(env, splitter_id);

  splitter->set_buffer_size((int64_t)buffer_size);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_setCompressionCodec(
    JNIEnv* env, jobject, jlong splitter_id, jstring codec_jstr) {
  auto splitter = GetShuffleSplitter(env, splitter_id);

  auto compression_codec = arrow::Compression::UNCOMPRESSED;
  auto codec_l = env->GetStringUTFChars(codec_jstr, JNI_FALSE);
  if (codec_l != nullptr) {
    std::string codec_u;
    std::transform(codec_l, codec_l + std::strlen(codec_l), std::back_inserter(codec_u),
                   ::toupper);
    auto result = arrow::util::Codec::GetCompressionType(codec_u);
    if (result.ok()) {
      compression_codec = *result;
    } else {
      env->ThrowNew(io_exception_class,
                    std::string("failed to get compression codec, error message is " +
                                result.status().message())
                        .c_str());
    }
    if (compression_codec == arrow::Compression::LZ4) {
      compression_codec = arrow::Compression::LZ4_FRAME;
    }
  }
  env->ReleaseStringUTFChars(codec_jstr, codec_l);

  splitter->set_compression_codec(compression_codec);
}

JNIEXPORT jobjectArray JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_getPartitionFileInfo(
    JNIEnv* env, jobject, jlong splitter_id) {
  auto splitter = GetShuffleSplitter(env, splitter_id);

  const auto& partition_file_info = splitter->GetPartitionFileInfo();
  auto num_partitions = partition_file_info.size();

  jobjectArray partition_file_info_array =
      env->NewObjectArray(num_partitions, partition_file_info_class, nullptr);

  for (auto i = 0; i < num_partitions; ++i) {
    jobject file_info_obj =
        env->NewObject(partition_file_info_class, partition_file_info_constructor,
                       partition_file_info[i].first,
                       env->NewStringUTF(partition_file_info[i].second.c_str()));
    env->SetObjectArrayElement(partition_file_info_array, i, file_info_obj);
  }
  return partition_file_info_array;
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_getTotalBytesWritten(
    JNIEnv* env, jobject, jlong splitter_id) {
  auto splitter = GetShuffleSplitter(env, splitter_id);
  auto result = splitter->TotalBytesWritten();

  if (!result.ok()) {
    env->ThrowNew(io_exception_class,
                  std::string("native split: get total bytes written failed").c_str());
  }

  return (jlong)*result;
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_close(
    JNIEnv* env, jobject, jlong splitter_id) {
  shuffle_splitter_holder_.Erase(splitter_id);
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_make(
    JNIEnv* env, jobject, jbyteArray schema_arr) {
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status status;

  status = MakeSchema(env, schema_arr, &schema);
  if (!status.ok()) {
    env->ThrowNew(
        io_exception_class,
        std::string("failed to readSchema, err msg is " + status.message()).c_str());
  }

  return decompression_schema_holder_.Insert(schema);
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_decompress(
    JNIEnv* env, jobject obj, jlong schema_holder_id, jstring codec_jstr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes, jlongArray buf_mask) {
  auto schema = decompression_schema_holder_.Lookup(schema_holder_id);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "native decompress: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  auto in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  auto in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);
  auto in_buf_mask = env->GetLongArrayElements(buf_mask, JNI_FALSE);
  int buf_idx = 0;
  int field_idx = 0;

  // get decompression compression_codec
  auto compression_codec = arrow::Compression::UNCOMPRESSED;
  auto codec_l = env->GetStringUTFChars(codec_jstr, JNI_FALSE);
  if (codec_l != nullptr) {
    std::string codec_u;
    std::transform(codec_l, codec_l + std::strlen(codec_l), std::back_inserter(codec_u),
                   ::toupper);
    auto result = arrow::util::Codec::GetCompressionType(codec_u);
    if (result.ok()) {
      compression_codec = *result;
    } else {
      env->ThrowNew(io_exception_class,
                    std::string("failed to get compression codec, error message is " +
                                result.status().message())
                        .c_str());
    }
    if (compression_codec == arrow::Compression::LZ4) {
      compression_codec = arrow::Compression::LZ4_FRAME;
    }
  }
  env->ReleaseStringUTFChars(codec_jstr, codec_l);

  std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
  while (field_idx < schema->num_fields()) {
    auto field = schema->field(field_idx);
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;

    // decompress validity buffer
    auto result = arrow::BitUtil::GetBit((uint8_t*)in_buf_mask, buf_idx)
                      ? DecompressBuffer(in_buf_addrs[buf_idx], in_buf_sizes[buf_idx],
                                         arrow::Compression::UNCOMPRESSED)
                      : DecompressBuffer(in_buf_addrs[buf_idx], in_buf_sizes[buf_idx],
                                         compression_codec);
    if (result.ok()) {
      buffers.push_back(std::move(result).ValueOrDie());
    } else {
      env->ThrowNew(
          io_exception_class,
          std::string("failed to decompress validity buffer, error message is " +
                      result.status().message())
              .c_str());
    }

    // decompress value buffer
    result = DecompressBuffer(in_buf_addrs[buf_idx + 1], in_buf_sizes[buf_idx + 1],
                              compression_codec);
    if (result.ok()) {
      buffers.push_back(std::move(result).ValueOrDie());
    } else {
      env->ThrowNew(io_exception_class,
                    std::string("failed to decompress value buffer, error message is " +
                                result.status().message())
                        .c_str());
    }

    if (arrow::is_binary_like(field->type()->id())) {
      // decompress offset buffer
      result = DecompressBuffer(in_buf_addrs[buf_idx + 2], in_buf_sizes[buf_idx + 2],
                                compression_codec);
      if (result.ok()) {
        buffers.push_back(std::move(result).ValueOrDie());
      } else {
        env->ThrowNew(
            io_exception_class,
            std::string("failed to decompress offset buffer, error message is " +
                        result.status().message())
                .c_str());
      }
      buf_idx += 3;
    } else {
      buf_idx += 2;
    }
    arrays.push_back(arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers)));

    ++field_idx;
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_mask, in_buf_mask, JNI_ABORT);

  return MakeRecordBatchBuilder(
      env, schema, arrow::RecordBatch::Make(schema, num_rows, std::move(arrays)));
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_close(
    JNIEnv* env, jobject, jlong schema_holder_id) {
  decompression_schema_holder_.Erase(schema_holder_id);
}

#ifdef __cplusplus
}
#endif
