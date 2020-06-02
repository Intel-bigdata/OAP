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

#include <arrow/pretty_print.h>
#include <google/protobuf/io/coded_stream.h>

#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <gandiva/arrow.h>
#include <gandiva/gandiva_aliases.h>
#include <gandiva/tree_expr_builder.h>

static jclass io_exception_class;
static jclass illegal_access_exception_class;
static jclass illegal_argument_exception_class;

#define ARROW_ASSIGN_OR_THROW_IMPL(status_name, lhs, rexpr)                    \
  auto status_name = (rexpr);                                                  \
  if (!status_name.status().ok()) {                                            \
    env->ThrowNew(io_exception_class, status_name.status().message().c_str()); \
  }                                                                            \
  lhs = std::move(status_name).ValueOrDie();

#define ARROW_ASSIGN_OR_THROW_NAME(x, y) ARROW_CONCAT(x, y)

// Executes an expression that returns a Result, extracting its value
// into the variable defined by lhs (or returning on error).
//
// Example: Assigning to a new value
//   ARROW_ASSIGN_OR_THROW(auto value, MaybeGetValue(arg));
//
// Example: Assigning to an existing value
//   ValueType value;
//   ARROW_ASSIGN_OR_THROW(value, MaybeGetValue(arg));
//
// WARNING: ASSIGN_OR_RAISE expands into multiple statements; it cannot be used
//  in a single statement (e.g. as the body of an if statement without {})!
#define ARROW_ASSIGN_OR_THROW(lhs, rexpr)                                              \
  ARROW_ASSIGN_OR_THROW_IMPL(ARROW_ASSIGN_OR_THROW_NAME(_error_or_value, __COUNTER__), \
                             lhs, rexpr);

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  if (global_class == nullptr) {
    std::string error_message =
        "Unable to createGlobalClassReference for" + std::string(class_name);
    env->ThrowNew(illegal_access_exception_class, error_message.c_str());
  }
  return global_class;
}

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
                                " within signature" + std::string(sig);
    env->ThrowNew(illegal_access_exception_class, error_message.c_str());
  }

  return ret;
}

arrow::Status MakeRecordBatch(const std::shared_ptr<arrow::Schema>& schema, int num_rows,
                              int64_t* in_buf_addrs, int64_t* in_buf_sizes,
                              int in_bufs_len,
                              std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
  auto num_fields = schema->num_fields();
  int buf_idx = 0;
  int sz_idx = 0;

  for (int i = 0; i < num_fields; i++) {
    auto field = schema->field(i);
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;

    if (buf_idx >= in_bufs_len) {
      return arrow::Status::Invalid("insufficient number of in_buf_addrs");
    }
    int64_t validity_addr = in_buf_addrs[buf_idx++];
    int64_t validity_size = in_buf_sizes[sz_idx++];
    auto validity = std::shared_ptr<arrow::Buffer>(
        new arrow::Buffer(reinterpret_cast<uint8_t*>(validity_addr), validity_size));
    buffers.push_back(validity);

    if (arrow::is_binary_like(field->type()->id())) {
      if (buf_idx >= in_bufs_len) {
        return arrow::Status::Invalid("insufficient number of in_buf_addrs");
      }

      // add offsets buffer for variable-len fields.
      int64_t offsets_addr = in_buf_addrs[buf_idx++];
      int64_t offsets_size = in_buf_sizes[sz_idx++];
      auto offsets = std::shared_ptr<arrow::Buffer>(
          new arrow::Buffer(reinterpret_cast<uint8_t*>(offsets_addr), offsets_size));
      buffers.push_back(offsets);
    }

    if (buf_idx >= in_bufs_len) {
      return arrow::Status::Invalid("insufficient number of in_buf_addrs");
    }
    int64_t value_addr = in_buf_addrs[buf_idx++];
    int64_t value_size = in_buf_sizes[sz_idx++];
    auto data = std::shared_ptr<arrow::Buffer>(
        new arrow::Buffer(reinterpret_cast<uint8_t*>(value_addr), value_size));
    buffers.push_back(data);

    auto array_data = arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers));
    arrays.push_back(array_data);
  }

  *batch = arrow::RecordBatch::Make(schema, num_rows, arrays);
  return arrow::Status::OK();
}

std::string JStringToCString(JNIEnv* env, jstring string) {
  int32_t jlen, clen;
  clen = env->GetStringUTFLength(string);
  jlen = env->GetStringLength(string);
  std::vector<char> buffer(clen);
  env->GetStringUTFRegion(string, 0, jlen, buffer.data());
  return std::string(buffer.data(), clen);
}

arrow::Status MakeSchema(JNIEnv* env, jbyteArray schema_arr,
                         std::shared_ptr<arrow::Schema>* schema) {
  jsize schema_len = env->GetArrayLength(schema_arr);
  jbyte* schema_bytes = env->GetByteArrayElements(schema_arr, 0);

  auto serialized_schema =
      std::make_shared<arrow::Buffer>((uint8_t*)schema_bytes, schema_len);
  arrow::ipc::DictionaryMemo in_memo;
  arrow::io::BufferReader buf_reader(serialized_schema);
  *schema = arrow::ipc::ReadSchema(&buf_reader, &in_memo).ValueOrDie();
  env->ReleaseByteArrayElements(schema_arr, schema_bytes, JNI_ABORT);

  return arrow::Status::OK();
}

arrow::Status MakeExprVector(JNIEnv* env, jbyteArray exprs_arr,
                             gandiva::ExpressionVector* expr_vector,
                             gandiva::FieldVector* ret_types) {
  exprs::ExpressionList exprs;
  jsize exprs_len = env->GetArrayLength(exprs_arr);
  jbyte* exprs_bytes = env->GetByteArrayElements(exprs_arr, 0);

  if (!ParseProtobuf(reinterpret_cast<uint8_t*>(exprs_bytes), exprs_len, &exprs)) {
    env->ReleaseByteArrayElements(exprs_arr, exprs_bytes, JNI_ABORT);
    return arrow::Status::UnknownError("Unable to parse");
  }

  // create Expression out of the list of exprs
  for (int i = 0; i < exprs.exprs_size(); i++) {
    gandiva::ExpressionPtr root = ProtoTypeToExpression(exprs.exprs(i));

    if (root == nullptr) {
      env->ReleaseByteArrayElements(exprs_arr, exprs_bytes, JNI_ABORT);
      return arrow::Status::UnknownError("Unable to construct expression object");
    }

    expr_vector->push_back(root);
    ret_types->push_back(root->result());
  }

  return arrow::Status::OK();
}

jbyteArray ToSchemaByteArray(JNIEnv* env, std::shared_ptr<arrow::Schema> schema) {
  arrow::Status status;
  std::shared_ptr<arrow::Buffer> buffer;
  status = arrow::ipc::SerializeSchema(*schema.get(), nullptr,
                                       arrow::default_memory_pool(), &buffer);
  if (!status.ok()) {
    std::string error_message =
        "Unable to convert schema to byte array, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jbyteArray out = env->NewByteArray(buffer->size());
  auto src = reinterpret_cast<const jbyte*>(buffer->data());
  env->SetByteArrayRegion(out, 0, buffer->size(), src);
  return out;
}
