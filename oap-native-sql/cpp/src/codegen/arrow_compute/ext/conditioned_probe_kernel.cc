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

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/context.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/hash.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <arrow/visitor_inline.h>
#include <dlfcn.h>
#include <gandiva/configuration.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <unordered_map>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "codegen/arrow_compute/ext/codegen_node_visitor_v2.h"
#include "codegen/arrow_compute/ext/conditioner.h"
#include "codegen/arrow_compute/ext/item_iterator.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/shuffle_v2_action.h"
#include "third_party/arrow/utils/hashing.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  ConditionedProbeArrays  ////////////////
class ConditionedProbeArraysKernel::Impl {
 public:
  Impl() {}
  virtual ~Impl() {}
  virtual arrow::Status Evaluate(const ArrayList& in) {
    return arrow::Status::NotImplemented(
        "ConditionedProbeArraysKernel::Impl Evaluate is abstract");
  }  // namespace extra
  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "ConditionedProbeArraysKernel::Impl MakeResultIterator is abstract");
  }
};  // namespace arrowcompute

template <typename InType, typename MemoTableType>
class ConditionedProbeArraysTypedImpl : public ConditionedProbeArraysKernel::Impl {
 public:
  ConditionedProbeArraysTypedImpl(
      arrow::compute::FunctionContext* ctx,
      std::vector<std::shared_ptr<arrow::Field>> left_key_list,
      std::vector<std::shared_ptr<arrow::Field>> right_key_list,
      std::shared_ptr<gandiva::Node> func_node, int join_type,
      std::vector<std::shared_ptr<arrow::Field>> left_field_list,
      std::vector<std::shared_ptr<arrow::Field>> right_field_list)
      : ctx_(ctx),
        left_key_list_(left_key_list),
        right_key_list_(right_key_list),
        func_node_(func_node),
        join_type_(join_type),
        left_field_list_(left_field_list),
        right_field_list_(right_field_list) {
    hash_table_ = std::make_shared<MemoTableType>(ctx_->memory_pool());
    for (auto key_field : left_key_list) {
      int i = 0;
      for (auto field : left_field_list) {
        if (key_field->name() == field->name()) {
          break;
        }
        i++;
      }
      left_key_indices_.push_back(i);
    }
    for (auto key_field : right_key_list) {
      int i = 0;
      for (auto field : right_field_list) {
        if (key_field->name() == field->name()) {
          break;
        }
        i++;
      }
      right_key_indices_.push_back(i);
    }
    if (left_key_list.size() > 1) {
      std::vector<std::shared_ptr<arrow::DataType>> type_list;
      for (auto key_field : left_key_list) {
        type_list.push_back(key_field->type());
      }
      extra::HashAggrArrayKernel::Make(ctx_, type_list, &concat_kernel_);
    }
    for (auto field : left_field_list) {
      std::shared_ptr<ItemIterator> iter;
      MakeArrayListItemIterator(field->type(), &iter);
      input_iterator_list_.push_back(iter);
    }
    for (auto field : right_field_list) {
      std::shared_ptr<ItemIterator> iter;
      MakeArrayItemIterator(field->type(), &iter);
      input_iterator_list_.push_back(iter);
    }
    input_cache_.resize(left_field_list.size());

    // This Function suppose to return a lambda function for later ResultIterator
    auto start = std::chrono::steady_clock::now();
    if (func_node_) {
      LoadJITFunction(func_node_, left_field_list_, right_field_list_, &conditioner_);
    }
    auto end = std::chrono::steady_clock::now();
    std::cout
        << "Code Generation took "
        << (std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() /
            1000)
        << " ms." << std::endl;
  }

  arrow::Status Evaluate(const ArrayList& in_arr_list) override {
    if (in_arr_list.size() != input_cache_.size()) {
      return arrow::Status::Invalid(
          "ConditionedShuffleArrayListKernel input arrayList size does not match numCols "
          "in cache, which are ",
          in_arr_list.size(), " and ", input_cache_.size());
    }
    // we need to convert std::vector<Batch> to std::vector<ArrayList>
    for (int col_id = 0; col_id < input_cache_.size(); col_id++) {
      input_cache_[col_id].push_back(in_arr_list[col_id]);
    }

    // do concat_join if necessary
    std::shared_ptr<arrow::Array> in;
    if (concat_kernel_) {
      ArrayList concat_kernel_arr_list;
      for (auto i : left_key_indices_) {
        concat_kernel_arr_list.push_back(in_arr_list[i]);
      }
      RETURN_NOT_OK(concat_kernel_->Evaluate(concat_kernel_arr_list, &in));
    } else {
      in = in_arr_list[left_key_indices_[0]];
    }

    // we should put items into hashmap
    auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
    auto insert_on_found = [this](int32_t i) {
      left_table_size_++;
      memo_index_to_arrayid_[i].emplace_back(cur_array_id_, cur_id_);
    };
    auto insert_on_not_found = [this](int32_t i) {
      left_table_size_++;
      memo_index_to_arrayid_.push_back({ArrayItemIndex(cur_array_id_, cur_id_)});
    };

    cur_id_ = 0;
    int memo_index = 0;
    if (typed_array->null_count() == 0) {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        hash_table_->GetOrInsert(typed_array->GetView(cur_id_), insert_on_found,
                                 insert_on_not_found, &memo_index);
      }
    } else {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        if (typed_array->IsNull(cur_id_)) {
          hash_table_->GetOrInsertNull(insert_on_found, insert_on_not_found);
        } else {
          hash_table_->GetOrInsert(typed_array->GetView(cur_id_), insert_on_found,
                                   insert_on_not_found, &memo_index);
        }
      }
    }
    cur_array_id_++;
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    std::function<arrow::Status(const std::shared_ptr<arrow::Array>& in,
                                std::function<bool(ArrayItemIndex, int)> ConditionCheck,
                                std::shared_ptr<arrow::RecordBatch>* out)>
        eval_func;
    // prepare process next function
    std::cout << "HashMap lenghth is " << memo_index_to_arrayid_.size()
              << ", total stored items count is " << left_table_size_ << std::endl;
    switch (join_type_) {
      case 0: { /*Inner Join*/
        eval_func = [this, schema](
                        const std::shared_ptr<arrow::Array>& in,
                        std::function<bool(ArrayItemIndex, int)> ConditionCheck,
                        std::shared_ptr<arrow::RecordBatch>* out) {
          // prepare
          std::unique_ptr<arrow::FixedSizeBinaryBuilder> left_indices_builder;
          auto left_array_type = arrow::fixed_size_binary(sizeof(ArrayItemIndex));
          left_indices_builder.reset(
              new arrow::FixedSizeBinaryBuilder(left_array_type, ctx_->memory_pool()));

          std::unique_ptr<arrow::UInt32Builder> right_indices_builder;
          right_indices_builder.reset(
              new arrow::UInt32Builder(arrow::uint32(), ctx_->memory_pool()));

          auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
          for (int i = 0; i < typed_array->length(); i++) {
            if (!typed_array->IsNull(i)) {
              auto index = hash_table_->Get(typed_array->GetView(i));
              if (index != -1) {
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              }
            }
          }
          // create buffer and null_vector to FixedSizeBinaryArray
          std::shared_ptr<arrow::Array> left_arr_out;
          std::shared_ptr<arrow::Array> right_arr_out;
          RETURN_NOT_OK(left_indices_builder->Finish(&left_arr_out));
          RETURN_NOT_OK(right_indices_builder->Finish(&right_arr_out));
          auto result_schema =
              arrow::schema({arrow::field("left_indices", left_array_type),
                             arrow::field("right_indices", arrow::uint32())});
          *out = arrow::RecordBatch::Make(result_schema, right_arr_out->length(),
                                          {left_arr_out, right_arr_out});
          return arrow::Status::OK();
        };
      } break;
      case 1: { /*Outer Join*/
        eval_func = [this, schema](
                        const std::shared_ptr<arrow::Array>& in,
                        std::function<bool(ArrayItemIndex, int)> ConditionCheck,
                        std::shared_ptr<arrow::RecordBatch>* out) {
          std::unique_ptr<arrow::FixedSizeBinaryBuilder> left_indices_builder;
          auto left_array_type = arrow::fixed_size_binary(sizeof(ArrayItemIndex));
          left_indices_builder.reset(
              new arrow::FixedSizeBinaryBuilder(left_array_type, ctx_->memory_pool()));

          std::unique_ptr<arrow::UInt32Builder> right_indices_builder;
          right_indices_builder.reset(
              new arrow::UInt32Builder(arrow::uint32(), ctx_->memory_pool()));

          auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
          for (int i = 0; i < typed_array->length(); i++) {
            if (typed_array->IsNull(i)) {
              auto index = hash_table_->GetNull();
              if (index == -1) {
                RETURN_NOT_OK(left_indices_builder->AppendNull());
                RETURN_NOT_OK(right_indices_builder->Append(i));
              } else {
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              }
            } else {
              auto index = hash_table_->Get(typed_array->GetView(i));
              if (index == -1) {
                RETURN_NOT_OK(left_indices_builder->AppendNull());
                RETURN_NOT_OK(right_indices_builder->Append(i));
              } else {
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              }
            }
          }
          // create buffer and null_vector to FixedSizeBinaryArray
          std::shared_ptr<arrow::Array> left_arr_out;
          std::shared_ptr<arrow::Array> right_arr_out;
          RETURN_NOT_OK(left_indices_builder->Finish(&left_arr_out));
          RETURN_NOT_OK(right_indices_builder->Finish(&right_arr_out));
          auto result_schema =
              arrow::schema({arrow::field("left_indices", left_array_type),
                             arrow::field("right_indices", arrow::uint32())});
          *out = arrow::RecordBatch::Make(result_schema, right_arr_out->length(),
                                          {left_arr_out, right_arr_out});

          return arrow::Status::OK();
        };
      } break;
      case 2: { /*Anti Join*/
        eval_func = [this, schema](
                        const std::shared_ptr<arrow::Array>& in,
                        std::function<bool(ArrayItemIndex, int)> ConditionCheck,
                        std::shared_ptr<arrow::RecordBatch>* out) {
          std::unique_ptr<arrow::FixedSizeBinaryBuilder> left_indices_builder;
          auto left_array_type = arrow::fixed_size_binary(sizeof(ArrayItemIndex));
          left_indices_builder.reset(
              new arrow::FixedSizeBinaryBuilder(left_array_type, ctx_->memory_pool()));

          std::unique_ptr<arrow::UInt32Builder> right_indices_builder;
          right_indices_builder.reset(
              new arrow::UInt32Builder(arrow::uint32(), ctx_->memory_pool()));

          auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
          for (int i = 0; i < typed_array->length(); i++) {
            if (!typed_array->IsNull(i)) {
              auto index = hash_table_->Get(typed_array->GetView(i));
              if (index == -1) {
                RETURN_NOT_OK(left_indices_builder->AppendNull());
                RETURN_NOT_OK(right_indices_builder->Append(i));
              } else {
                bool found = false;
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    found = true;
                    break;
                  }
                }
                if (!found) {
                  RETURN_NOT_OK(left_indices_builder->AppendNull());
                  RETURN_NOT_OK(right_indices_builder->Append(i));
                }
              }
            } else {
              auto index = hash_table_->GetNull();
              if (index == -1) {
                RETURN_NOT_OK(left_indices_builder->AppendNull());
                RETURN_NOT_OK(right_indices_builder->Append(i));
              } else {
                bool found = false;
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    found = true;
                    break;
                  }
                }
                if (!found) {
                  RETURN_NOT_OK(left_indices_builder->AppendNull());
                  RETURN_NOT_OK(right_indices_builder->Append(i));
                }
              }
            }
          }

          // create buffer and null_vector to FixedSizeBinaryArray
          std::shared_ptr<arrow::Array> left_arr_out;
          std::shared_ptr<arrow::Array> right_arr_out;
          RETURN_NOT_OK(left_indices_builder->Finish(&left_arr_out));
          RETURN_NOT_OK(right_indices_builder->Finish(&right_arr_out));
          auto result_schema =
              arrow::schema({arrow::field("left_indices", left_array_type),
                             arrow::field("right_indices", arrow::uint32())});
          *out = arrow::RecordBatch::Make(result_schema, right_arr_out->length(),
                                          {left_arr_out, right_arr_out});
          return arrow::Status::OK();
        };
      } break;
      case 3: { /*Semi Join*/
        eval_func = [this, schema](
                        const std::shared_ptr<arrow::Array>& in,
                        std::function<bool(ArrayItemIndex, int)> ConditionCheck,
                        std::shared_ptr<arrow::RecordBatch>* out) {
          // prepare
          std::unique_ptr<arrow::FixedSizeBinaryBuilder> left_indices_builder;
          auto left_array_type = arrow::fixed_size_binary(sizeof(ArrayItemIndex));
          left_indices_builder.reset(
              new arrow::FixedSizeBinaryBuilder(left_array_type, ctx_->memory_pool()));

          std::unique_ptr<arrow::UInt32Builder> right_indices_builder;
          right_indices_builder.reset(
              new arrow::UInt32Builder(arrow::uint32(), ctx_->memory_pool()));

          auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
          for (int i = 0; i < typed_array->length(); i++) {
            if (!typed_array->IsNull(i)) {
              auto index = hash_table_->Get(typed_array->GetView(i));
              if (index != -1) {
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    RETURN_NOT_OK(left_indices_builder->AppendNull());
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                    break;
                  }
                }
              }
            }
          }
          // create buffer and null_vector to FixedSizeBinaryArray
          std::shared_ptr<arrow::Array> left_arr_out;
          std::shared_ptr<arrow::Array> right_arr_out;
          RETURN_NOT_OK(left_indices_builder->Finish(&left_arr_out));
          RETURN_NOT_OK(right_indices_builder->Finish(&right_arr_out));
          auto result_schema =
              arrow::schema({arrow::field("left_indices", left_array_type),
                             arrow::field("right_indices", arrow::uint32())});
          *out = arrow::RecordBatch::Make(result_schema, right_arr_out->length(),
                                          {left_arr_out, right_arr_out});
          return arrow::Status::OK();
        };
      } break;
      default:
        return arrow::Status::Invalid(
            "ConditionedProbeArraysTypedImpl only support join type: InnerJoin, "
            "RightJoin");
    }

    *out = std::make_shared<ConditionedProbeArraysResultIterator>(
        ctx_, conditioner_, right_key_indices_, concat_kernel_, input_iterator_list_,
        input_cache_, eval_func);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;

  uint64_t left_table_size_ = 0;
  std::shared_ptr<gandiva::Node> func_node_;
  std::vector<int> left_key_indices_;
  std::vector<int> right_key_indices_;
  std::vector<std::shared_ptr<arrow::Field>> left_key_list_;
  std::vector<std::shared_ptr<arrow::Field>> right_key_list_;
  std::vector<std::shared_ptr<arrow::Field>> left_field_list_;
  std::vector<std::shared_ptr<arrow::Field>> right_field_list_;
  std::vector<std::shared_ptr<ItemIterator>> input_iterator_list_;
  int join_type_;
  std::shared_ptr<extra::KernalBase> concat_kernel_;
  std::vector<ArrayList> input_cache_;
  std::shared_ptr<arrow::DataType> out_type_;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<MemoTableType> hash_table_;
  std::vector<std::vector<ArrayItemIndex>> memo_index_to_arrayid_;
  std::shared_ptr<ConditionerBase> conditioner_;

  uint64_t cur_array_id_ = 0;
  uint64_t cur_id_ = 0;

  arrow::Status LoadJITFunction(
      std::shared_ptr<gandiva::Node> func_node,
      std::vector<std::shared_ptr<arrow::Field>> left_field_list,
      std::vector<std::shared_ptr<arrow::Field>> right_field_list,
      std::shared_ptr<ConditionerBase>* out) {
    // generate ddl signature
    std::stringstream func_args_ss;
    func_args_ss << func_node->ToString();
    for (auto field : left_field_list) {
      func_args_ss << field->ToString();
    }
    for (auto field : right_field_list) {
      func_args_ss << field->ToString();
    }

    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(func_args_ss.str());
    std::string signature = signature_ss.str();
    std::cout << "LoadJITFunction signature is " << signature << std::endl;

    auto file_lock = FileSpinLock("/tmp");
    std::cout << "GetFileLock" << std::endl;
    auto status = LoadLibrary(signature, out);
    if (!status.ok()) {
      // process
      auto codes = ProduceCodes(func_node_, left_field_list_, right_field_list_);
      // compile codes
      RETURN_NOT_OK(CompileCodes(codes, signature));
      RETURN_NOT_OK(LoadLibrary(signature, out));
    }
    FileSpinUnLock(file_lock);
    std::cout << "ReleaseFileLock" << std::endl;
    return arrow::Status::OK();
  }

  std::string ProduceCodes(std::shared_ptr<gandiva::Node> func_node,
                           std::vector<std::shared_ptr<arrow::Field>> left_field_list,
                           std::vector<std::shared_ptr<arrow::Field>> right_field_list) {
    // CodeGen
    std::stringstream codes_ss;
    codes_ss << R"(#include <arrow/type.h>
#include <algorithm>
#define int8 int8_t
#define int16 int16_t
#define int32 int32_t
#define int64 int64_t
#define uint8 uint8_t
#define uint16 uint16_t
#define uint32 uint32_t
#define uint64 uint64_t

struct ArrayItemIndex {
  uint64_t id = 0;
  uint64_t array_id = 0;
  ArrayItemIndex(uint64_t array_id, uint64_t id) : array_id(array_id), id(id) {}
};

class ConditionerBase {
public:
  virtual arrow::Status Submit(
      std::vector<std::function<bool(ArrayItemIndex)>> left_is_null_func_list,
      std::vector<std::function<void *(ArrayItemIndex)>> left_get_func_list,
      std::vector<std::function<bool(int)>> right_is_null_func_list,
      std::vector<std::function<void *(int)>> right_get_func_list,
      std::function<bool(ArrayItemIndex, int)> *out) {
    return arrow::Status::NotImplemented(
        "ConditionerBase Submit is an abstract interface.");
  }
};

class Conditioner : public ConditionerBase {
public:
  arrow::Status Submit(
      std::vector<std::function<bool(ArrayItemIndex)>> left_is_null_func_list,
      std::vector<std::function<void*(ArrayItemIndex)>> left_get_func_list,
      std::vector<std::function<bool(int)>> right_is_null_func_list,
      std::vector<std::function<void*(int)>> right_get_func_list,
      std::function<bool(ArrayItemIndex, int)>* out) override {
    left_is_null_func_list_ = left_is_null_func_list;
    left_get_func_list_ = left_get_func_list;
    right_is_null_func_list_ = right_is_null_func_list;
    right_get_func_list_ = right_get_func_list;
    *out = [this](ArrayItemIndex left_index, int right_index) {
)";

    std::shared_ptr<CodeGenNodeVisitorV2> func_node_visitor;
    int func_count = 0;
    MakeCodeGenNodeVisitorV2(func_node, {left_field_list, right_field_list}, &func_count,
                             &codes_ss, &func_node_visitor);
    codes_ss << "      return (" << func_node_visitor->GetResult() << ");" << std::endl;
    codes_ss << R"(
    };
    return arrow::Status::OK();
  }
private:
  std::vector<std::function<bool(ArrayItemIndex)>> left_is_null_func_list_;
  std::vector<std::function<void *(ArrayItemIndex)>> left_get_func_list_;
  std::vector<std::function<bool(int)>> right_is_null_func_list_;
  std::vector<std::function<void *(int)>> right_get_func_list_;
};

extern "C" void MakeConditioner(std::shared_ptr<ConditionerBase> *out) {
  *out = std::make_shared<Conditioner>();
})";
    return codes_ss.str();
  }

  int FileSpinLock(std::string path) {
    std::string lockfile = path + "/nativesql_compile.lock";

    auto fd = open(lockfile.c_str(), O_CREAT, S_IRWXU|S_IRWXG);
    flock(fd, LOCK_EX);

    return fd;
  }

  void FileSpinUnLock(int fd) {
    flock(fd, LOCK_UN);
    close(fd);
  }

  arrow::Status CompileCodes(std::string codes, std::string signature) {
    // temporary cpp/library output files
    srand(time(NULL));
    std::string outpath = "/tmp";
    std::string prefix = "/spark-columnar-plugin-codegen-";
    std::string cppfile = outpath + prefix + signature + ".cc";
    std::string libfile = outpath + prefix + signature + ".so";
    std::string logfile = outpath + prefix + signature + ".log";
    std::ofstream out(cppfile.c_str(), std::ofstream::out);

    // output code to file
    if (out.bad()) {
      std::cout << "cannot open " << cppfile << std::endl;
      exit(EXIT_FAILURE);
    }
    out << codes;
    out.flush();
    out.close();

    // compile the code
    std::string cmd = "gcc -std=c++11 -Wall -Wextra " + cppfile + " -o " + libfile +
                      " -O3 -shared -fPIC -larrow 2> " + logfile;
    int ret = system(cmd.c_str());
    if (WEXITSTATUS(ret) != EXIT_SUCCESS) {
      std::cout << "compilation failed, see " << logfile << std::endl;
      exit(EXIT_FAILURE);
    }

    struct stat tstat;
    ret = stat(libfile.c_str(), &tstat);
    if (ret == -1) {
      std::cout << "stat failed: " << strerror(errno) << std::endl;
      exit(EXIT_FAILURE);
    }

    return arrow::Status::OK();
  }

  arrow::Status LoadLibrary(std::string signature,
                            std::shared_ptr<ConditionerBase>* out) {
    std::string outpath = "/tmp";
    std::string prefix = "/spark-columnar-plugin-codegen-";
    std::string libfile = outpath + prefix + signature + ".so";
    // load dynamic library
    void* dynlib = dlopen(libfile.c_str(), RTLD_LAZY);
    if (!dynlib) {
      return arrow::Status::Invalid(libfile, " is not generated");
    }

    // loading symbol from library and assign to pointer
    // (to be cast to function pointer later)

    void (*MakeConditioner)(std::shared_ptr<ConditionerBase> * out);
    *(void**)(&MakeConditioner) = dlsym(dynlib, "MakeConditioner");
    const char* dlsym_error = dlerror();
    if (dlsym_error != NULL) {
      std::cerr << "error loading symbol:\n" << dlsym_error << std::endl;
      exit(EXIT_FAILURE);
    }

    MakeConditioner(out);
    return arrow::Status::OK();
  }

  class ConditionedProbeArraysResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    ConditionedProbeArraysResultIterator(
        arrow::compute::FunctionContext* ctx,
        std::shared_ptr<ConditionerBase> conditioner, std::vector<int> right_key_indices,
        std::shared_ptr<KernalBase> concat_kernel,
        std::vector<std::shared_ptr<ItemIterator>> input_iterator_list,
        std::vector<std::vector<std::shared_ptr<arrow::Array>>> left_input_cache,
        std::function<
            arrow::Status(const std::shared_ptr<arrow::Array>& in,
                          std::function<bool(ArrayItemIndex, int)> ConditionCheck,
                          std::shared_ptr<arrow::RecordBatch>* out)>
            eval_func)
        : eval_func_(eval_func),
          conditioner_(conditioner),
          ctx_(ctx),
          right_key_indices_(right_key_indices),
          concat_kernel_(concat_kernel),
          input_iterator_list_(input_iterator_list) {
      int i = 0;
      for (; i < left_input_cache.size(); i++) {
        std::function<bool(ArrayItemIndex)> is_null;
        std::function<void*(ArrayItemIndex)> get;
        input_iterator_list_[i]->Submit(left_input_cache[i], &is_null, &get);
        left_is_null_func_list_.push_back(is_null);
        left_get_func_list_.push_back(get);
      }
      right_id_ = i;
      condition_check_ = [this](ArrayItemIndex left_index, int right_index) {
        return true;
      };
    }

    std::string ToString() override { return "ConditionedProbeArraysResultIterator"; }

    arrow::Status ProcessAndCacheOne(
        ArrayList in, const std::shared_ptr<arrow::Array>& selection) override {
      // key preparation
      std::shared_ptr<arrow::Array> in_arr;
      if (right_key_indices_.size() > 1) {
        ArrayList in_arr_list;
        for (auto i : right_key_indices_) {
          in_arr_list.push_back(in[i]);
        }
        RETURN_NOT_OK(concat_kernel_->Evaluate(in_arr_list, &in_arr));
      } else {
        in_arr = in[right_key_indices_[0]];
      }
      // condition lambda preparation
      if (conditioner_) {
        std::vector<std::function<bool(int)>> right_is_null_func_list;
        std::vector<std::function<void*(int)>> right_get_func_list;
        for (int i = 0; i < in.size(); i++) {
          auto arr = in[i];
          std::function<bool(int)> is_null;
          std::function<void*(int)> get;
          input_iterator_list_[i + right_id_]->Submit(arr, &is_null, &get);
          right_is_null_func_list.push_back(is_null);
          right_get_func_list.push_back(get);
        }
        conditioner_->Submit(left_is_null_func_list_, left_get_func_list_,
                             right_is_null_func_list, right_get_func_list,
                             &condition_check_);
      }
      RETURN_NOT_OK(eval_func_(in_arr, condition_check_, &out_cache_));
      return arrow::Status::OK();
    }

    arrow::Status GetResult(std::shared_ptr<arrow::RecordBatch>* out) {
      *out = out_cache_;
      return arrow::Status::OK();
    }

   private:
    std::function<arrow::Status(const std::shared_ptr<arrow::Array>& in,
                                std::function<bool(ArrayItemIndex, int)> ConditionCheck,
                                std::shared_ptr<arrow::RecordBatch>* out)>
        eval_func_;
    int right_id_;
    std::shared_ptr<ConditionerBase> conditioner_;
    std::function<bool(ArrayItemIndex, int)> condition_check_;
    std::vector<int> right_key_indices_;
    std::shared_ptr<arrow::RecordBatch> out_cache_;
    arrow::compute::FunctionContext* ctx_;
    std::shared_ptr<extra::KernalBase> concat_kernel_;
    std::vector<std::shared_ptr<ItemIterator>> input_iterator_list_;
    std::vector<std::function<bool(ArrayItemIndex)>> left_is_null_func_list_;
    std::vector<std::function<void*(ArrayItemIndex)>> left_get_func_list_;
  };
};  // namespace extra

arrow::Status ConditionedProbeArraysKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> left_key_list,
    std::vector<std::shared_ptr<arrow::Field>> right_key_list,
    std::shared_ptr<gandiva::Node> func_node, int join_type,
    std::vector<std::shared_ptr<arrow::Field>> left_field_list,
    std::vector<std::shared_ptr<arrow::Field>> right_field_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ConditionedProbeArraysKernel>(
      ctx, left_key_list, right_key_list, func_node, join_type, left_field_list,
      right_field_list);
  return arrow::Status::OK();
}

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::Time32Type)             \
  PROCESS(arrow::Time64Type)             \
  PROCESS(arrow::TimestampType)          \
  PROCESS(arrow::BinaryType)             \
  PROCESS(arrow::StringType)             \
  PROCESS(arrow::FixedSizeBinaryType)    \
  PROCESS(arrow::Decimal128Type)
ConditionedProbeArraysKernel::ConditionedProbeArraysKernel(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> left_key_list,
    std::vector<std::shared_ptr<arrow::Field>> right_key_list,
    std::shared_ptr<gandiva::Node> func_node, int join_type,
    std::vector<std::shared_ptr<arrow::Field>> left_field_list,
    std::vector<std::shared_ptr<arrow::Field>> right_field_list) {
  auto type = left_key_list[0]->type();
  if (left_key_list.size() > 1) {
    type = arrow::int64();
  }
  switch (type->id()) {
#define PROCESS(InType)                                                                \
  case InType::type_id: {                                                              \
    using MemoTableType = typename arrow::internal::HashTraits<InType>::MemoTableType; \
    impl_.reset(new ConditionedProbeArraysTypedImpl<InType, MemoTableType>(            \
        ctx, left_key_list, right_key_list, func_node, join_type, left_field_list,     \
        right_field_list));                                                            \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  kernel_name_ = "ConditionedProbeArraysKernel";
}
#undef PROCESS_SUPPORTED_TYPES

arrow::Status ConditionedProbeArraysKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status ConditionedProbeArraysKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
