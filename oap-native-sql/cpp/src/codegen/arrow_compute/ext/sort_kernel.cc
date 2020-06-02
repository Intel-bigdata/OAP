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
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/compute/expression.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/sort_to_indices.h>
#include <arrow/compute/logical_type.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <vector>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/item_iterator.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  SortArraysToIndices  ////////////////
class SortArraysToIndicesKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       std::vector<std::shared_ptr<arrow::Field>> key_field_list,
       std::shared_ptr<arrow::Schema> result_schema, bool nulls_first, bool asc)
      : ctx_(ctx), nulls_first_(nulls_first), asc_(asc) {
    for (auto field : key_field_list) {
      auto indices = result_schema->GetAllFieldIndices(field->name());
      if (indices.size() != 1) {
        std::cout << "[ERROR] SortArraysToIndicesKernel::Impl can't find key "
                  << field->ToString() << " from " << result_schema->ToString()
                  << std::endl;
        throw;
      }
      key_index_list_.push_back(indices[0]);
    }

    auto status = LoadJITFunction(key_field_list, result_schema, &sorter);
    if (!status.ok()) {
      std::cout << "LoadJITFunction failed, msg is " << status.message() << std::endl;
      throw;
    }
  }

  arrow::Status Evaluate(const ArrayList& in) {
    RETURN_NOT_OK(sorter->Evaluate(in));
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    RETURN_NOT_OK(sorter->MakeResultIterator(schema, out));
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) { return arrow::Status::OK(); }

 private:
  std::shared_ptr<CodeGenBase> sorter;
  arrow::compute::FunctionContext* ctx_;
  bool nulls_first_;
  bool asc_;
  std::vector<int> key_index_list_;
  class TypedSorterCodeGenImpl {
   public:
    TypedSorterCodeGenImpl(std::string indice, std::string dataTypeName, std::string name)
        : indice_(indice), dataTypeName_(dataTypeName), name_(name) {}
    std::string GetCachedVariablesDefine() {
      return "using DataType_" + indice_ + " = typename arrow::" + dataTypeName_ +
             ";\n"
             "using ArrayType_" +
             indice_ + " = typename arrow::TypeTraits<DataType_" + indice_ +
             ">::ArrayType;\n"
             "std::vector<std::shared_ptr<ArrayType_" +
             indice_ + ">> cached_" + indice_ + "_;\n";
    }
    std::string GetResultIterDefine() {
      return "cached_" + indice_ + "_ = cached_" + indice_ +
             ";\n"
             "std::unique_ptr<arrow::ArrayBuilder> builder_" +
             indice_ +
             ";\n"
             "arrow::MakeBuilder(ctx_->memory_pool(), data_type_" +
             indice_ + ", &builder_" + indice_ +
             ");\n"
             "builder_" +
             indice_ + "_.reset(arrow::internal::checked_cast<BuilderType_" + indice_ +
             "*>(builder_" + indice_ + ".release()));\n";
    }
    std::string GetFieldDefine() {
      return "arrow::field(\"" + name_ + "\", data_type_" + indice_ + ")";
    }
    std::string GetResultIterVariables() {
      return R"(
    using DataType_)" +
             indice_ + R"( = typename arrow::)" + dataTypeName_ + R"(;
    using ArrayType_)" +
             indice_ + R"( = typename arrow::TypeTraits<DataType_)" + indice_ +
             R"(>::ArrayType;
    using BuilderType_)" +
             indice_ + R"( = typename arrow::TypeTraits<DataType_)" + indice_ +
             R"(>::BuilderType;
    std::shared_ptr<arrow::DataType> data_type_)" +
             indice_ + R"( = arrow::TypeTraits<DataType_)" + indice_ +
             R"(>::type_singleton();
    std::vector<std::shared_ptr<ArrayType_)" +
             indice_ + R"(>> cached_)" + indice_ + R"(_;
    std::shared_ptr<BuilderType_)" +
             indice_ + R"(> builder_)" + indice_ + R"(_;
    )";
    }

   private:
    std::string indice_;
    std::string dataTypeName_;
    std::string name_;
  };

  arrow::Status LoadJITFunction(std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                                std::shared_ptr<arrow::Schema> result_schema,
                                std::shared_ptr<CodeGenBase>* out) {
    // generate ddl signature
    std::stringstream func_args_ss;
    func_args_ss << "[Sorter]" << (nulls_first_ ? "nulls_first" : "nulls_last") << "|"
                 << (asc_ ? "asc" : "desc");
    int i = 0;
    for (auto field : key_field_list) {
      func_args_ss << "[sort_key_" << i << "]" << field->ToString();
    }

    func_args_ss << "[schema]" << result_schema->ToString();

    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(func_args_ss.str());
    std::string signature = signature_ss.str();

    auto file_lock = FileSpinLock("/tmp");
    auto status = LoadLibrary(signature, ctx_, out);
    if (!status.ok()) {
      // process
      auto codes = ProduceCodes(result_schema);
      // compile codes
      RETURN_NOT_OK(CompileCodes(codes, signature));
      RETURN_NOT_OK(LoadLibrary(signature, ctx_, out));
    }
    FileSpinUnLock(file_lock);
    return arrow::Status::OK();
  }

  std::string ProduceCodes(std::shared_ptr<arrow::Schema> result_schema) {
    int indice = 0;
    std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list;
    for (auto field : result_schema->fields()) {
      auto codegen = std::make_shared<TypedSorterCodeGenImpl>(
          std::to_string(indice), GetTypeString(field->type()), field->name());
      shuffle_typed_codegen_list.push_back(codegen);
      indice++;
    }
    std::string cached_insert_str = GetCachedInsert(shuffle_typed_codegen_list.size());
    std::string comp_func_str = GetCompFunction(key_index_list_);

    std::string pre_sort_valid_str = GetPreSortValid();

    std::string pre_sort_null_str = GetPreSortNull();

    std::string sort_func_str = GetSortFunction();

    std::string make_result_iter_str =
        GetMakeResultIter(shuffle_typed_codegen_list.size());

    std::string cached_variables_define_str =
        GetCachedVariablesDefine(shuffle_typed_codegen_list);

    std::string result_iter_param_define_str =
        GetResultIterParamsDefine(shuffle_typed_codegen_list.size());

    std::string result_iter_define_str = GetResultIterDefine(shuffle_typed_codegen_list);

    std::string typed_build_str = GetTypedBuild(shuffle_typed_codegen_list.size());

    std::string result_variables_define_str =
        GetResultIterVariables(shuffle_typed_codegen_list);

    std::string typed_res_array_build_str =
        GetTypedResArrayBuild(shuffle_typed_codegen_list.size());

    std::string typed_res_array_str = GetTypedResArray(shuffle_typed_codegen_list.size());

    return BaseCodes() + R"(
class TypedSorterImpl : public CodeGenBase {
 public:
  TypedSorterImpl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}

  arrow::Status Evaluate(const ArrayList& in) override {
    num_batches_++;
    items_total_ += in[0]->length();
    nulls_total_ += in[0]->null_count();
    first_.push_back(in[0]);
    )" + cached_insert_str +
           R"(
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override {
    )" + comp_func_str +
           R"(
    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total_ * sizeof(ArrayItemIndex);
    RETURN_NOT_OK(arrow::AllocateBuffer(ctx_->memory_pool(), buf_size, &indices_buf));

    // start to partition not_null with null
    ArrayItemIndex* indices_begin =
        reinterpret_cast<ArrayItemIndex*>(indices_buf->mutable_data());
    ArrayItemIndex* indices_end = indices_begin + items_total_;

    int64_t indices_i = 0;
    int64_t indices_null = 0;

    // we should support nulls first and nulls last here
    // we should also support desc and asc here

    for (int array_id = 0; array_id < num_batches_; array_id++) {
      for (int64_t i = 0; i < first_[array_id]->length(); i++) {
        if (!first_[array_id]->IsNull(i)) {
          )" +
           pre_sort_valid_str +
           R"(
          indices_i++;
        } else {
          )" +
           pre_sort_null_str +
           R"(
          indices_null++;
        }
      }
    }
    )" + sort_func_str +
           R"(
    auto out_type = std::make_shared<arrow::FixedSizeBinaryType>(sizeof(ArrayItemIndex) /
                                                                 sizeof(int32_t));
    *out = std::make_shared<arrow::FixedSizeBinaryArray>(out_type, items_total_,
                                                         indices_buf);
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    std::shared_ptr<arrow::Array> indices_out;
    RETURN_NOT_OK(Finish(&indices_out));
    )" + make_result_iter_str +
           R"(
    return arrow::Status::OK();
  }

 private:
  )" + cached_variables_define_str +
           R"(
  std::vector<std::shared_ptr<arrow::Array>> first_;
  arrow::compute::FunctionContext* ctx_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  uint64_t nulls_total_ = 0;

  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::FunctionContext* ctx,
                       std::shared_ptr<arrow::Array> indices_in,
   )" + result_iter_param_define_str +
           R"(): ctx_(ctx), total_length_(indices_in->length()), indices_in_cache_(indices_in) {
     )" + result_iter_define_str +
           R"(
      indices_begin_ = (ArrayItemIndex*)indices_in->data()->buffers[1]->mutable_data();
    }

    std::string ToString() override { return "SortArraysToIndicesResultIterator"; }

    bool HasNext() override {
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      auto length = (total_length_ - offset_) > 4096 ? 4096 : (total_length_ - offset_);
      uint64_t count = 0;
      while (count < length) {
        auto item = indices_begin_ + offset_ + count++;
      )" + typed_build_str +
           R"(
      }
      offset_ += length;
      )" + typed_res_array_build_str +
           R"(
      *out = arrow::RecordBatch::Make(result_schema_, length, {)" +
           typed_res_array_str + R"(});
      return arrow::Status::OK();
    }

   private:
   )" + result_variables_define_str +
           R"(
    std::shared_ptr<arrow::Array> indices_in_cache_;
    uint64_t offset_ = 0;
    ArrayItemIndex* indices_begin_;
    const uint64_t total_length_;
    std::shared_ptr<arrow::Schema> result_schema_;
    arrow::compute::FunctionContext* ctx_;
  };
};

extern "C" void MakeCodeGen(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<CodeGenBase>* out) {
  *out = std::make_shared<TypedSorterImpl>(ctx);
}

    )";
  }
  std::string GetCachedInsert(int shuffle_size) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      ss << "cached_" << i << "_.push_back(std::dynamic_pointer_cast<ArrayType_" << i
         << ">(in[" << i << "]));" << std::endl;
    }
    return ss.str();
  }
  std::string GetCompFunction(std::vector<int> sort_key_index_list) {
    std::stringstream ss;
    ss << "auto comp = [this](ArrayItemIndex x, ArrayItemIndex y) {"
       << GetCompFunction_(0, sort_key_index_list) << "};";
    return ss.str();
  }
  std::string GetCompFunction_(int cur_key_index, std::vector<int> sort_key_index_list) {
    std::string comp_str;
    auto cur_key_id = sort_key_index_list[cur_key_index];
    if (asc_) {
      std::stringstream ss;
      ss << "return cached_" << cur_key_id << "_[x.array_id]->GetView(x.id) < cached_"
         << cur_key_id << "_[y.array_id]->GetView(y.id);\n";
      comp_str = ss.str();
    } else {
      std::stringstream ss;
      ss << "return cached_" << cur_key_id << "_[x.array_id]->GetView(x.id) > cached_"
         << cur_key_id << "_[y.array_id]->GetView(y.id);\n";
      comp_str = ss.str();
    }
    if ((cur_key_index + 1) < sort_key_index_list.size()) {
      std::stringstream ss;
      ss << "if (cached_" << cur_key_id << "_[x.array_id]->GetView(x.id) == cached_"
         << cur_key_id << "_[y.array_id]->GetView(y.id)) {"
         << GetCompFunction_(cur_key_index + 1, sort_key_index_list) << "} else { "
         << comp_str << "}";
      return ss.str();
    } else {
      return comp_str;
    }
  }
  std::string GetPreSortValid() {
    if (nulls_first_) {
      return R"(
    (indices_begin + nulls_total_ + indices_i)->array_id = array_id;
    (indices_begin + nulls_total_ + indices_i)->id = i;)";

    } else {
      return R"(
    (indices_begin + indices_i)->array_id = array_id;
    (indices_begin + indices_i)->id = i;)";
    }
  }
  std::string GetPreSortNull() {
    if (nulls_first_) {
      return R"(
    (indices_begin + indices_null)->array_id = array_id;
    (indices_begin + indices_null)->id = i;)";
    } else {
      return R"(
    (indices_end - nulls_total_ + indices_null)->array_id = array_id;
    (indices_end - nulls_total_ + indices_null)->id = i;)";
    }
  }
  std::string GetSortFunction() {
    if (nulls_first_) {
      return "std::sort(indices_begin + nulls_total_, indices_begin + "
             "items_total_, "
             "comp);";
    } else {
      return "std::sort(indices_begin, indices_begin + items_total_ - "
             "nulls_total_, comp);";
    }
  }
  std::string GetMakeResultIter(int shuffle_size) {
    std::stringstream ss;
    std::stringstream params_ss;
    for (int i = 0; i < shuffle_size; i++) {
      if (i + 1 < shuffle_size) {
        params_ss << "cached_" << i << "_,";
      } else {
        params_ss << "cached_" << i << "_";
      }
    }
    auto params = params_ss.str();
    ss << "*out = std::make_shared<SorterResultIterator>(ctx_, indices_out, " << params
       << ");";
    return ss.str();
  }
  std::string GetCachedVariablesDefine(
      std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list) {
    std::stringstream ss;
    for (auto codegen : shuffle_typed_codegen_list) {
      ss << codegen->GetCachedVariablesDefine() << std::endl;
    }
    return ss.str();
  }
  std::string GetResultIterParamsDefine(int shuffle_size) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      if (i + 1 < shuffle_size) {
        ss << "std::vector<std::shared_ptr<ArrayType_" << i << ">> cached_" << i << ","
           << std::endl;
      } else {
        ss << "std::vector<std::shared_ptr<ArrayType_" << i << ">> cached_" << i;
      }
    }
    return ss.str();
  }
  std::string GetResultIterDefine(
      std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list) {
    std::stringstream ss;
    std::stringstream field_define_ss;
    for (auto codegen : shuffle_typed_codegen_list) {
      ss << codegen->GetResultIterDefine() << std::endl;
      if (codegen != *(shuffle_typed_codegen_list.end() - 1)) {
        field_define_ss << codegen->GetFieldDefine() << ",";
      } else {
        field_define_ss << codegen->GetFieldDefine();
      }
    }
    ss << "result_schema_ = arrow::schema({" << field_define_ss.str() << "});\n"
       << std::endl;
    return ss.str();
  }
  std::string GetTypedBuild(int shuffle_size) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      ss << "if (!cached_" << i << "_[item->array_id]->IsNull(item->id)) {\n"
         << "  RETURN_NOT_OK(builder_" << i << "_->Append(cached_" << i
         << "_[item->array_id]->GetView(item->id)));\n"
         << "} else {\n"
         << "  RETURN_NOT_OK(builder_" << i << "_->AppendNull());\n"
         << "}" << std::endl;
    }
    return ss.str();
  }
  std::string GetTypedResArrayBuild(int shuffle_size) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      ss << "std::shared_ptr<arrow::Array> out_" << i << ";\n"
         << "RETURN_NOT_OK(builder_" << i << "_->Finish(&out_" << i << "));\n"
         << "builder_" << i << "_->Reset();" << std::endl;
    }
    return ss.str();
  }
  std::string GetTypedResArray(int shuffle_size) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      if (i + 1 < shuffle_size) {
        ss << "out_" << i << ", ";
      } else {
        ss << "out_" << i;
      }
    }
    return ss.str();
  }
  std::string GetResultIterVariables(
      std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list) {
    std::stringstream ss;
    for (auto codegen : shuffle_typed_codegen_list) {
      ss << codegen->GetResultIterVariables() << std::endl;
    }
    return ss.str();
  }
};

arrow::Status SortArraysToIndicesKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::shared_ptr<arrow::Schema> result_schema, std::shared_ptr<KernalBase>* out,
    bool nulls_first, bool asc) {
  *out = std::make_shared<SortArraysToIndicesKernel>(ctx, key_field_list, result_schema,
                                                     nulls_first, asc);
  return arrow::Status::OK();
}

SortArraysToIndicesKernel::SortArraysToIndicesKernel(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::shared_ptr<arrow::Schema> result_schema, bool nulls_first, bool asc) {
  impl_.reset(new Impl(ctx, key_field_list, result_schema, nulls_first, asc));
  kernel_name_ = "SortArraysToIndicesKernelKernel";
}
#undef PROCESS_SUPPORTED_TYPES

arrow::Status SortArraysToIndicesKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status SortArraysToIndicesKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
