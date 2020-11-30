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

#include <arrow/array/concatenate.h>
#include <arrow/compute/context.h>
#include <arrow/compute/kernels/sort_to_indices.h>
#include <arrow/compute/kernels/take.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <gandiva/projector.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <vector>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/typed_node_visitor.h"
#include "third_party/ska_sort.hpp"
#include "precompile/array.h"
#include "precompile/type.h"
#include "array_appender.h"
#include "utils/macros.h"

/**
                 The Overall Implementation of Sort Kernel
 * In general, there are three kenels to use when sorting for different data.
   They are SortInplaceKernel, SortOnekeyKernel and SortArraysToIndicesKernel.
 * If sorting for one non-string and non-bool col without payload, SortInplaceKernel 
   is used. In this kernel, if sorted data has no null value, ska_sort is used for 
   asc direciton, and std sort is used for desc direciton. If sorted data has null
   value, arrow sort is used. Data is partitioned to null, NaN (for double and 
   float only) and valid value before sort.
 * If sorting for one col with payload, and one string or bool col without payload,
   SortOnekeyKernel is used. In this kernel, ska_sort is used for asc direciton, 
   and std sort is used for desc direciton. Data is partitioned to null, NaN (for 
   double and float only) and valid value before sort.
 * If sorting for multiple cols, SortArraysToIndicesKernel is used. This kernel 
   will do codegen, and std sort is used.
 * Projection is supported in all the three kernels. If projection is required, 
   projection is completed before sort, and the projected cols are used to do 
   comparison.
   FIXME: 1. datatype change after projection is not supported in Inplace.
**/

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
using namespace sparkcolumnarplugin::precompile;

///////////////  SortArraysToIndices  ////////////////
class SortArraysToIndicesKernel::Impl {
 public:
  Impl() {}
  Impl(arrow::compute::FunctionContext* ctx,
       std::shared_ptr<arrow::Schema> result_schema,
       std::shared_ptr<gandiva::Projector> key_projector,
       std::vector<std::shared_ptr<arrow::DataType>> projected_types,
       std::vector<std::shared_ptr<arrow::Field>> key_field_list,
       std::vector<bool> sort_directions, 
       std::vector<bool> nulls_order,
       bool NaN_check)
      : ctx_(ctx), 
        result_schema_(result_schema), 
        key_projector_(key_projector), 
        key_field_list_(key_field_list),
        sort_directions_(sort_directions), 
        nulls_order_(nulls_order), 
        asc_(sort_directions[0]), 
        nulls_first_(nulls_order[0]),
        projected_types_(projected_types),
        NaN_check_(NaN_check) {
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
  }
  virtual ~Impl() {}
  virtual arrow::Status LoadJITFunction(
      std::vector<std::shared_ptr<arrow::Field>> key_field_list,
      std::shared_ptr<arrow::Schema> result_schema) {
    // generate ddl signature
    std::stringstream func_args_ss;
    func_args_ss << (key_projector_? "project" : "original");
    int col_num = sort_directions_.size();
    for (int i = 0; i < col_num; i++) {
        func_args_ss << "[Sorter]" << (nulls_order_[i] ? "nulls_first" : "nulls_last") << "|"
                     << (sort_directions_[i] ? "asc" : "desc");
    }
    for (auto i : key_index_list_) {
      auto field = result_schema->field(i);
      func_args_ss << "[sort_key_" << i << "]" << field->type()->ToString();
    }

    func_args_ss << "[schema]";
    for (auto field : result_schema->fields()) {
      func_args_ss << field->type()->ToString();
    }

#ifdef DEBUG
    std::cout << "func_args_ss is " << func_args_ss.str() << std::endl;
#endif

    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(func_args_ss.str());
    signature_ = signature_ss.str();

    auto file_lock = FileSpinLock();
    auto status = LoadLibrary(signature_, ctx_, &sorter);
    if (!status.ok()) {
      // process
      auto codes = ProduceCodes(result_schema);
      // compile codes
      RETURN_NOT_OK(CompileCodes(codes, signature_));
      RETURN_NOT_OK(LoadLibrary(signature_, ctx_, &sorter));
    }
    FileSpinUnLock(file_lock);
    return arrow::Status::OK();
  }

  virtual arrow::Status Evaluate(const ArrayList& in) {
    std::vector<std::shared_ptr<arrow::Array>> outputs;
    if (key_projector_) {
      auto length = in.size() > 0 ? in[0]->length() : 0;
      auto in_batch = arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(
          key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
    }
    RETURN_NOT_OK(sorter->Evaluate(in, outputs));
    return arrow::Status::OK();
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    RETURN_NOT_OK(sorter->MakeResultIterator(schema, out));
    return arrow::Status::OK();
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::OK();
  }
  std::string GetSignature() { return signature_; }

 protected:
  std::shared_ptr<CodeGenBase> sorter;
  arrow::compute::FunctionContext* ctx_;
  std::string signature_;
  std::vector<int> key_index_list_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  std::vector<std::shared_ptr<arrow::DataType>> projected_types_;
  std::vector<std::shared_ptr<arrow::Field>> key_field_list_;
  // true for asc, false for desc
  std::vector<bool> sort_directions_;
  // true for nulls_first, false for nulls_last
  std::vector<bool> nulls_order_;
  // keep the direction and nulls order for the first key
  bool nulls_first_;
  bool asc_;
  bool NaN_check_;

  class TypedSorterCodeGenImpl {
   public:
    TypedSorterCodeGenImpl(std::string indice, std::shared_ptr<arrow::DataType> data_type,
                           std::string name)
        : indice_(indice), data_type_(data_type), name_(name) {}
    std::string GetCachedVariablesDefine() {
      std::stringstream ss;
      ss << "using ArrayType_" << indice_ << " = " << GetTypeString(data_type_, "Array")
         << ";" << std::endl;
      ss << "std::vector<std::shared_ptr<ArrayType_" << indice_ << ">> cached_" << indice_
         << "_;" << std::endl;
      return ss.str();
    }
    std::string GetResultIterDefine() {
      std::stringstream ss;
      ss << "cached_" << indice_ << "_ = cached_" << indice_ << ";" << std::endl;
      ss << "builder_" + indice_ + "_ = std::make_shared<"
         << GetTypeString(data_type_, "Builder") << ">(ctx_->memory_pool());"
         << std::endl;
      return ss.str();
    }
    std::string GetFieldDefine() {
      return "arrow::field(\"" + name_ + "\", data_type_" + indice_ + ")";
    }
    std::string GetResultIterVariables() {
      std::stringstream ss;
      ss << "using ArrayType_" << indice_ << " = " + GetTypeString(data_type_, "Array")
         << ";" << std::endl;
      ss << "using BuilderType_" << indice_ << " = "
         << GetTypeString(data_type_, "Builder") << ";" << std::endl;
      ss << "std::vector<std::shared_ptr<ArrayType_" << indice_ << ">> cached_" << indice_
         << "_;" << std::endl;
      ss << "std::shared_ptr<BuilderType_" << indice_ << "> builder_" << indice_ << "_;"
         << std::endl;
      ss << "std::shared_ptr<arrow::DataType> data_type_" << indice_
         << " = arrow::" + GetArrowTypeDefString(data_type_) << ";" << std::endl;
      return ss.str();
    }

   private:
    std::string indice_;
    std::string name_;
    std::shared_ptr<arrow::DataType> data_type_;
  };

  virtual std::string ProduceCodes(std::shared_ptr<arrow::Schema> result_schema) {
    int indice = 0;
    std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list;
    for (auto field : result_schema->fields()) {
      auto codegen = std::make_shared<TypedSorterCodeGenImpl>(
          std::to_string(indice), field->type(), field->name());
      shuffle_typed_codegen_list.push_back(codegen);
      indice++;
    }
    std::string cached_insert_str = GetCachedInsert(shuffle_typed_codegen_list.size(),
                                                    projected_types_.size(),
                                                    key_projector_);
    std::string comp_func_str = GetCompFunction(key_index_list_, key_projector_,
                                                projected_types_, key_field_list_, 
                                                sort_directions_, nulls_order_);

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

    std::string projected_variables_str = GetProjectedVariables(key_projector_, 
                                                                projected_types_);
    
    std::string cur_col_str = GetCurCol(key_index_list_[0], key_projector_);

    std::string first_cmp_col_str = GetFirstCmpCol(key_index_list_[0], key_projector_);

    return BaseCodes() + R"(
#include <arrow/buffer.h>

#include <algorithm>
#include <cmath>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "precompile/builder.h"
#include "precompile/type.h"
#include "third_party/ska_sort.hpp"
using namespace sparkcolumnarplugin::precompile;

class TypedSorterImpl : public CodeGenBase {
 public:
  TypedSorterImpl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}

  arrow::Status Evaluate(const ArrayList& in, const ArrayList& projected_batch) override {
    num_batches_++;
    )" + cached_insert_str 
       + cur_col_str +
           R"(
    items_total_ += cur->length();
    nulls_total_ += cur->null_count();
    length_list_.push_back(cur->length());
    return arrow::Status::OK();
  }

  arrow::Status FinishInternal(std::shared_ptr<FixedSizeBinaryArray>* out) {
    // we should support nulls first and nulls last here
    // we should also support desc and asc here
    )" + comp_func_str +
           R"(
    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total_ * sizeof(ArrayItemIndexS);
    RETURN_NOT_OK(arrow::AllocateBuffer(ctx_->memory_pool(), buf_size, &indices_buf));

    ArrayItemIndexS* indices_begin =
        reinterpret_cast<ArrayItemIndexS*>(indices_buf->mutable_data());
    ArrayItemIndexS* indices_end = indices_begin + items_total_;

    int64_t indices_i = 0;
    for (int array_id = 0; array_id < num_batches_; array_id++) {
      for (int64_t i = 0; i < length_list_[array_id]; i++) {
        (indices_begin + indices_i)->array_id = array_id;
        (indices_begin + indices_i)->id = i;
        indices_i++;
      }
    }

    )" + sort_func_str +
           R"(
    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
    RETURN_NOT_OK(MakeFixedSizeBinaryType(sizeof(ArrayItemIndexS) / sizeof(int32_t), &out_type));
    RETURN_NOT_OK(MakeFixedSizeBinaryArray(out_type, items_total_, indices_buf, out));
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    std::shared_ptr<FixedSizeBinaryArray> indices_out;
    RETURN_NOT_OK(FinishInternal(&indices_out));
    )" + make_result_iter_str +
           R"(
    return arrow::Status::OK();
  }

 private:
  )" + cached_variables_define_str
     + projected_variables_str +
           R"(
  std::vector<int64_t> length_list_;
  arrow::compute::FunctionContext* ctx_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  uint64_t nulls_total_ = 0;

  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::FunctionContext* ctx,
                       std::shared_ptr<FixedSizeBinaryArray> indices_in,
   )" + result_iter_param_define_str +
           R"(): ctx_(ctx), total_length_(indices_in->length()), indices_in_cache_(indices_in) {
     )" + result_iter_define_str +
           R"(
      indices_begin_ = (ArrayItemIndexS*)indices_in->value_data();
    }

    std::string ToString() override { return "SortArraysToIndicesResultIterator"; }

    bool HasNext() override {
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      auto length = (total_length_ - offset_) > )" +
           std::to_string(GetBatchSize()) + R"( ? )" + std::to_string(GetBatchSize()) +
           R"( : (total_length_ - offset_);
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
    std::shared_ptr<FixedSizeBinaryArray> indices_in_cache_;
    uint64_t offset_ = 0;
    ArrayItemIndexS* indices_begin_;
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
  std::string GetCachedInsert(int shuffle_size, int projected_size,
                              const std::shared_ptr<gandiva::Projector>& key_projector) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      ss << "cached_" << i << "_.push_back(std::make_shared<ArrayType_" << i 
          << ">(in[" << i << "]));" << std::endl;
    }
    if (key_projector) {
      for (int i = 0; i < projected_size; i++) {
        ss << "projected_" << i << "_.push_back(std::make_shared<ProjectedType_" << i 
           << ">(projected_batch[" << i << "]));" << std::endl;
      }
    }
    return ss.str();
  }
  std::string GetCompFunction(const std::vector<int>& sort_key_index_list, 
                              const std::shared_ptr<gandiva::Projector>& key_projector,
                              const std::vector<std::shared_ptr<arrow::DataType>>& projected_types,
                              const std::vector<std::shared_ptr<arrow::Field>>& key_field_list,
                              const std::vector<bool>& sort_directions, 
                              const std::vector<bool>& nulls_order) {
    std::stringstream ss;
    bool projected;
    if (key_projector) {
      projected = true;
    } else {
      projected = false;
    }
    ss << "auto comp = [this](ArrayItemIndexS x, ArrayItemIndexS y) {"
       << GetCompFunction_(0, projected, sort_key_index_list, key_field_list, 
                           projected_types, sort_directions, nulls_order) << "};";
    return ss.str();
  }
  std::string GetCompFunction_(int cur_key_index, bool projected, 
                               const std::vector<int>& sort_key_index_list,
                               const std::vector<std::shared_ptr<arrow::Field>>& key_field_list,
                               const std::vector<std::shared_ptr<arrow::DataType>>& projected_types,
                               const std::vector<bool>& sort_directions, 
                               const std::vector<bool>& nulls_order) {
    std::string comp_str;
    int cur_key_id;
    auto field = key_field_list[cur_key_index];
    bool asc = sort_directions[cur_key_index];
    bool nulls_first = nulls_order[cur_key_index];
    std::shared_ptr<arrow::DataType> data_type;
    std::string array;
    // if projected, use projected batch to compare, and use projected type
    if (projected) {
      array = "projected_";
      data_type = projected_types[cur_key_index];
      // use the index of projected key
      cur_key_id = cur_key_index;
    } else {
      array = "cached_";
      data_type = field->type();
      // use the key_id
      cur_key_id = sort_key_index_list[cur_key_index];
    }

    auto x_num_value = array + std::to_string(cur_key_id) + "_[x.array_id]->GetView(x.id)";
    auto x_str_value = array + std::to_string(cur_key_id) + "_[x.array_id]->GetString(x.id)";
    auto y_num_value = array + std::to_string(cur_key_id) + "_[y.array_id]->GetView(y.id)";
    auto y_str_value = array + std::to_string(cur_key_id) + "_[y.array_id]->GetString(y.id)";
    auto is_x_null = array + std::to_string(cur_key_id) + "_[x.array_id]->IsNull(x.id)";
    auto is_y_null = array + std::to_string(cur_key_id) + "_[y.array_id]->IsNull(y.id)";
    auto is_x_nan = "std::isnan(" + x_num_value + ")";
    auto is_y_nan = "std::isnan(" + y_num_value + ")";

    // Multiple keys sorting w/ nulls first/last is supported.
    std::stringstream ss;
    // We need to determine the position of nulls.
    ss << "if (" << is_x_null << " && " << is_y_null << ") {\n";
    ss << "return false;} " << "else if (" << is_x_null << ") {\n";
    // If value accessed from x is null, return true to make nulls first.
    if (nulls_first) {
      ss << "return true;\n}";
    } else {
      ss << "return false;\n}";
    }
    // If value accessed from y is null, return false to make nulls first.
    ss << " else if (" << is_y_null << ") {\n";
    if (nulls_first) {
      ss << "return false;\n}";
    } else {
      ss << "return true;\n}";
    }
    // If datatype is floating, we need to do partition for NaN if NaN check is enabled
    if (data_type->id() == arrow::Type::DOUBLE || 
        data_type->id() == arrow::Type::FLOAT) {
      if (NaN_check_) {
        ss << " else if (" << is_x_nan << " && " << is_y_nan << ") {\n";
        ss << "return false;} " << "else if (" << is_x_nan << ") {\n";
        if (asc) {
          ss << "return false;\n}";
        } else {
          ss << "return true;\n}";
        }
        ss << "else if (" << is_y_nan << ") {\n";
        if (asc) {
          ss << "return true;\n}";
        } else {
          ss << "return false;\n}";
        }
      }
    }

    // If values accessed from x and y are both not null
    ss << " else {\n";

    // Multiple keys sorting w/ different ordering is supported.
    // For string type of data, GetString should be used instead of GetView.
    if (asc) {
      if (data_type->id() == arrow::Type::STRING) {
        ss << "return " << x_str_value << " < " << y_str_value << ";\n}\n";
      } else {
        ss << "return " << x_num_value << " < " << y_num_value << ";\n}\n";
      }
    } else {
      if (data_type->id() == arrow::Type::STRING) {
        ss << "return " << x_str_value << " > " << y_str_value << ";\n}\n";
      } else {
        ss << "return " << x_num_value << " > " << y_num_value << ";\n}\n";
      }
    }
    comp_str = ss.str();
    if ((cur_key_index + 1) == sort_key_index_list.size()) {
      return comp_str;
    }
    // clear the contents of stringstream
    ss.str(std::string());
    if (data_type->id() == arrow::Type::STRING) {
      ss << "if ((" <<  is_x_null << " && " << is_y_null << ") || (" 
         << x_str_value << " == " << y_str_value << ")) {";
    } else {
      if (NaN_check_ && (data_type->id() == arrow::Type::DOUBLE || 
          data_type->id() == arrow::Type::FLOAT)) {
        // need to check NaN
        ss << "if ((" <<  is_x_null << " && " << is_y_null << ") || ("
           << is_x_nan << " && " << is_y_nan << ") || ("
           << x_num_value << " == " << y_num_value << ")) {";
      } else {
        ss << "if ((" <<  is_x_null << " && " << is_y_null << ") || ("
           << x_num_value << " == " << y_num_value << ")) {";
      }
    }
    ss << GetCompFunction_(
        cur_key_index + 1, projected, sort_key_index_list, key_field_list, 
        projected_types, sort_directions, nulls_order) 
        << "} else { " << comp_str << "}";
    return ss.str();
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
    return "std::sort(indices_begin, indices_begin + "
            "items_total_, "
            "comp);"; 
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
  std::string GetProjectedVariables(
      const std::shared_ptr<gandiva::Projector>& key_projector,
      const std::vector<std::shared_ptr<arrow::DataType>>& projected_types) {
    std::stringstream ss;
    if (key_projector) {
      for (int i = 0; i < projected_types.size(); i++) {
        ss << "using ProjectedType_" << i << " = " 
               + GetTypeString(projected_types[i], "Array") << ";" << std::endl;
        ss << "std::vector<std::shared_ptr<ProjectedType_" << i << ">> projected_" 
           << i << "_;" << std::endl;
        ss << std::endl;
      }
    }
    return ss.str();
  }
  std::string GetCurCol(int first_key_id,
      const std::shared_ptr<gandiva::Projector>& key_projector) {
    std::stringstream ss;
    std::string array;
    if (key_projector) {
      array = "projected_";
      ss << "auto cur = " + array << "0_[" + array << "0_.size() - 1];" 
         << std::endl;
    } else {
      array = "cached_";
      ss << "auto cur = " + array << first_key_id << "_[" + array 
         << first_key_id << "_.size() - 1];" << std::endl;
    }
    return ss.str();   
  }
  std::string GetFirstCmpCol(int first_key_id,
      const std::shared_ptr<gandiva::Projector>& key_projector) {
    std::stringstream ss;
    std::string array;
    if (key_projector) {
      array = "projected_";
      ss << array << "0_[array_id]";
    } else {
      array = "cached_";
      ss << array << first_key_id << "_[array_id]";
    }
    return ss.str();   
  }
};

///////////////  SortArraysInPlace  ////////////////
template <typename DATATYPE, typename CTYPE>
class SortInplaceKernel : public SortArraysToIndicesKernel::Impl {
 public:
  SortInplaceKernel(arrow::compute::FunctionContext* ctx,
                    std::shared_ptr<arrow::Schema> result_schema,
                    std::shared_ptr<gandiva::Projector> key_projector,
                    std::vector<bool> sort_directions, 
                    std::vector<bool> nulls_order,
                    bool NaN_check)
      : ctx_(ctx), 
        nulls_first_(nulls_order[0]), 
        asc_(sort_directions[0]),
        result_schema_(result_schema), 
        key_projector_(key_projector),
        NaN_check_(NaN_check) {}

  arrow::Status Evaluate(const ArrayList& in) override {
    num_batches_++;
    // do projection here
    arrow::ArrayVector outputs;
    if (key_projector_) {
      auto length = in.size() > 0 ? in[0]->length() : 0;
      auto in_batch =
          arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(
          key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      cached_0_.push_back(outputs[0]);   
      nulls_total_ += outputs[0]->null_count(); 
    } else {
      cached_0_.push_back(in[0]);
      nulls_total_ += in[0]->null_count();
    }

    items_total_ += in[0]->length();
    return arrow::Status::OK();
  }

  template <typename TYPE>
  auto SortNoNull(TYPE* indices_begin, TYPE* indices_end)
      -> typename std::enable_if_t<std::is_floating_point<TYPE>::value> {
    if (asc_) {
      auto sort_end = indices_end;
      if (NaN_check_) {
        sort_end = std::partition(indices_begin, indices_end,
                                  [](TYPE i){return !std::isnan(i);});
      }
      ska_sort(indices_begin, sort_end);
    } else {
      auto sort_begin = indices_begin;
      if (NaN_check_) {
        sort_begin = std::partition(indices_begin, indices_end,
                                    [](TYPE i){return std::isnan(i);});
      }
      auto desc_comp = [this](TYPE& x, TYPE& y) {
        return x > y;
      };
      std::sort(sort_begin, indices_end, desc_comp);
    }
  }

  template <typename TYPE>
  auto SortNoNull(TYPE* indices_begin, TYPE* indices_end)
      -> typename std::enable_if_t<!std::is_floating_point<TYPE>::value> {
    if (asc_) {
      ska_sort(indices_begin, indices_end);
    } else {
      auto desc_comp = [this](TYPE& x, TYPE& y) { 
                       return x > y;
                       };
      std::sort(indices_begin, indices_end, desc_comp);
    }
  }

  template <typename TYPE, typename ArrayType>
  auto Sort(int64_t* indices_begin, int64_t* indices_end, const ArrayType& values)
      -> typename std::enable_if_t<std::is_floating_point<TYPE>::value> {
    std::iota(indices_begin, indices_end, 0);
    auto sort_begin = indices_begin;
    auto sort_end = indices_end;
    
    if (asc_) {
      if (nulls_first_) {
        sort_begin = 
              std::partition(indices_begin, indices_end, 
                             [&values](uint64_t ind) { 
                             return values.IsNull(ind); 
                             });
        if (NaN_check_) {
          // values should be sorted to:
          // null, null, ..., valid-1, valid-2, ..., valid-3, NaN, NaN, ...
          sort_end = 
              std::partition(sort_begin, indices_end, 
                            [&values](uint64_t ind) { 
                            return !std::isnan(values.GetView(ind)); 
                            });
        }
        ska_sort(sort_begin, sort_end, 
                [&values](auto& x) -> decltype(auto){ return values.GetView(x); });
      } else {
        sort_end = 
              std::partition(indices_begin, indices_end, 
                             [&values](uint64_t ind) { return !values.IsNull(ind); });
        if (NaN_check_) {
          // values should be sorted to:
          // valid-1, valid-2, ..., valid-3, NaN, NaN, ..., null, null, ...
          sort_end = 
              std::partition(indices_begin, sort_end, 
                             [&values](uint64_t ind) { 
                             return !std::isnan(values.GetView(ind)); 
                             });
        }
        ska_sort(indices_begin, sort_end,
                [&values](auto& x) -> decltype(auto){ return values.GetView(x); });
      }
    } else {
      auto comp = [&values](uint64_t left, uint64_t right) {
                  return values.GetView(left) > values.GetView(right);
                  };
      if (nulls_first_) {
        sort_begin = 
                std::partition(indices_begin, indices_end, 
                               [&values](uint64_t ind) { return values.IsNull(ind); 
                               });
        if (NaN_check_) {
          // values should be sorted to: 
          // null, null, NaN, NaN, ..., valid-1, valid-2, ..., valid-3, ...
          sort_begin = 
              std::partition(sort_begin, indices_end, 
                             [&values](uint64_t ind) { 
                             return std::isnan(values.GetView(ind)); 
                             });
        }
        std::sort(sort_begin, indices_end, comp);
      } else {
        sort_end = 
              std::partition(indices_begin, indices_end, 
                             [&values](uint64_t ind) { 
                             return !values.IsNull(ind); 
                             });
        if (NaN_check_) {
          // values should be sorted to: 
          // NaN, NaN, ..., valid-1, valid-2, ..., valid-3, ..., null, null, ...
          sort_begin = 
              std::partition(indices_begin, sort_end, 
                             [&values](uint64_t ind) { 
                             return std::isnan(values.GetView(ind)); 
                             });
        }
        std::sort(sort_begin, sort_end, comp);
      }
    }
  }

  template <typename TYPE, typename ArrayType>
  auto Sort(int64_t* indices_begin, int64_t* indices_end, const ArrayType& values)
      -> typename std::enable_if_t<!std::is_floating_point<TYPE>::value> {
    std::iota(indices_begin, indices_end, 0);
    if (asc_) {
      if (nulls_first_) {
        auto nulls_end =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return values.IsNull(ind); });
        ska_sort(nulls_end, indices_end, 
                [&values](auto& x) -> decltype(auto){ return values.GetView(x); });
      } else {
        auto nulls_begin =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return !values.IsNull(ind); });
        ska_sort(indices_begin, nulls_begin, 
                [&values](auto& x) -> decltype(auto){ return values.GetView(x); });
      }
    } else {
      auto comp = [&values](uint64_t left, uint64_t right) {
                  return values.GetView(left) > values.GetView(right);
                  };
      if (nulls_first_) {
        auto nulls_end =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return values.IsNull(ind); });
        std::sort(nulls_end, indices_end, comp);
      } else {
        auto nulls_begin =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return !values.IsNull(ind); });
        std::sort(indices_begin, nulls_begin, comp);
      }
    }
  }
  
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    RETURN_NOT_OK(arrow::Concatenate(cached_0_, ctx_->memory_pool(), 
                                     &concatenated_array_));
    if (nulls_total_ > 0) {
      auto typed_array = 
          std::dynamic_pointer_cast<ArrayType_0>(concatenated_array_);
      std::shared_ptr<arrow::Array> indices_out;

      int64_t buf_size = typed_array->length() * sizeof(uint64_t);
      ARROW_ASSIGN_OR_RAISE(auto indices_buf, 
                            AllocateBuffer(buf_size, ctx_->memory_pool()));
      int64_t* indices_begin = 
          reinterpret_cast<int64_t*>(indices_buf->mutable_data());
      int64_t* indices_end = indices_begin + typed_array->length();

      Sort<CTYPE, ArrayType_0>(indices_begin, indices_end, *typed_array.get());
      indices_out = 
          std::make_shared<arrow::UInt64Array>(typed_array->length(), 
                                               std::move(indices_buf));
      std::shared_ptr<arrow::Array> sort_out;
      arrow::compute::TakeOptions options;
      RETURN_NOT_OK(
          arrow::compute::Take(ctx_, *concatenated_array_.get(),
                               *indices_out.get(), options, &sort_out));
      *out = 
          std::make_shared<SorterResultIterator>(ctx_, schema, sort_out, 
                                                 nulls_first_, asc_);
    } else {
      CTYPE* indices_begin = 
          concatenated_array_->data()->GetMutableValues<CTYPE>(1);
      CTYPE* indices_end = indices_begin + concatenated_array_->length();
      
      SortNoNull<CTYPE>(indices_begin, indices_end);
      *out = 
          std::make_shared<SorterResultIterator>(ctx_, schema, concatenated_array_, 
                                                 nulls_first_, asc_);
    }
    return arrow::Status::OK();
  }

 private:
  using ArrayType_0 = typename arrow::TypeTraits<DATATYPE>::ArrayType;
  arrow::ArrayVector cached_0_;
  std::shared_ptr<arrow::Array> concatenated_array_;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  bool nulls_first_;
  bool asc_;
  bool NaN_check_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  uint64_t nulls_total_ = 0;

  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::FunctionContext* ctx,
                         std::shared_ptr<arrow::Schema> result_schema,
                         std::shared_ptr<arrow::Array> result_arr,
                         bool nulls_first, bool asc)
        : ctx_(ctx),
          result_schema_(result_schema),
          total_length_(result_arr->length()),
          nulls_total_(result_arr->null_count()),
          nulls_first_(nulls_first),
          asc_(asc),
          result_arr_(result_arr) {
      batch_size_ = GetBatchSize();
    }

    std::string ToString() override { return "SortArraysToIndicesResultIterator"; }

    bool HasNext() override {
      if (total_offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    template<typename KeyType>
    class SliceImpl {
     public:
      SliceImpl(const arrow::ArrayData& in, arrow::MemoryPool* pool, 
                int64_t length, int64_t offset, int64_t null_total, 
                bool null_first, int64_t total_length)
          : in_(in), 
            pool_(pool), 
            length_(length), 
            offset_(offset), 
            null_total_(null_total) {
        out_data_.type = in.type;
        out_data_.length = length;
        out_data_.buffers.resize(in.buffers.size());
        out_data_.child_data.resize(in.child_data.size());
        for (auto& data : out_data_.child_data) {
          data = std::make_shared<arrow::ArrayData>();
        }
        // decide null_count
        if (null_first) {
          if ((offset + length) > null_total_) {
            out_data_.null_count = 
                (null_total_ - offset > 0) ? (null_total_ - offset) : 0;
          } else {
            out_data_.null_count = length;
          } 
        } else {
          auto valid_total = total_length - null_total_;
          if ((offset + length) < valid_total) {
            out_data_.null_count = 0;
          } else {
            out_data_.null_count = 
                (offset - valid_total) > 0 ? 
                length : (offset + length - valid_total);
          }
        }
      }

      /// offset, length pair for representing a Range of a buffer or array
      struct Range {
        int64_t offset, length;

        Range() : offset(-1), length(0) {}
        Range(int64_t o, int64_t l) : offset(o), length(l) {}
      };

      /// non-owning view into a range of bits
      struct Bitmap {
        Bitmap() = default;
        Bitmap(const uint8_t* d, Range r) : data(d), range(r) {}
        explicit Bitmap(const std::shared_ptr<arrow::Buffer>& buffer, Range r)
            : Bitmap(buffer ? buffer->data() : nullptr, r) {}

        const uint8_t* data;
        Range range;

        bool AllSet() const { return data == nullptr; }
      };

      arrow::Result<std::shared_ptr<arrow::Buffer>> SliceBufferImpl(
        const std::shared_ptr<arrow::Buffer>& buffer) {
        ARROW_ASSIGN_OR_RAISE(auto out, AllocateBuffer(size * length_, pool_));
        auto out_data = out->mutable_data();
        auto data_begin = buffer->data();
        std::memcpy(out_data, data_begin + size * offset_, size * length_);
        return std::move(out);
      }
      
      arrow::Status SliceBuffer(const std::shared_ptr<arrow::Buffer>& buffer) {
        return SliceBufferImpl(buffer).Value(&out_data_.buffers[1]);
      }

      arrow::Result<std::shared_ptr<arrow::Buffer>> SliceBitmapImpl(const Bitmap& bitmap) {
        auto length = bitmap.range.length;
        auto offset = bitmap.range.offset;
        ARROW_ASSIGN_OR_RAISE(auto out, AllocateBitmap(length, pool_));
        uint8_t* dst = out->mutable_data();


        int64_t bitmap_offset = 0;
        if (bitmap.AllSet()) {
          arrow::BitUtil::SetBitsTo(dst, offset, length, true);
        } else {
          arrow::internal::CopyBitmap(bitmap.data, offset, length, dst,
                                      bitmap_offset, false);
        }
        
        // finally (if applicable) zero out any trailing bits
        if (auto preceding_bits = arrow::BitUtil::kPrecedingBitmask[length_ % 8]) {
          dst[length_ / 8] &= preceding_bits;
        }
        return std::move(out);
      }

      arrow::Status SliceBitmap(const std::shared_ptr<arrow::Buffer>& buffer) {
        Range range(size * offset_, size * length_);
        Bitmap bitmap = Bitmap(buffer, range);
        return SliceBitmapImpl(bitmap).Value(&out_data_.buffers[0]);
      } 
      
      arrow::Status Slice(arrow::ArrayData* out) {
        const auto& buffer_0 = in_.buffers[0];
        const auto& buffer_1 = in_.buffers[1];
        if (out_data_.null_count) {
          SliceBitmap(buffer_0);
        }
        SliceBuffer(buffer_1);
        *out = std::move(out_data_);
        return arrow::Status::OK();
      }

     private:
      arrow::ArrayData in_;
      arrow::ArrayData out_data_;
      arrow::MemoryPool* pool_;
      int64_t length_;
      int64_t offset_;
      int64_t null_total_;
      int size = sizeof(KeyType);
    };

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      auto length = (total_length_ - total_offset_) > batch_size_ ? batch_size_
                                                            : (total_length_ - total_offset_);
      arrow::ArrayData result_data = *result_arr_->data();
      arrow::ArrayData out_data;
      SliceImpl<CTYPE>(result_data, ctx_->memory_pool(), length, total_offset_, 
                nulls_total_, nulls_first_, total_length_).Slice(&out_data);
      std::shared_ptr<arrow::Array> out_0 = 
          MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
      total_offset_ += length;    
      *out = arrow::RecordBatch::Make(result_schema_, length, {out_0});
      return arrow::Status::OK();
    }

   private:
    using ArrayType_0 = typename arrow::TypeTraits<DATATYPE>::ArrayType;
    using BuilderType_0 = typename arrow::TypeTraits<DATATYPE>::BuilderType;
    std::shared_ptr<arrow::DataType> data_type_0 =
        arrow::TypeTraits<DATATYPE>::type_singleton();
    std::shared_ptr<arrow::Array> result_arr_;

    uint64_t total_offset_ = 0;
    uint64_t valid_offset_ = 0;
    const uint64_t total_length_;
    const uint64_t nulls_total_;
    std::shared_ptr<arrow::Schema> result_schema_;
    arrow::compute::FunctionContext* ctx_;
    uint64_t batch_size_;
    bool nulls_first_;
    bool asc_;
  };
};

///////////////  SortArraysOneKey  ////////////////
template <typename DATATYPE, typename CTYPE>
class SortOnekeyKernel  : public SortArraysToIndicesKernel::Impl {
 public:
  SortOnekeyKernel(arrow::compute::FunctionContext* ctx,
                   std::shared_ptr<arrow::Schema> result_schema,
                   std::shared_ptr<gandiva::Projector> key_projector,
                   std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                   std::vector<bool> sort_directions, 
                   std::vector<bool> nulls_order,
                   bool NaN_check)
      : ctx_(ctx), 
        nulls_first_(nulls_order[0]), 
        asc_(sort_directions[0]), 
        result_schema_(result_schema), 
        key_projector_(key_projector),
        NaN_check_(NaN_check) {
      #ifdef DEBUG
          std::cout << "UseSortOneKeyForArithmetic" << std::endl;
      #endif
      auto indices = result_schema->GetAllFieldIndices(key_field_list[0]->name());
      if (indices.size() < 1) {
        std::cout << "[ERROR] SortOnekeyKernel for arithmetic can't find key "
                  << key_field_list[0]->ToString() << " from " 
                  << result_schema->ToString() << std::endl;
        throw;
      }
      key_id_ = indices[0];
      col_num_ = result_schema->num_fields();
  }
  ~SortOnekeyKernel(){}

  arrow::Status Evaluate(const ArrayList& in) override {
    num_batches_++;
    // do projection here
    arrow::ArrayVector outputs;
    if (key_projector_) {
      auto length = in.size() > 0 ? in[key_id_]->length() : 0;
      auto in_batch =
          arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(
          key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      cached_key_.push_back(std::dynamic_pointer_cast<ArrayType_key>(outputs[0]));   
      nulls_total_ += outputs[0]->null_count(); 
    } else {
      cached_key_.push_back(std::dynamic_pointer_cast<ArrayType_key>(in[key_id_]));
      nulls_total_ += in[key_id_]->null_count();
    }

    items_total_ += in[key_id_]->length();
    length_list_.push_back(in[key_id_]->length());
    if (cached_.size() <= col_num_) {
      cached_.resize(col_num_ + 1);
    }
    for (int i = 0; i < col_num_; i++) {
      cached_[i].push_back(in[i]);
    }
    return arrow::Status::OK();
  }
    
  void PartitionNulls(ArrayItemIndexS* indices_begin, 
                      ArrayItemIndexS* indices_end) {
    int64_t indices_i = 0;
    int64_t indices_null = 0;
    
    if (nulls_total_ == 0) {
      // if all batches have no null value,
      // we do not need to check whether the value is null
      for (int array_id = 0; array_id < num_batches_; array_id++) {
        for (int64_t i = 0; i < length_list_[array_id]; i++) {
          (indices_begin + indices_i)->array_id = array_id;
          (indices_begin + indices_i)->id = i;
          indices_i++;
        }
      }
    } else {
      // we should support nulls first and nulls last here
      for (int array_id = 0; array_id < num_batches_; array_id++) {
        if (cached_key_[array_id]->null_count() == 0) {
          // if this array has no null value, 
          // we do need to check if the value is null
          for (int64_t i = 0; i < length_list_[array_id]; i++) {
            if (nulls_first_) {
              (indices_begin + nulls_total_ + indices_i)->array_id = array_id;
              (indices_begin + nulls_total_ + indices_i)->id = i;
              indices_i++;
            } else {
              (indices_begin + indices_i)->array_id = array_id;
              (indices_begin + indices_i)->id = i;
              indices_i++;
            }
          }
        } else {
          for (int64_t i = 0; i < length_list_[array_id]; i++) {
            if (nulls_first_) {
              if (!cached_key_[array_id]->IsNull(i)) {
                (indices_begin + nulls_total_ + indices_i)->array_id = array_id;
                (indices_begin + nulls_total_ + indices_i)->id = i;
                indices_i++;
              } else {
                (indices_begin + indices_null)->array_id = array_id;
                (indices_begin + indices_null)->id = i;
                indices_null++;
              }
            } else {
              if (!cached_key_[array_id]->IsNull(i)) {
                (indices_begin + indices_i)->array_id = array_id;
                (indices_begin + indices_i)->id = i;
                indices_i++;
              } else {
                (indices_end - nulls_total_ + indices_null)->array_id = array_id;
                (indices_end - nulls_total_ + indices_null)->id = i;
                indices_null++;
              }
            }
          }
        }
      }
    }   
  }

  int64_t PartitionNaNs(ArrayItemIndexS* indices_begin, 
                        ArrayItemIndexS* indices_end) {
    int64_t indices_i = 0;
    int64_t indices_nan = 0;
    
    for (int array_id = 0; array_id < num_batches_; array_id++) {
      for (int64_t i = 0; i < length_list_[array_id]; i++) {
        if (cached_key_[array_id]->IsNull(i)) {
          continue;
        }
        if (nulls_first_) {
          if (asc_) {
            // values should be partitioned to:
            // null, null, ..., valid-1, valid-2, ..., valid-3, NaN, NaN, ...
            if (!std::isnan(cached_key_[array_id]->GetView(i))) {
              (indices_begin + nulls_total_ + indices_i)->array_id = array_id;
              (indices_begin + nulls_total_ + indices_i)->id = i;
              indices_i++;
            } else {
              (indices_end - indices_nan - 1)->array_id = array_id;
              (indices_end - indices_nan - 1)->id = i;
              indices_nan++;
            }
          } else {
            // values should be partitioned to:
            // null, null, ..., NaN, NaN, ..., valid-1, valid-2, ..., valid-3, ...
            if (!std::isnan(cached_key_[array_id]->GetView(i))) {
              (indices_end - indices_i - 1)->array_id = array_id;
              (indices_end - indices_i - 1)->id = i;
              indices_i++;
            } else {
              (indices_begin + nulls_total_ + indices_nan)->array_id = array_id;
              (indices_begin + nulls_total_ + indices_nan)->id = i;
              indices_nan++;
            }
          }
        } else {
          if (asc_) {
            // values should be partitioned to:
            // valid-1, valid-2, ..., valid-3, ..., NaN, NaN, ..., null, null, ...
            if (!std::isnan(cached_key_[array_id]->GetView(i))) {
              (indices_begin + indices_i)->array_id = array_id;
              (indices_begin + indices_i)->id = i;
              indices_i++;
            } else {
              (indices_end - nulls_total_ - indices_nan - 1)->array_id = array_id;
              (indices_end - nulls_total_ - indices_nan - 1)->id = i;
              indices_nan++;
            }
          } else {
            // values should be partitioned to:
            // NaN, NaN, ..., valid-1, valid-2, ..., valid-3, ..., null, null, ...
            if (!std::isnan(cached_key_[array_id]->GetView(i))) {
              (indices_end - nulls_total_ - indices_i - 1)->array_id = array_id;
              (indices_end - nulls_total_ - indices_i - 1)->id = i;
              indices_i++;
            } else {
              (indices_begin + indices_nan)->array_id = array_id;
              (indices_begin + indices_nan)->id = i;
              indices_nan++;
            }
          }
        }
      }
    }
    return indices_nan;
  }

  template <typename T>
  auto Partition(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end, int64_t &num_nan)
      -> typename std::enable_if_t<std::is_floating_point<T>::value> {
    PartitionNulls(indices_begin, indices_end);
    if (NaN_check_) {
      num_nan = PartitionNaNs(indices_begin, indices_end);
    }
  }

  template <typename T>
  auto Partition(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end, int64_t &num_nan)
      -> typename std::enable_if_t<!std::is_floating_point<T>::value> {
    PartitionNulls(indices_begin, indices_end);
  }

  template <typename T>
  auto Sort(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end, int64_t num_nan)
      -> typename std::enable_if_t<!std::is_same<T, std::string>::value> {
    if (asc_) {
      if (nulls_first_) {
        ska_sort(indices_begin + nulls_total_, indices_begin + items_total_ - num_nan, 
            [this](auto& x) -> decltype(auto){ return cached_key_[x.array_id]->GetView(x.id); });
      } else {
        ska_sort(indices_begin, indices_begin + items_total_ - nulls_total_ - num_nan, 
            [this](auto& x) -> decltype(auto){ return cached_key_[x.array_id]->GetView(x.id); });
      }
    } else {
      auto comp = [this](ArrayItemIndexS x, ArrayItemIndexS y) {
        return cached_key_[x.array_id]->GetView(x.id) > cached_key_[y.array_id]->GetView(y.id);};
      if (nulls_first_) {
        std::sort(indices_begin + nulls_total_ + num_nan, indices_begin + items_total_, comp);
      } else {
        std::sort(indices_begin + num_nan, indices_begin + items_total_ - nulls_total_, comp);
      }
    }
  }

  template <typename T>
  auto Sort(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end, int64_t num_nan)
      -> typename std::enable_if_t<std::is_same<T, std::string>::value> {
    if (asc_) {
      auto comp = [this](ArrayItemIndexS x, ArrayItemIndexS y) {
        return cached_key_[x.array_id]->GetString(x.id) < cached_key_[y.array_id]->GetString(y.id);};
      if (nulls_first_) {
        std::sort(indices_begin + nulls_total_, indices_begin + items_total_, comp);
      } else {
        std::sort(indices_begin, indices_begin + items_total_ - nulls_total_, comp);
      }
    } else {
      auto comp = [this](ArrayItemIndexS x, ArrayItemIndexS y) {
        return cached_key_[x.array_id]->GetString(x.id) > cached_key_[y.array_id]->GetString(y.id);};
      if (nulls_first_) {
        std::sort(indices_begin + nulls_total_, indices_begin + items_total_, comp);
      } else {
        std::sort(indices_begin, indices_begin + items_total_ - nulls_total_, comp);
      }
    }
  }

  arrow::Status FinishInternal(std::shared_ptr<FixedSizeBinaryArray>* out) {    
    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total_ * sizeof(ArrayItemIndexS);
    RETURN_NOT_OK(arrow::AllocateBuffer(ctx_->memory_pool(), buf_size, &indices_buf));
    ArrayItemIndexS* indices_begin = 
      reinterpret_cast<ArrayItemIndexS*>(indices_buf->mutable_data());
    ArrayItemIndexS* indices_end = indices_begin + items_total_;
    // do partition and sort here
    int64_t num_nan = 0;
    Partition<CTYPE>(indices_begin, indices_end, num_nan);
    Sort<CTYPE>(indices_begin, indices_end, num_nan);
    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
    RETURN_NOT_OK(MakeFixedSizeBinaryType(sizeof(ArrayItemIndexS) / sizeof(int32_t), &out_type));
    RETURN_NOT_OK(MakeFixedSizeBinaryArray(out_type, items_total_, indices_buf, out));
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    std::shared_ptr<FixedSizeBinaryArray> indices_out;
    RETURN_NOT_OK(FinishInternal(&indices_out));
    *out = std::make_shared<SorterResultIterator>(ctx_, schema, indices_out, cached_);
    return arrow::Status::OK();
  }

 private:
  using ArrayType_key = typename arrow::TypeTraits<DATATYPE>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_key>> cached_key_;
  std::vector<arrow::ArrayVector> cached_;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  bool nulls_first_;
  bool asc_;
  bool NaN_check_;
  std::vector<int64_t> length_list_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  uint64_t nulls_total_ = 0;
  int col_num_;
  int key_id_;
  
  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::FunctionContext* ctx,
                         std::shared_ptr<arrow::Schema> schema,
                         std::shared_ptr<FixedSizeBinaryArray> indices_in,
                         std::vector<arrow::ArrayVector>& cached)
        : ctx_(ctx),
          schema_(schema),
          indices_in_cache_(indices_in),
          total_length_(indices_in->length()),
          cached_in_(cached) {
      col_num_ = schema->num_fields();
      indices_begin_ = (ArrayItemIndexS*)indices_in->value_data();
      // appender_type won't be used
      AppenderBase::AppenderType appender_type = AppenderBase::left;
      for (int i = 0; i < col_num_; i++) {
        auto field = schema->field(i);
        std::shared_ptr<AppenderBase> appender;
        MakeAppender(ctx_, field->type(), appender_type, &appender);
        appender_list_.push_back(appender);
      }
      for (int i = 0; i < col_num_; i++) {
        arrow::ArrayVector array_vector = cached_in_[i];
        int array_num = array_vector.size();
        for (int array_id = 0; array_id < array_num; array_id++) {
          auto arr = array_vector[array_id];
          appender_list_[i]->AddArray(arr);
        }
      }
      batch_size_ = GetBatchSize();
    }
    ~SorterResultIterator(){}

    std::string ToString() override { return "SortArraysToIndicesResultIterator"; }

    bool HasNext() override {
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      auto length = (total_length_ - offset_) > batch_size_ ? batch_size_
                                                            : (total_length_ - offset_);
      uint64_t count = 0;
      for (int i = 0; i < col_num_; i++) {
        while (count < length) {
          auto item = indices_begin_ + offset_ + count++;
          RETURN_NOT_OK(appender_list_[i]->Append(item->array_id, item->id));
        }
        count = 0;
      }
      offset_ += length;
      ArrayList arrays;
      for (int i = 0; i < col_num_; i++) {
        std::shared_ptr<arrow::Array> out_array;
        RETURN_NOT_OK(appender_list_[i]->Finish(&out_array));
        arrays.push_back(out_array);
        appender_list_[i]->Reset();
      }

      *out = arrow::RecordBatch::Make(schema_, length, arrays);
      return arrow::Status::OK();
    }

   private:
    uint64_t offset_ = 0;
    const uint64_t total_length_;
    std::shared_ptr<arrow::Schema> schema_;
    arrow::compute::FunctionContext* ctx_;
    uint64_t batch_size_;
    int col_num_;
    ArrayItemIndexS* indices_begin_;
    std::vector<arrow::ArrayVector> cached_in_;
    std::vector<std::shared_ptr<arrow::DataType>> type_list_;
    std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    std::vector<std::shared_ptr<arrow::Array>> array_list_;
    std::shared_ptr<FixedSizeBinaryArray> indices_in_cache_;
  };
};

arrow::Status SortArraysToIndicesKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::shared_ptr<arrow::Schema> result_schema,
    gandiva::NodeVector sort_key_node,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::vector<bool> sort_directions, 
    std::vector<bool> nulls_order,
    bool NaN_check,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<SortArraysToIndicesKernel>(
    ctx, result_schema, sort_key_node, key_field_list, sort_directions, nulls_order, NaN_check);
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
  PROCESS(arrow::Date64Type)
SortArraysToIndicesKernel::SortArraysToIndicesKernel(
    arrow::compute::FunctionContext* ctx,
    std::shared_ptr<arrow::Schema> result_schema,
    gandiva::NodeVector sort_key_node,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::vector<bool> sort_directions, 
    std::vector<bool> nulls_order,
    bool NaN_check) {
  // sort_key_node may need to do projection
  bool pre_processed_key_ = false;
  gandiva::NodePtr key_project;
  gandiva::ExpressionVector key_project_exprs;

  for (auto node : sort_key_node) {
    std::shared_ptr<TypedNodeVisitor> node_visitor;
    THROW_NOT_OK(MakeTypedNodeVisitor(node, &node_visitor));
    if (node_visitor->GetResultType() != TypedNodeVisitor::FieldNode) {
      pre_processed_key_ = true;
      break;
    }
  }
  // If not all key_node are FieldNode, we need to do projection
  std::shared_ptr<gandiva::Projector> key_projector;
  std::vector<std::shared_ptr<arrow::DataType>> projected_types;
  if (pre_processed_key_) {
    key_project_exprs = GetGandivaKernel(sort_key_node);
    auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
    THROW_NOT_OK(gandiva::Projector::Make(result_schema, key_project_exprs,
                                          configuration, &key_projector));
    for (const auto& expr : key_project_exprs) {
      auto key_type = expr->root()->return_type();
      projected_types.push_back(key_type);
    }
  }

  if (key_field_list.size() == 1 && result_schema->num_fields() == 1 
      && key_field_list[0]->type()->id() != arrow::Type::STRING
      && key_field_list[0]->type()->id() != arrow::Type::BOOL) {
    // Will use SortInplace when sorting for one non-string and non-boolean col 
#ifdef DEBUG
    std::cout << "UseSortInplace" << std::endl;
#endif
    switch (key_field_list[0]->type()->id()) {
#define PROCESS(InType)                                                       \
  case InType::type_id: {                                                     \
    using CType = typename arrow::TypeTraits<InType>::CType;                  \
    impl_.reset(new SortInplaceKernel<InType, CType>(ctx, result_schema, key_projector, sort_directions, nulls_order, NaN_check)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      default: {
        std::cout << "SortInplaceKernel type not supported, type is "
                  << key_field_list[0]->type() << std::endl;
      } break;
    }
  } else if (key_field_list.size() == 1 && result_schema->num_fields() >= 1) {
    // Will use SortOnekey when: 
    // 1. sorting for one col with payload 2. sorting for one string col or one bool col
#ifdef DEBUG
    std::cout << "UseSortOneKey" << std::endl;
#endif
    if (pre_processed_key_) {
      // if needs projection, will use projected type for key col
      if (projected_types[0]->id() == arrow::Type::STRING) {
        impl_.reset(new SortOnekeyKernel<arrow::StringType, std::string>(
          ctx, result_schema, key_projector, key_field_list, sort_directions, nulls_order, NaN_check));
      } else {
        switch (projected_types[0]->id()) {
#define PROCESS(InType)                                                       \
    case InType::type_id: {                                                   \
      using CType = typename arrow::TypeTraits<InType>::CType;                \
      impl_.reset(new SortOnekeyKernel<InType, CType>(ctx, result_schema, key_projector, key_field_list, sort_directions, nulls_order, NaN_check)); \
    } break;
          PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
          default: {
            std::cout << "SortOnekeyKernel type not supported, type is "
                      << key_field_list[0]->type() << std::endl;
          } break;
        }
      }
    } else {
      // if no projection, will use the original type for key col
      if (key_field_list[0]->type()->id() == arrow::Type::STRING) {
        impl_.reset(new SortOnekeyKernel<arrow::StringType, std::string>(
          ctx, result_schema, key_projector, key_field_list, sort_directions, nulls_order, NaN_check));
      } else {
        switch (key_field_list[0]->type()->id()) {
#define PROCESS(InType)                                                       \
    case InType::type_id: {                                                   \
      using CType = typename arrow::TypeTraits<InType>::CType;                \
      impl_.reset(new SortOnekeyKernel<InType, CType>(ctx, result_schema, key_projector, key_field_list, sort_directions, nulls_order, NaN_check)); \
    } break;
          PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
          default: {
            std::cout << "SortOnekeyKernel type not supported, type is "
                      << key_field_list[0]->type() << std::endl;
          } break;
        }
      }
    }
  } else {
    // Will use Sort Codegen when sorting for several cols
    impl_.reset(new Impl(ctx, result_schema, key_projector, projected_types, 
                         key_field_list, sort_directions, nulls_order, NaN_check));
    auto status = impl_->LoadJITFunction(key_field_list, result_schema);
    if (!status.ok()) {
      std::cout << "LoadJITFunction failed, msg is " << status.message() << std::endl;
      throw;
    }
  }
  kernel_name_ = "SortArraysToIndicesKernel";
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

std::string SortArraysToIndicesKernel::GetSignature() { return impl_->GetSignature(); }

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
