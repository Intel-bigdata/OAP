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
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <codegen/arrow_compute/expr_visitor.h>
#include <gandiva/configuration.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>
#include <unistd.h>

#include <chrono>
#include <memory>

#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/common/hash_relation.h"
#include "codegen/common/result_iterator.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {

class ExprVisitorImpl {
 public:
  ExprVisitorImpl(ExprVisitor* p) : p_(p) {}
  virtual arrow::Status Eval() {
    return arrow::Status::NotImplemented("ExprVisitorImpl Eval is abstract.");
  }

  virtual arrow::Status Init() {
    return arrow::Status::NotImplemented("ExprVisitorImpl Init is abstract.");
  }

  virtual arrow::Status SetMember() {
    return arrow::Status::NotImplemented("ExprVisitorImpl SetMember is abstract.");
  }

  virtual arrow::Status SetDependency(
      const std::shared_ptr<ResultIterator<arrow::RecordBatch>>& dependency_iter,
      int index) {
    return arrow::Status::NotImplemented("ExprVisitorImpl SetDependency is abstract.");
  }

  virtual arrow::Status Finish() {
#ifdef DEBUG
    std::cout << "ExprVisitorImpl::Finish visitor is " << p_->func_name_ << ", ptr is "
              << p_ << std::endl;
#endif
    return arrow::Status::OK();
  }

  virtual arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                           std::shared_ptr<ResultIteratorBase>* out) {
    return arrow::Status::NotImplemented("ExprVisitorImpl ", p_->func_name_,
                                         " MakeResultIterator is abstract.");
  }

 protected:
  ExprVisitor* p_;
  bool initialized_ = false;
  ArrowComputeResultType finish_return_type_;
  std::shared_ptr<extra::KernalBase> kernel_;
  arrow::Status GetColumnIdAndFieldByName(std::shared_ptr<arrow::Schema> schema,
                                          std::string col_name, int* id,
                                          std::shared_ptr<arrow::Field>* out_field) {
    auto names = schema->field_names();
    for (int i = 0; i < names.size(); i++) {
      auto name = names.at(i);
      if (name == col_name) {
        *id = i;
        *out_field = schema->field(i);
        return arrow::Status::OK();
      }
    }
    return arrow::Status::Invalid("GetColumnIdAndFieldByName doesn't found col_name ",
                                  col_name);
  }
};

//////////////////////// SplitArrayListWithActionVisitorImpl //////////////////////
class SplitArrayListWithActionVisitorImpl : public ExprVisitorImpl {
 public:
  SplitArrayListWithActionVisitorImpl(ExprVisitor* p) : ExprVisitorImpl(p) {}
  static arrow::Status Make(ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<SplitArrayListWithActionVisitorImpl>(p);
    *out = impl;
    return arrow::Status::OK();
  }
  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    if (p_->action_name_list_.empty()) {
      return arrow::Status::Invalid(
          "ExprVisitor::SplitArrayListWithAction have empty action_name_list, "
          "this "
          "is invalid.");
    }

    std::vector<std::shared_ptr<arrow::DataType>> type_list;
    for (auto col_name : p_->action_param_list_) {
      std::shared_ptr<arrow::Field> field;
      int col_id;
      RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, col_name, &col_id, &field));
      p_->result_fields_.push_back(field);
      col_id_list_.push_back(col_id);
      type_list.push_back(field->type());
    }
    RETURN_NOT_OK(extra::SplitArrayListWithActionKernel::Make(
        &p_->ctx_, p_->action_name_list_, type_list, &kernel_));
    initialized_ = true;
    finish_return_type_ = ArrowComputeResultType::Batch;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::Array: {
        ArrayList col_list;
        for (auto col_id : col_id_list_) {
          if (col_id >= p_->in_record_batch_->num_columns()) {
            return arrow::Status::Invalid(
                "SplitArrayListWithActionVisitorImpl Eval col_id is bigger than input "
                "batch numColumns.");
          }
          auto col = p_->in_record_batch_->column(col_id);
          col_list.push_back(col);
        }
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Evaluate(col_list, p_->in_array_));
        p_->dependency_result_type_ = ArrowComputeResultType::None;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "SplitArrayListWithActionVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish() override {
    RETURN_NOT_OK(ExprVisitorImpl::Finish());
    switch (finish_return_type_) {
      case ArrowComputeResultType::Batch: {
        RETURN_NOT_OK(kernel_->Finish(&p_->result_batch_));
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default: {
        return arrow::Status::NotImplemented(
            "SplitArrayListWithActionVisitorImpl only support finish_return_type as "
            "Batch.");
        break;
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIteratorBase>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::Batch: {
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter_out;
        TIME_MICRO_OR_RAISE(p_->elapse_time_,
                            kernel_->MakeResultIterator(schema, &iter_out));
        *out = std::dynamic_pointer_cast<ResultIteratorBase>(iter_out);
        p_->return_type_ = ArrowComputeResultType::BatchIterator;
      } break;
      default:
        return arrow::Status::Invalid(
            "SplitArrayListWithActionVisitorImpl Finish does not support dependency type "
            "other than Batch.");
    }
    return arrow::Status::OK();
  }

 private:
  std::vector<int> col_id_list_;
};

////////////////////////// AggregateVisitorImpl ///////////////////////
class AggregateVisitorImpl : public ExprVisitorImpl {
 public:
  AggregateVisitorImpl(ExprVisitor* p, std::string func_name)
      : ExprVisitorImpl(p), func_name_(func_name) {}
  static arrow::Status Make(ExprVisitor* p, std::string func_name,
                            std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<AggregateVisitorImpl>(p, func_name);
    *out = impl;
    return arrow::Status::OK();
  }
  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    for (auto col_name : p_->param_field_names_) {
      std::shared_ptr<arrow::Field> field;
      int col_id;
      RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, col_name, &col_id, &field));
      p_->result_fields_.push_back(field);
      col_id_list_.push_back(col_id);
    }
    auto data_type = p_->result_fields_[0]->type();

    if (func_name_.compare("sum") == 0) {
      RETURN_NOT_OK(extra::SumArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
      kernel_list_.push_back(kernel_);
    } else if (func_name_.compare("count") == 0) {
      RETURN_NOT_OK(extra::CountArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
      kernel_list_.push_back(kernel_);
    } else if (func_name_.compare("sum_count") == 0) {
      p_->result_fields_.push_back(arrow::field("cnt", arrow::int64()));
      RETURN_NOT_OK(extra::SumCountArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
      kernel_list_.push_back(kernel_);
    } else if (func_name_.compare("sum_count_merge") == 0) {
      RETURN_NOT_OK(extra::SumArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
      kernel_list_.push_back(kernel_);
      RETURN_NOT_OK(extra::SumArrayKernel::Make(&p_->ctx_, p_->result_fields_[1]->type(),
                                                &kernel_));
      kernel_list_.push_back(kernel_);
    } else if (func_name_.compare("avgByCount") == 0) {
      p_->result_fields_.erase(p_->result_fields_.end() - 1);
      RETURN_NOT_OK(extra::AvgByCountArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
      kernel_list_.push_back(kernel_);
    } else if (func_name_.compare("min") == 0) {
      RETURN_NOT_OK(extra::MinArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
      kernel_list_.push_back(kernel_);
    } else if (func_name_.compare("max") == 0) {
      RETURN_NOT_OK(extra::MaxArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
      kernel_list_.push_back(kernel_);
    } else if (func_name_.compare("stddev_samp_partial") == 0) {
      p_->result_fields_.push_back(arrow::field("avg", arrow::int64()));
      p_->result_fields_.push_back(arrow::field("m2", arrow::int64()));
      RETURN_NOT_OK(
          extra::StddevSampPartialArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
      kernel_list_.push_back(kernel_);
    } else if (func_name_.compare("stddev_samp_final") == 0) {
      p_->result_fields_.erase(p_->result_fields_.end() - 1);
      p_->result_fields_.erase(p_->result_fields_.end() - 1);
      RETURN_NOT_OK(
          extra::StddevSampFinalArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
      kernel_list_.push_back(kernel_);
    }
    initialized_ = true;
    finish_return_type_ = ArrowComputeResultType::Batch;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        ArrayList in;
        for (auto col_id : col_id_list_) {
          if (col_id >= p_->in_record_batch_->num_columns()) {
            return arrow::Status::Invalid(
                "AggregateVisitorImpl Eval col_id is bigger than input "
                "batch numColumns.");
          }
          auto col = p_->in_record_batch_->column(col_id);
          in.push_back(col);
        }
        for (int i = 0; i < kernel_list_.size(); i++) {
          if (kernel_list_.size() > 1) {
            RETURN_NOT_OK(kernel_list_[i]->Evaluate({in[i]}));
          } else {
            RETURN_NOT_OK(kernel_list_[i]->Evaluate(in));
          }
        }
      } break;
      default:
        return arrow::Status::NotImplemented(
            "AggregateVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish() override {
    RETURN_NOT_OK(ExprVisitorImpl::Finish());
    switch (finish_return_type_) {
      case ArrowComputeResultType::Batch: {
        for (auto kernel : kernel_list_) {
          RETURN_NOT_OK(kernel->Finish(&p_->result_batch_));
        }
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default: {
        return arrow::Status::NotImplemented(
            "AggregateVisitorImpl only support finish_return_type as "
            "Array.");
        break;
      }
    }
    return arrow::Status::OK();
  }

 private:
  std::vector<int> col_id_list_;
  std::string func_name_;
  std::vector<std::shared_ptr<extra::KernalBase>> kernel_list_;
};

class WindowVisitorImpl : public ExprVisitorImpl {
 public:
  WindowVisitorImpl(ExprVisitor* p, std::vector<std::string> window_function_names,
                    std::vector<std::shared_ptr<arrow::DataType>> return_types,
                    std::vector<std::vector<gandiva::FieldPtr>> function_param_fields,
                    std::vector<gandiva::FieldPtr> partition_fields)
      : ExprVisitorImpl(p) {
    this->window_function_names_ = window_function_names;
    this->return_types_ = return_types,
    this->function_param_fields_ = function_param_fields;
    this->partition_fields_ = partition_fields;
  }

  static arrow::Status Make(
      ExprVisitor* p, std::vector<std::string> window_function_names,
      std::vector<std::shared_ptr<arrow::DataType>> return_types,
      std::vector<std::vector<gandiva::FieldPtr>> function_param_fields,
      std::vector<gandiva::FieldPtr> partition_fields,
      std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<WindowVisitorImpl>(
        p, window_function_names, return_types, function_param_fields, partition_fields);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    std::vector<std::shared_ptr<arrow::DataType>> partition_type_list;
    for (auto partition_field : partition_fields_) {
      std::shared_ptr<arrow::Field> field;
      int col_id;
      RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, partition_field->name(),
                                              &col_id, &field));
      partition_field_ids_.push_back(col_id);
      partition_type_list.push_back(field->type());
    }
    if (partition_type_list.size() > 1) {
      RETURN_NOT_OK(
          extra::HashArrayKernel::Make(&p_->ctx_, partition_type_list, &concat_kernel_));
    }

    RETURN_NOT_OK(extra::EncodeArrayKernel::Make(&p_->ctx_, &partition_kernel_));

    for (int func_id = 0; func_id < window_function_names_.size(); func_id++) {
      std::string window_function_name = window_function_names_.at(func_id);
      std::shared_ptr<arrow::DataType> return_type = return_types_.at(func_id);
      std::vector<gandiva::FieldPtr> function_param_fields_of_each =
          function_param_fields_.at(func_id);
      std::shared_ptr<extra::KernalBase> function_kernel;
      std::vector<int> function_param_field_ids_of_each;
      std::vector<std::shared_ptr<arrow::DataType>> function_param_type_list;
      for (auto function_param_field : function_param_fields_of_each) {
        std::shared_ptr<arrow::Field> field;
        int col_id;
        RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, function_param_field->name(),
                                                &col_id, &field));
        function_param_field_ids_of_each.push_back(col_id);
        function_param_type_list.push_back(field->type());
      }
      function_param_field_ids_.push_back(function_param_field_ids_of_each);

      if (window_function_name == "sum" || window_function_name == "avg") {
        RETURN_NOT_OK(extra::WindowAggregateFunctionKernel::Make(
            &p_->ctx_, window_function_name, function_param_type_list, return_type,
            &function_kernel));
      } else if (window_function_name == "rank_asc") {
        RETURN_NOT_OK(extra::WindowRankKernel::Make(&p_->ctx_, window_function_name,
                                                    function_param_type_list,
                                                    &function_kernel, false));
      } else if (window_function_name == "rank_desc") {
        RETURN_NOT_OK(extra::WindowRankKernel::Make(&p_->ctx_, window_function_name,
                                                    function_param_type_list,
                                                    &function_kernel, true));
      } else {
        return arrow::Status::Invalid("window function not supported: " +
                                      window_function_name);
      }
      function_kernels_.push_back(function_kernel);
    }

    initialized_ = true;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    std::shared_ptr<arrow::Array> out1;
    std::shared_ptr<arrow::Array> out2;
    if (partition_field_ids_.empty()) {
      auto row_count = p_->in_record_batch_->num_rows();
      arrow::Int32Builder single_partition_out2;
      for (int i = 0; i < row_count; i++) {
        // only parition 0 is generated for all rows
        RETURN_NOT_OK(single_partition_out2.Append(0));
      }
      RETURN_NOT_OK(single_partition_out2.Finish(&out2));
    } else {
      if (!concat_kernel_) {
        out1 = p_->in_record_batch_->column(partition_field_ids_[0]);
      } else {
        ArrayList in1;
        for (auto col_id : partition_field_ids_) {
          if (col_id >= p_->in_record_batch_->num_columns()) {
            return arrow::Status::Invalid(
                "WindowVisitorImpl: Partition field number overflows defined column "
                "count");
          }
          auto col = p_->in_record_batch_->column(col_id);
          in1.push_back(col);
        }
        RETURN_NOT_OK(concat_kernel_->Evaluate(in1, &out1));
      }

      std::shared_ptr<arrow::Array> in2 = out1;
      RETURN_NOT_OK(partition_kernel_->Evaluate(in2, &out2));
    }

    for (int func_id = 0; func_id < window_function_names_.size(); func_id++) {
      ArrayList in3;
      for (auto col_id : function_param_field_ids_.at(func_id)) {
        if (col_id >= p_->in_record_batch_->num_columns()) {
          return arrow::Status::Invalid(
              "WindowVisitorImpl: Function parameter number overflows defined column "
              "count");
        }
        auto col = p_->in_record_batch_->column(col_id);
        in3.push_back(col);
      }
      in3.push_back(out2);
      RETURN_NOT_OK(function_kernels_.at(func_id)->Evaluate(in3));
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish() override {
    int32_t num_batches = -1;
    std::vector<ArrayList> outs;
    for (int func_id = 0; func_id < window_function_names_.size(); func_id++) {
      ArrayList out0;
      RETURN_NOT_OK(function_kernels_.at(func_id)->Finish(&out0));
      if (num_batches == -1) {
        num_batches = out0.size();
      } else if (num_batches != out0.size()) {
        return arrow::Status::Invalid(
            "WindowVisitorImpl: Return batch counts are not the same for "
            "different window functions");
      }
      outs.push_back(out0);
    }
    if (num_batches == -1) {
      return arrow::Status::Invalid(
          "WindowVisitorImpl: No batches returned for window functions");
    }
    std::vector<ArrayList> out;
    std::vector<int> out_sizes;
    for (int batch_id = 0; batch_id < num_batches; batch_id++) {
      ArrayList temp;
      int64_t length = -1L;
      for (auto out0 : outs) {
        std::shared_ptr<arrow::Array> arr = out0.at(batch_id);
        if (length == -1L) {
          length = arr->length();
        } else if (length != arr->length()) {
          return arrow::Status::Invalid(
              "WindowVisitorImpl: Return array length in the same batch are not the same "
              "for "
              "different window functions");
        }
        temp.push_back(arr);
      }
      if (length == -1) {
        return arrow::Status::Invalid(
            "WindowVisitorImpl: No valid batch length returned for window functions");
      }
      out.push_back(temp);
      out_sizes.push_back(length);
    }
    p_->result_batch_list_ = out;
    p_->result_batch_size_list_ = out_sizes;
    p_->return_type_ = ArrowComputeResultType::BatchList;
    return ExprVisitorImpl::Finish();
  }

 private:
  std::vector<std::string> window_function_names_;
  std::vector<std::shared_ptr<arrow::DataType>> return_types_;
  std::vector<std::vector<gandiva::FieldPtr>> function_param_fields_;
  std::vector<gandiva::FieldPtr> partition_fields_;
  std::vector<std::vector<int>> function_param_field_ids_;
  std::vector<int> partition_field_ids_;
  std::shared_ptr<extra::KernalBase> concat_kernel_;
  std::shared_ptr<extra::KernalBase> partition_kernel_;
  std::vector<std::shared_ptr<extra::KernalBase>> function_kernels_;
};

////////////////////////// EncodeVisitorImpl ///////////////////////
class EncodeVisitorImpl : public ExprVisitorImpl {
 public:
  EncodeVisitorImpl(ExprVisitor* p) : ExprVisitorImpl(p) {}
  static arrow::Status Make(ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<EncodeVisitorImpl>(p);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::EncodeArrayKernel::Make(&p_->ctx_, &kernel_));
    std::vector<std::shared_ptr<arrow::DataType>> type_list;
    for (auto col_name : p_->param_field_names_) {
      std::shared_ptr<arrow::Field> field;
      int col_id;
      RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, col_name, &col_id, &field));
      col_id_list_.push_back(col_id);
      type_list.push_back(field->type());
    }

    // create a new kernel to memcpy all keys as one binary array
    if (type_list.size() > 1) {
      RETURN_NOT_OK(extra::HashArrayKernel::Make(&p_->ctx_, type_list, &concat_kernel_));
    }

    auto result_field = field("res", arrow::uint64());
    p_->result_fields_.push_back(result_field);
    initialized_ = true;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        std::shared_ptr<arrow::Array> col;
        if (concat_kernel_) {
          std::vector<std::shared_ptr<arrow::Array>> array_list;
          for (auto col_id : col_id_list_) {
            array_list.push_back(p_->in_record_batch_->column(col_id));
          }

          TIME_MICRO_OR_RAISE(concat_elapse_time,
                              concat_kernel_->Evaluate(array_list, &col));
        } else {
          col = p_->in_record_batch_->column(col_id_list_[0]);
        }

        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Evaluate(col, &p_->result_array_));
        p_->return_type_ = ArrowComputeResultType::Array;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "EncodeVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish() override { return arrow::Status::OK(); }

 private:
  std::vector<int> col_id_list_;
  std::shared_ptr<extra::KernalBase> concat_kernel_;
  uint64_t concat_elapse_time = 0;
};

////////////////////////// SortArraysToIndicesVisitorImpl ///////////////////////
class SortArraysToIndicesVisitorImpl : public ExprVisitorImpl {
 public:
  SortArraysToIndicesVisitorImpl(std::vector<std::shared_ptr<arrow::Field>> field_list,
                                 std::shared_ptr<gandiva::FunctionNode> root_node,
                                 std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                                 ExprVisitor* p)
      : root_node_(root_node),
        field_list_(field_list),
        ret_fields_(ret_fields),
        ExprVisitorImpl(p) {
    auto children = root_node->children();
    // first child is key_function
    sort_key_node_ =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[0])->children();
    // second child is key_field
    auto key_field_node = std::dynamic_pointer_cast<gandiva::FunctionNode>(children[1]);
    for (auto field : key_field_node->children()) {
      auto field_node = std::dynamic_pointer_cast<gandiva::FieldNode>(field);
      key_field_list_.push_back(field_node->field());
    }
    // third child is sort_directions
    auto sort_directions_node =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[2]);
    for (auto direction : sort_directions_node->children()) {
      auto dir_node = std::dynamic_pointer_cast<gandiva::LiteralNode>(direction);
      bool dir_val = arrow::util::get<bool>(dir_node->holder());
      sort_directions_.push_back(dir_val);
    }
    // fourth child is nulls_order
    auto nulls_order_node = std::dynamic_pointer_cast<gandiva::FunctionNode>(children[3]);
    for (auto order : nulls_order_node->children()) {
      auto order_node = std::dynamic_pointer_cast<gandiva::LiteralNode>(order);
      bool order_val = arrow::util::get<bool>(order_node->holder());
      nulls_order_.push_back(order_val);
    }
    result_schema_ = arrow::schema(ret_fields);
  }

  static arrow::Status Make(std::vector<std::shared_ptr<arrow::Field>> field_list,
                            std::shared_ptr<gandiva::FunctionNode> root_node,
                            std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                            ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<SortArraysToIndicesVisitorImpl>(field_list, root_node,
                                                                 ret_fields, p);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::SortArraysToIndicesKernel::Make(
        &p_->ctx_, result_schema_, sort_key_node_, key_field_list_, sort_directions_,
        nulls_order_, &kernel_));
    p_->signature_ = kernel_->GetSignature();
    initialized_ = true;
    finish_return_type_ = ArrowComputeResultType::BatchIterator;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        std::vector<std::shared_ptr<arrow::Array>> col_list;
        for (auto col : p_->in_record_batch_->columns()) {
          col_list.push_back(col);
        }
        RETURN_NOT_OK(kernel_->Evaluate(col_list));
      } break;
      default:
        return arrow::Status::NotImplemented(
            "SortArraysToIndicesVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIteratorBase>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::BatchIterator: {
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter_out;
        TIME_MICRO_OR_RAISE(p_->elapse_time_,
                            kernel_->MakeResultIterator(schema, &iter_out));
        *out = std::dynamic_pointer_cast<ResultIteratorBase>(iter_out);
        p_->return_type_ = ArrowComputeResultType::BatchIterator;
      } break;
      default:
        return arrow::Status::Invalid(
            "SortArraysToIndicesVisitorImpl MakeResultIterator does not support "
            "dependency type other than Batch.");
    }
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<gandiva::FunctionNode> root_node_;
  gandiva::FieldVector field_list_;
  gandiva::FieldVector ret_fields_;
  gandiva::NodeVector sort_key_node_;
  std::vector<std::shared_ptr<arrow::Field>> key_field_list_;
  std::vector<bool> sort_directions_;
  std::vector<bool> nulls_order_;
  std::shared_ptr<arrow::Schema> result_schema_;
};

////////////////////////// ConditionedProbeArraysVisitorImpl ///////////////////////
class ConditionedProbeArraysVisitorImpl : public ExprVisitorImpl {
 public:
  ConditionedProbeArraysVisitorImpl(std::vector<std::shared_ptr<arrow::Field>> field_list,
                                    std::shared_ptr<gandiva::FunctionNode> root_node,
                                    std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                                    ExprVisitor* p)
      : root_node_(root_node),
        field_list_(field_list),
        ret_fields_(ret_fields),
        ExprVisitorImpl(p) {
    auto func_name = root_node->descriptor()->name();
    auto children = root_node->children();
    if (func_name.compare("conditionedProbeArraysInner") == 0) {
      join_type_ = 0;
    } else if (func_name.compare("conditionedProbeArraysOuter") == 0) {
      join_type_ = 1;
    } else if (func_name.compare("conditionedProbeArraysAnti") == 0) {
      join_type_ = 2;
    } else if (func_name.compare("conditionedProbeArraysSemi") == 0) {
      join_type_ = 3;
    } else if (func_name.compare("conditionedProbeArraysExistence") == 0) {
      join_type_ = 4;
    }
    left_field_list_ =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[0])->children();
    right_field_list_ =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[1])->children();
    left_key_list_ =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[2])->children();
    right_key_list_ =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[3])->children();
    result_field_list_ =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[4])->children();
    hash_configuration_list_ =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[5])->children();
    if (children.size() > 6) {
      condition_ =
          std::dynamic_pointer_cast<gandiva::FunctionNode>(children[6])->children()[0];
    }
  }
  static arrow::Status Make(std::vector<std::shared_ptr<arrow::Field>> field_list,
                            std::shared_ptr<gandiva::FunctionNode> root_node,
                            std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                            ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<ConditionedProbeArraysVisitorImpl>(field_list, root_node,
                                                                    ret_fields, p);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::ConditionedProbeKernel::Make(
        &p_->ctx_, left_key_list_, right_key_list_, left_field_list_, right_field_list_,
        condition_, join_type_, result_field_list_, hash_configuration_list_, 0,
        &kernel_));
    p_->signature_ = kernel_->GetSignature();
    initialized_ = true;
    finish_return_type_ = ArrowComputeResultType::BatchIterator;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        ArrayList in;
        for (int i = 0; i < p_->in_record_batch_->num_columns(); i++) {
          in.push_back(p_->in_record_batch_->column(i));
        }
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Evaluate(in));
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ConditionedProbeArraysVisitorImpl: Does not support this type of "
            "input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIteratorBase>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::BatchIterator: {
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter_out;
        TIME_MICRO_OR_RAISE(p_->elapse_time_,
                            kernel_->MakeResultIterator(schema, &iter_out));
        *out = std::dynamic_pointer_cast<ResultIteratorBase>(iter_out);
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::Invalid(
            "ConditionedProbeArraysVisitorImpl MakeResultIterator does not support "
            "dependency type other than Batch.");
    }
    return arrow::Status::OK();
  }

 private:
  int join_type_;
  std::shared_ptr<gandiva::FunctionNode> root_node_;
  gandiva::NodePtr condition_;
  gandiva::FieldVector field_list_;
  gandiva::FieldVector ret_fields_;
  gandiva::NodeVector left_key_list_;
  gandiva::NodeVector right_key_list_;
  gandiva::NodeVector left_field_list_;
  gandiva::NodeVector right_field_list_;
  gandiva::NodeVector result_field_list_;
  gandiva::NodeVector hash_configuration_list_;
};

////////////////////////// ConditionedJoinArraysVisitorImpl ///////////////////////
class ConditionedJoinArraysVisitorImpl : public ExprVisitorImpl {
 public:
  ConditionedJoinArraysVisitorImpl(
      std::vector<std::shared_ptr<arrow::Field>> left_key_list,
      std::vector<std::shared_ptr<arrow::Field>> right_key_list,
      std::shared_ptr<gandiva::Node> func_node, int join_type,
      std::vector<std::shared_ptr<arrow::Field>> left_field_list,
      std::vector<std::shared_ptr<arrow::Field>> right_field_list,
      std::vector<std::shared_ptr<arrow::Field>> ret_fields, ExprVisitor* p)
      : left_key_list_(left_key_list),
        right_key_list_(right_key_list),
        join_type_(join_type),
        func_node_(func_node),
        left_field_list_(left_field_list),
        right_field_list_(right_field_list),
        ret_fields_(ret_fields),
        ExprVisitorImpl(p) {}
  static arrow::Status Make(std::vector<std::shared_ptr<arrow::Field>> left_key_list,
                            std::vector<std::shared_ptr<arrow::Field>> right_key_list,
                            std::shared_ptr<gandiva::Node> func_node, int join_type,
                            std::vector<std::shared_ptr<arrow::Field>> left_field_list,
                            std::vector<std::shared_ptr<arrow::Field>> right_field_list,
                            std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                            ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<ConditionedJoinArraysVisitorImpl>(
        left_key_list, right_key_list, func_node, join_type, left_field_list,
        right_field_list, ret_fields, p);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::ConditionedJoinArraysKernel::Make(
        &p_->ctx_, left_key_list_, right_key_list_, func_node_, join_type_,
        left_field_list_, right_field_list_, arrow::schema(ret_fields_), &kernel_));
    p_->signature_ = kernel_->GetSignature();
    initialized_ = true;
    finish_return_type_ = ArrowComputeResultType::BatchIterator;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        ArrayList in;
        for (int i = 0; i < p_->in_record_batch_->num_columns(); i++) {
          in.push_back(p_->in_record_batch_->column(i));
        }
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Evaluate(in));
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ConditionedJoinArraysVisitorImpl: Does not support this type of "
            "input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIteratorBase>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::BatchIterator: {
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter_out;
        TIME_MICRO_OR_RAISE(p_->elapse_time_,
                            kernel_->MakeResultIterator(schema, &iter_out));
        *out = std::dynamic_pointer_cast<ResultIteratorBase>(iter_out);
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::Invalid(
            "ConditionedJoinArraysVisitorImpl MakeResultIterator does not support "
            "dependency type other than Batch.");
    }
    return arrow::Status::OK();
  }

 private:
  int col_id_;
  int join_type_;
  std::shared_ptr<gandiva::Node> func_node_;
  std::vector<std::shared_ptr<arrow::Field>> left_key_list_;
  std::vector<std::shared_ptr<arrow::Field>> right_key_list_;
  std::vector<std::shared_ptr<arrow::Field>> left_field_list_;
  std::vector<std::shared_ptr<arrow::Field>> right_field_list_;
  std::vector<std::shared_ptr<arrow::Field>> ret_fields_;
};

////////////////////////// HashAggregateArraysVisitorImpl ///////////////////////
class HashAggregateArraysVisitorImpl : public ExprVisitorImpl {
 public:
  HashAggregateArraysVisitorImpl(std::vector<std::shared_ptr<arrow::Field>> field_list,
                                 std::vector<std::shared_ptr<gandiva::Node>> action_list,
                                 std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                                 ExprVisitor* p)
      : action_list_(action_list),
        field_list_(field_list),
        ret_fields_(ret_fields),
        ExprVisitorImpl(p) {}
  static arrow::Status Make(std::vector<std::shared_ptr<arrow::Field>> field_list,
                            std::vector<std::shared_ptr<gandiva::Node>> action_list,
                            std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                            ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<HashAggregateArraysVisitorImpl>(field_list, action_list,
                                                                 ret_fields, p);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::HashAggregateKernel::Make(&p_->ctx_, field_list_, action_list_,
                                                   arrow::schema(ret_fields_), &kernel_));
    p_->signature_ = kernel_->GetSignature();
    finish_return_type_ = ArrowComputeResultType::BatchIterator;
    initialized_ = true;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        ArrayList in;
        for (int i = 0; i < p_->in_record_batch_->num_columns(); i++) {
          in.push_back(p_->in_record_batch_->column(i));
        }
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Evaluate(in));
      } break;
      default:
        return arrow::Status::NotImplemented(
            "HashAggregateArraysVisitorImpl: Does not support this type of "
            "input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIteratorBase>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::BatchIterator: {
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter_out;
        TIME_MICRO_OR_RAISE(p_->elapse_time_,
                            kernel_->MakeResultIterator(schema, &iter_out));
        *out = std::dynamic_pointer_cast<ResultIteratorBase>(iter_out);
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::Invalid(
            "HashAggregateArraysVisitorImpl MakeResultIterator does not support "
            "dependency type other than Batch.");
    }
    return arrow::Status::OK();
  }

 private:
  std::vector<std::shared_ptr<gandiva::Node>> action_list_;
  std::vector<std::shared_ptr<arrow::Field>> field_list_;
  std::vector<std::shared_ptr<arrow::Field>> ret_fields_;
};

////////////////////////// WholeStageCodeGenVisitorImpl ///////////////////////
class WholeStageCodeGenVisitorImpl : public ExprVisitorImpl {
 public:
  WholeStageCodeGenVisitorImpl(std::vector<std::shared_ptr<arrow::Field>> field_list,
                               std::shared_ptr<gandiva::Node> root_node,
                               std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                               ExprVisitor* p)
      : root_node_(root_node),
        field_list_(field_list),
        ret_fields_(ret_fields),
        ExprVisitorImpl(p) {
    finish_return_type_ = ArrowComputeResultType::BatchIterator;
  }
  static arrow::Status Make(std::vector<std::shared_ptr<arrow::Field>> field_list,
                            std::shared_ptr<gandiva::Node> root_node,
                            std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                            ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<WholeStageCodeGenVisitorImpl>(field_list, root_node,
                                                               ret_fields, p);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::WholeStageCodeGenKernel::Make(&p_->ctx_, field_list_, root_node_,
                                                       ret_fields_, &kernel_));
    p_->signature_ = kernel_->GetSignature();
    initialized_ = true;
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIteratorBase>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::BatchIterator: {
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter_out;
        TIME_MICRO_OR_RAISE(p_->elapse_time_,
                            kernel_->MakeResultIterator(schema, &iter_out));
        *out = std::dynamic_pointer_cast<ResultIteratorBase>(iter_out);
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::Invalid(
            "WholeStageCodeGenVisitorImpl MakeResultIterator does not support "
            "dependency type other than Batch.");
    }
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<gandiva::Node> root_node_;
  std::vector<std::shared_ptr<arrow::Field>> field_list_;
  std::vector<std::shared_ptr<arrow::Field>> ret_fields_;
};

////////////////////////// HashRelationVisitorImpl ///////////////////////
class HashRelationVisitorImpl : public ExprVisitorImpl {
 public:
  HashRelationVisitorImpl(std::vector<std::shared_ptr<arrow::Field>> field_list,
                          std::shared_ptr<gandiva::Node> root_node,
                          std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                          ExprVisitor* p)
      : root_node_(root_node),
        field_list_(field_list),
        ret_fields_(ret_fields),
        ExprVisitorImpl(p) {
    finish_return_type_ = ArrowComputeResultType::BatchIterator;
  }
  static arrow::Status Make(std::vector<std::shared_ptr<arrow::Field>> field_list,
                            std::shared_ptr<gandiva::Node> root_node,
                            std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                            ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl =
        std::make_shared<HashRelationVisitorImpl>(field_list, root_node, ret_fields, p);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::HashRelationKernel::Make(&p_->ctx_, field_list_, root_node_,
                                                  ret_fields_, &kernel_));
    p_->signature_ = kernel_->GetSignature();
    initialized_ = true;
    finish_return_type_ = ArrowComputeResultType::BatchIterator;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        ArrayList in;
        for (int i = 0; i < p_->in_record_batch_->num_columns(); i++) {
          in.push_back(p_->in_record_batch_->column(i));
        }
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Evaluate(in));
      } break;
      default:
        return arrow::Status::NotImplemented(
            "HashRelationVisitorImpl: Does not support this type of "
            "input.");
    }
    return arrow::Status::OK();
  }
  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIteratorBase>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::BatchIterator: {
        std::shared_ptr<ResultIterator<HashRelation>> iter_out;
        TIME_MICRO_OR_RAISE(p_->elapse_time_,
                            kernel_->MakeResultIterator(schema, &iter_out));
        *out = std::dynamic_pointer_cast<ResultIteratorBase>(iter_out);
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::Invalid(
            "HashRelationVisitorImpl MakeResultIterator does not support "
            "dependency type other than Batch.");
    }
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<gandiva::Node> root_node_;
  std::vector<std::shared_ptr<arrow::Field>> field_list_;
  std::vector<std::shared_ptr<arrow::Field>> ret_fields_;
};

////////////////////////// ConcatArrayListVisitorImpl ///////////////////////
class ConcatArrayListVisitorImpl : public ExprVisitorImpl {
 public:
  ConcatArrayListVisitorImpl(std::vector<std::shared_ptr<arrow::Field>> field_list,
                             std::shared_ptr<gandiva::Node> root_node,
                             std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                             ExprVisitor* p)
      : root_node_(root_node),
        field_list_(field_list),
        ret_fields_(ret_fields),
        ExprVisitorImpl(p) {
    finish_return_type_ = ArrowComputeResultType::BatchIterator;
  }
  static arrow::Status Make(std::vector<std::shared_ptr<arrow::Field>> field_list,
                            std::shared_ptr<gandiva::Node> root_node,
                            std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                            ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<ConcatArrayListVisitorImpl>(field_list, root_node,
                                                             ret_fields, p);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::ConcatArrayListKernel::Make(&p_->ctx_, field_list_, root_node_,
                                                     ret_fields_, &kernel_));
    p_->signature_ = kernel_->GetSignature();
    initialized_ = true;
    finish_return_type_ = ArrowComputeResultType::BatchIterator;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        ArrayList in;
        for (int i = 0; i < p_->in_record_batch_->num_columns(); i++) {
          in.push_back(p_->in_record_batch_->column(i));
        }
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Evaluate(in));
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ConcatArrayListVisitorImpl: Does not support this type of "
            "input.");
    }
    return arrow::Status::OK();
  }
  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIteratorBase>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::BatchIterator: {
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter_out;
        TIME_MICRO_OR_RAISE(p_->elapse_time_,
                            kernel_->MakeResultIterator(schema, &iter_out));
        *out = std::dynamic_pointer_cast<ResultIteratorBase>(iter_out);
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::Invalid(
            "ConcatArrayListVisitorImpl MakeResultIterator does not support "
            "dependency type other than Batch.");
    }
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<gandiva::Node> root_node_;
  std::vector<std::shared_ptr<arrow::Field>> field_list_;
  std::vector<std::shared_ptr<arrow::Field>> ret_fields_;
};

////////////////////////// CodegenProbeArraysVisitorImpl ///////////////////////
class CodegenProbeArraysVisitorImpl : public ExprVisitorImpl {
 public:
  CodegenProbeArraysVisitorImpl(
      std::vector<std::shared_ptr<arrow::Field>> left_key_list,
      std::vector<std::shared_ptr<arrow::Field>> right_key_list,
      std::shared_ptr<gandiva::Node> func_node, int join_type,
      std::vector<std::shared_ptr<arrow::Field>> left_field_list,
      std::vector<std::shared_ptr<arrow::Field>> right_field_list,
      std::vector<std::shared_ptr<arrow::Field>> ret_fields, ExprVisitor* p)
      : left_key_list_(left_key_list),
        right_key_list_(right_key_list),
        join_type_(join_type),
        func_node_(func_node),
        left_field_list_(left_field_list),
        right_field_list_(right_field_list),
        ret_fields_(ret_fields),
        ExprVisitorImpl(p) {}
  static arrow::Status Make(std::vector<std::shared_ptr<arrow::Field>> left_key_list,
                            std::vector<std::shared_ptr<arrow::Field>> right_key_list,
                            std::shared_ptr<gandiva::Node> func_node, int join_type,
                            std::vector<std::shared_ptr<arrow::Field>> left_field_list,
                            std::vector<std::shared_ptr<arrow::Field>> right_field_list,
                            std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                            ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<CodegenProbeArraysVisitorImpl>(
        left_key_list, right_key_list, func_node, join_type, left_field_list,
        right_field_list, ret_fields, p);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::ConditionedProbeArraysKernel::Make(
        &p_->ctx_, left_key_list_, right_key_list_, func_node_, join_type_,
        left_field_list_, right_field_list_, arrow::schema(ret_fields_), &kernel_));
    p_->signature_ = kernel_->GetSignature();
    initialized_ = true;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        ArrayList in;
        for (int i = 0; i < p_->in_record_batch_->num_columns(); i++) {
          in.push_back(p_->in_record_batch_->column(i));
        }
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Evaluate(in));
        finish_return_type_ = ArrowComputeResultType::BatchIterator;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ConditionedProbeArraysVisitorImpl: Does not support this type of "
            "input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIteratorBase>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::BatchIterator: {
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter_out;
        TIME_MICRO_OR_RAISE(p_->elapse_time_,
                            kernel_->MakeResultIterator(schema, &iter_out));
        *out = std::dynamic_pointer_cast<ResultIteratorBase>(iter_out);
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::Invalid(
            "ConditionedProbeArraysVisitorImpl MakeResultIterator does not support "
            "dependency type other than Batch.");
    }
    return arrow::Status::OK();
  }

 private:
  int col_id_;
  int join_type_;
  std::shared_ptr<gandiva::Node> func_node_;
  std::vector<std::shared_ptr<arrow::Field>> left_key_list_;
  std::vector<std::shared_ptr<arrow::Field>> right_key_list_;
  std::vector<std::shared_ptr<arrow::Field>> left_field_list_;
  std::vector<std::shared_ptr<arrow::Field>> right_field_list_;
  std::vector<std::shared_ptr<arrow::Field>> ret_fields_;
};

}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
