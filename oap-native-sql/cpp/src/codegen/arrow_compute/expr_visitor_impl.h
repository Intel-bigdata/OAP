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
    return arrow::Status::NotImplemented("ExprVisitorImpl Init is abstract.");
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

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
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
    *id = schema->GetFieldIndex(col_name);
    *out_field = schema->GetFieldByName(col_name);
    if (*id < 0) {
      return arrow::Status::Invalid("GetColumnIdAndFieldByName doesn't found col_name ",
                                    col_name);
    }
    return arrow::Status::OK();
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
        finish_return_type_ = ArrowComputeResultType::Batch;
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

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::Batch: {
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->MakeResultIterator(schema, out));
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
    } else if (func_name_.compare("count") == 0) {
      RETURN_NOT_OK(extra::CountArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
    } else if (func_name_.compare("sum_count") == 0) {
      p_->result_fields_.push_back(arrow::field("cnt", arrow::int64()));
      RETURN_NOT_OK(extra::SumCountArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
    } else if (func_name_.compare("avgByCount") == 0) {
      p_->result_fields_.erase(p_->result_fields_.end() - 1);
      RETURN_NOT_OK(extra::AvgByCountArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
    } else if (func_name_.compare("min") == 0) {
      RETURN_NOT_OK(extra::MinArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
    } else if (func_name_.compare("max") == 0) {
      RETURN_NOT_OK(extra::MaxArrayKernel::Make(&p_->ctx_, data_type, &kernel_));
    }
    initialized_ = true;
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
        RETURN_NOT_OK(kernel_->Evaluate(in));
        finish_return_type_ = ArrowComputeResultType::Batch;
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
        RETURN_NOT_OK(kernel_->Finish(&p_->result_batch_));
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

    auto result_field = field("res", arrow::uint32());
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

  arrow::Status Finish() override {
    std::cout << "Concat keys took " << TIME_TO_STRING(concat_elapse_time) << std::endl;

    return arrow::Status::OK();
  }

 private:
  std::vector<int> col_id_list_;
  std::shared_ptr<extra::KernalBase> concat_kernel_;
  uint64_t concat_elapse_time = 0;
};

////////////////////////// SortArraysToIndicesVisitorImpl ///////////////////////
class SortArraysToIndicesVisitorImpl : public ExprVisitorImpl {
 public:
  SortArraysToIndicesVisitorImpl(ExprVisitor* p, bool nulls_first, bool asc)
      : ExprVisitorImpl(p), nulls_first_(nulls_first), asc_(asc) {}
  static arrow::Status Make(ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out,
                            bool nulls_first, bool asc) {
    auto impl = std::make_shared<SortArraysToIndicesVisitorImpl>(p, nulls_first, asc);
    *out = impl;
    return arrow::Status::OK();
  }
  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    std::vector<std::shared_ptr<arrow::Field>> field_list;
    for (auto col_name : p_->param_field_names_) {
      int col_id;
      std::shared_ptr<arrow::Field> field;
      RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, col_name, &col_id, &field));
      p_->result_fields_.push_back(field);
      field_list.push_back(field);
    }
    RETURN_NOT_OK(extra::SortArraysToIndicesKernel::Make(
        &p_->ctx_, field_list, p_->schema_, &kernel_, nulls_first_, asc_));
    initialized_ = true;
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
        finish_return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "SortArraysToIndicesVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::Batch: {
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->MakeResultIterator(schema, out));
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
  bool nulls_first_;
  bool asc_;
};

////////////////////////// ConditionedProbeArraysVisitorImpl ///////////////////////
class ConditionedProbeArraysVisitorImpl : public ExprVisitorImpl {
 public:
  ConditionedProbeArraysVisitorImpl(
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
    auto impl = std::make_shared<ConditionedProbeArraysVisitorImpl>(
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

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::BatchIterator: {
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->MakeResultIterator(schema, out));
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
            "HashAggregateArraysVisitorImpl: Does not support this type of "
            "input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    switch (finish_return_type_) {
      case ArrowComputeResultType::BatchIterator: {
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->MakeResultIterator(schema, out));
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
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
