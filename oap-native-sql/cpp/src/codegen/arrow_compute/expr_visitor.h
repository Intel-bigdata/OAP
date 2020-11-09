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

#pragma once

#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <gandiva/node.h>
#include <gandiva/node_visitor.h>

#include <iostream>
#include <memory>
#include <unordered_map>

#include "codegen/common/result_iterator.h"
#include "codegen/common/visitor_base.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {

class ExprVisitor;
class BuilderVisitor;
class ExprVisitorImpl;

using ExprVisitorMap = std::unordered_map<std::string, std::shared_ptr<ExprVisitor>>;
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
enum class ArrowComputeResultType { Array, Batch, BatchList, BatchIterator, None };
enum class BuilderVisitorNodeType { FunctionNode, FieldNode };

class BuilderVisitor : public VisitorBase {
 public:
  BuilderVisitor(arrow::MemoryPool* memory_pool,
                 std::shared_ptr<arrow::Schema> schema_ptr,
                 std::shared_ptr<gandiva::Node> func,
                 std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                 ExprVisitorMap* expr_visitor_cache)
      : memory_pool_(memory_pool),
        schema_(schema_ptr),
        func_(func),
        ret_fields_(ret_fields),
        expr_visitor_cache_(expr_visitor_cache) {}
  BuilderVisitor(arrow::MemoryPool* memory_pool,
                 std::shared_ptr<arrow::Schema> schema_ptr,
                 std::shared_ptr<gandiva::Node> func,
                 std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                 std::shared_ptr<gandiva::Node> finish_func,
                 ExprVisitorMap* expr_visitor_cache)
      : memory_pool_(memory_pool),
        schema_(schema_ptr),
        func_(func),
        ret_fields_(ret_fields),
        finish_func_(finish_func),
        expr_visitor_cache_(expr_visitor_cache) {}
  ~BuilderVisitor() {}
  arrow::Status Eval() {
    RETURN_NOT_OK(func_->Accept(*this));
    return arrow::Status::OK();
  }
  arrow::Status GetResult(std::shared_ptr<ExprVisitor>* out);
  std::string GetResult();
  BuilderVisitorNodeType GetNodeType() { return node_type_; }

 private:
  arrow::Status Visit(const gandiva::FunctionNode& node) override;
  arrow::Status Visit(const gandiva::FieldNode& node) override;
  // input
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<gandiva::Node> func_;
  std::shared_ptr<gandiva::Node> finish_func_;
  std::vector<std::shared_ptr<arrow::Field>> ret_fields_;
  arrow::MemoryPool* memory_pool_;
  // output
  std::shared_ptr<ExprVisitor> expr_visitor_;
  BuilderVisitorNodeType node_type_;
  // ExprVisitor Cache, used when multiple node depends on same node.
  ExprVisitorMap* expr_visitor_cache_;
  std::string node_id_;
};

class ExprVisitor : public std::enable_shared_from_this<ExprVisitor> {
 public:
  static arrow::Status Make(arrow::MemoryPool* memory_pool,
                            std::shared_ptr<arrow::Schema> schema_ptr,
                            std::string func_name,
                            std::vector<std::string> param_field_names,
                            std::shared_ptr<ExprVisitor> dependency,
                            std::shared_ptr<gandiva::Node> finish_func,
                            std::shared_ptr<ExprVisitor>* out);
  static arrow::Status Make(arrow::MemoryPool* memory_pool,
                            const std::shared_ptr<gandiva::FunctionNode>& node,
                            std::shared_ptr<arrow::Schema> schema_ptr,
                            std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                            std::shared_ptr<ExprVisitor>* out);
  static arrow::Status MakeWindow(arrow::MemoryPool* memory_pool,
                                  std::shared_ptr<arrow::Schema> schema_ptr,
                                  std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                                  const gandiva::FunctionNode& node,
                                  std::shared_ptr<ExprVisitor>* out);

  ExprVisitor(arrow::compute::FunctionContext ctx,
              std::shared_ptr<arrow::Schema> schema_ptr, std::string func_name,
              std::vector<std::string> param_field_names,
              std::shared_ptr<ExprVisitor> dependency,
              std::shared_ptr<gandiva::Node> finish_func);

  ExprVisitor(arrow::compute::FunctionContext ctx, std::string func_name);

  ExprVisitor(arrow::compute::FunctionContext ctx,
              std::shared_ptr<arrow::Schema> schema_ptr, std::string func_name);

  ~ExprVisitor() {
#ifdef DEBUG
    std::cout << "Destruct " << func_name_ << " ExprVisitor, ptr is " << this
              << std::endl;
#endif
  }
  arrow::Status MakeExprVisitorImpl(const std::string& func_name, ExprVisitor* p);
  arrow::Status MakeExprVisitorImpl(const std::string& func_name,
                                    std::shared_ptr<gandiva::FunctionNode> func_node,
                                    std::vector<std::shared_ptr<arrow::Field>> field_list,
                                    std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                                    ExprVisitor* p);
  arrow::Status MakeExprVisitorImpl(
      const std::string& func_name, std::shared_ptr<gandiva::FunctionNode> func_node,
      std::vector<std::shared_ptr<arrow::Field>> left_field_list,
      std::vector<std::shared_ptr<arrow::Field>> right_field_list,
      std::vector<std::shared_ptr<arrow::Field>> ret_fields, ExprVisitor* p);
  arrow::Status MakeExprVisitorImpl(
      const std::string& func_name,
      std::vector<std::shared_ptr<gandiva::FunctionNode>> window_functions,
      std::shared_ptr<gandiva::FunctionNode> partition_spec,
      std::shared_ptr<gandiva::FunctionNode> order_spec,
      std::shared_ptr<gandiva::FunctionNode> frame_spec,
      std::vector<std::shared_ptr<arrow::Field>> ret_fields,
      ExprVisitor* p);
  arrow::Status AppendAction(const std::string& func_name,
                             std::vector<std::string> param_name);
  arrow::Status Init();
  arrow::Status Eval(const std::shared_ptr<arrow::Array>& selection_in,
                     const std::shared_ptr<arrow::RecordBatch>& in);
  arrow::Status Eval(const std::shared_ptr<arrow::RecordBatch>& in);
  arrow::Status Eval();
  std::string GetSignature() { return signature_; }
  arrow::Status SetMember(const std::shared_ptr<arrow::RecordBatch>& ms);
  arrow::Status SetDependency(
      const std::shared_ptr<ResultIterator<arrow::RecordBatch>>& dependency_iter,
      int index);
  arrow::Status GetResultFromDependency();
  arrow::Status Reset();
  arrow::Status ResetDependency();
  arrow::Status Finish(std::shared_ptr<ExprVisitor>* finish_visitor);
  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIteratorBase>* out);
  std::string GetName() { return func_name_; }

  ArrowComputeResultType GetResultType();
  arrow::Status GetResult(std::shared_ptr<arrow::Array>* out,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields);
  arrow::Status GetResult(ArrayList* out,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields);
  arrow::Status GetResult(std::vector<ArrayList>* out, std::vector<int>* out_sizes,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields);

  void PrintMetrics() {
    if (dependency_) {
      dependency_->PrintMetrics();
    }
    std::cout << func_name_ << " took " << TIME_TO_STRING(elapse_time_) << ", ";
  }

  // Input data holder.
  std::shared_ptr<arrow::Schema> schema_;
  std::string func_name_;
  std::string signature_;
  std::shared_ptr<ExprVisitor> dependency_;
  std::shared_ptr<arrow::Array> in_selection_array_;
  std::shared_ptr<arrow::RecordBatch> in_record_batch_;
  std::vector<std::shared_ptr<arrow::Field>> ret_fields_;

  // For dual input kernels like probe
  std::shared_ptr<arrow::RecordBatch> member_record_batch_;

  std::vector<std::string> param_field_names_;
  std::shared_ptr<gandiva::Node> finish_func_;
  std::vector<std::string> action_name_list_;
  std::vector<std::string> action_param_list_;

  // Input data from dependency.
  ArrowComputeResultType dependency_result_type_ = ArrowComputeResultType::None;
  std::vector<std::shared_ptr<arrow::Field>> in_fields_;
  std::vector<ArrayList> in_batch_array_;
  std::vector<int> in_batch_size_array_;
  ArrayList in_batch_;
  std::shared_ptr<arrow::Array> in_array_;
  // group_indices is used to tell item in array_list_ and batch_list_ belong to which
  // group
  std::vector<int> group_indices_;

  // Output data types.
  ArrowComputeResultType return_type_ = ArrowComputeResultType::None;
  // This is used when we want to output an Array after evaluate.
  std::shared_ptr<arrow::Array> result_array_;
  // This is used when we want to output an ArrayList after evaluation.
  ArrayList result_batch_;
  // This is used when we want to output an BatchList after evaluation.
  std::vector<ArrayList> result_batch_list_;
  std::vector<int> result_batch_size_list_;
  // Return fields
  std::vector<std::shared_ptr<arrow::Field>> result_fields_;

  // Long live variables
  arrow::compute::FunctionContext ctx_;
  std::shared_ptr<ExprVisitorImpl> impl_;
  std::shared_ptr<ExprVisitor> finish_visitor_;
  bool initialized_ = false;

  // metrics, in microseconds
  uint64_t elapse_time_ = 0;

  arrow::Status GetResult(std::shared_ptr<arrow::Array>* out,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields,
                          std::vector<int>* group_indices);
  arrow::Status GetResult(ArrayList* out,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields,
                          std::vector<int>* group_indices);
  arrow::Status GetResult(std::vector<ArrayList>* out, std::vector<int>* out_sizes,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields,
                          std::vector<int>* group_indices);
};

arrow::Status MakeExprVisitor(arrow::MemoryPool* memory_pool,
                              std::shared_ptr<arrow::Schema> schema_ptr,
                              std::shared_ptr<gandiva::Expression> expr,
                              std::vector<std::shared_ptr<arrow::Field>> ret_fields_,
                              ExprVisitorMap* expr_visitor_cache,
                              std::shared_ptr<ExprVisitor>* out);

arrow::Status MakeExprVisitor(arrow::MemoryPool* memory_pool,
                              std::shared_ptr<arrow::Schema> schema_ptr,
                              std::shared_ptr<gandiva::Expression> expr,
                              std::vector<std::shared_ptr<arrow::Field>> ret_fields_,
                              std::shared_ptr<gandiva::Expression> finish_expr,
                              ExprVisitorMap* expr_visitor_cache,
                              std::shared_ptr<ExprVisitor>* out);

}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
