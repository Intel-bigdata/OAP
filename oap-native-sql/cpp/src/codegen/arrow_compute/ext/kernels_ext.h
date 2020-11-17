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

#include <arrow/array.h>
#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/codegen_context.h"
#include "codegen/common/hash_relation.h"
#include "codegen/common/result_iterator.h"

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
class KernalBase {
 public:
  KernalBase() {}
  virtual ~KernalBase() {}
  virtual arrow::Status Evaluate(const ArrayList& in) {
    return arrow::Status::NotImplemented("Evaluate is abstract interface for ",
                                         kernel_name_, ", input is arrayList.");
  }
  virtual arrow::Status Evaluate(const ArrayList& in, ArrayList* out) {
    return arrow::Status::NotImplemented("Evaluate is abstract interface for ",
                                         kernel_name_,
                                         ", input is arrayList, output is arrayList.");
  }
  virtual arrow::Status Evaluate(const ArrayList& in,
                                 const std::shared_ptr<arrow::Array>& dict) {
    return arrow::Status::NotImplemented("Evaluate is abstract interface for ",
                                         kernel_name_, ", input is arrayList and array.");
  }
  virtual arrow::Status Evaluate(const ArrayList& in,
                                 std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("Evaluate is abstract interface for ",
                                         kernel_name_,
                                         ", input is arrayList, output is array.");
  }
  virtual arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                                 std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("Evaluate is abstract interface for ",
                                         kernel_name_,
                                         ", input is array, output is array.");
  }
  virtual std::string GetSignature() { return ""; }
  virtual arrow::Status Finish(ArrayList* out) {
    return arrow::Status::NotImplemented("Finish is abstract interface for ",
                                         kernel_name_, ", output is arrayList");
  }
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("Finish is abstract interface for ",
                                         kernel_name_, ", output is arrayList");
  }
  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented("MakeResultIterator is abstract interface for ",
                                         kernel_name_);
  }
  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<HashRelation>>* out) {
    return arrow::Status::NotImplemented("MakeResultIterator is abstract interface for ",
                                         kernel_name_);
  }
  virtual arrow::Status DoCodeGen(int level, std::vector<std::string> input,
                                  std::shared_ptr<CodeGenContext>* codegen_ctx,
                                  int* var_id) {
    return arrow::Status::NotImplemented("DoCodeGen is abstract interface for ",
                                         kernel_name_);
  }

  std::string kernel_name_;
};

class SplitArrayListWithActionKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::vector<std::string> action_name_list,
                            std::vector<std::shared_ptr<arrow::DataType>> type_list,
                            std::shared_ptr<KernalBase>* out);
  SplitArrayListWithActionKernel(arrow::compute::FunctionContext* ctx,
                                 std::vector<std::string> action_name_list,
                                 std::vector<std::shared_ptr<arrow::DataType>> type_list);
  arrow::Status Evaluate(const ArrayList& in,
                         const std::shared_ptr<arrow::Array>& dict) override;
  arrow::Status Finish(ArrayList* out) override;
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class EncodeArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<KernalBase>* out);
  EncodeArrayKernel(arrow::compute::FunctionContext* ctx);
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                         std::shared_ptr<arrow::Array>* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class WindowAggregateFunctionKernel : public KernalBase {
 public:
  class ActionFactory;
  WindowAggregateFunctionKernel(
      arrow::compute::FunctionContext* ctx,
      std::vector<std::shared_ptr<arrow::DataType>> type_list,
      std::shared_ptr<arrow::DataType> result_type,
      std::vector<std::shared_ptr<arrow::Int32Array>> accumulated_group_ids,
      std::shared_ptr<ActionFactory> action);
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::string function_name,
                            std::vector<std::shared_ptr<arrow::DataType>> type_list,
                            std::shared_ptr<arrow::DataType> result_type,
                            std::shared_ptr<KernalBase>* out);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;
  template <typename ArrowType>
  arrow::Status Finish0(ArrayList* out);

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ActionFactory> action_;
  std::vector<std::shared_ptr<arrow::Int32Array>> accumulated_group_ids_;
  std::vector<std::shared_ptr<arrow::DataType>> type_list_;
  std::shared_ptr<arrow::DataType> result_type_;
};

class HashArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::vector<std::shared_ptr<arrow::DataType>> type_list,
                            std::shared_ptr<KernalBase>* out);
  HashArrayKernel(arrow::compute::FunctionContext* ctx,
                  std::vector<std::shared_ptr<arrow::DataType>> type_list);
  arrow::Status Evaluate(const ArrayList& in,
                         std::shared_ptr<arrow::Array>* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class SumArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> data_type,
                            std::shared_ptr<KernalBase>* out);
  SumArrayKernel(arrow::compute::FunctionContext* ctx,
                 std::shared_ptr<arrow::DataType> data_type);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class CountArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> data_type,
                            std::shared_ptr<KernalBase>* out);
  CountArrayKernel(arrow::compute::FunctionContext* ctx,
                   std::shared_ptr<arrow::DataType> data_type);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class SumCountArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> data_type,
                            std::shared_ptr<KernalBase>* out);
  SumCountArrayKernel(arrow::compute::FunctionContext* ctx,
                      std::shared_ptr<arrow::DataType> data_type);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class AvgByCountArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> data_type,
                            std::shared_ptr<KernalBase>* out);
  AvgByCountArrayKernel(arrow::compute::FunctionContext* ctx,
                        std::shared_ptr<arrow::DataType> data_type);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class MinArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> data_type,
                            std::shared_ptr<KernalBase>* out);
  MinArrayKernel(arrow::compute::FunctionContext* ctx,
                 std::shared_ptr<arrow::DataType> data_type);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class MaxArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> data_type,
                            std::shared_ptr<KernalBase>* out);
  MaxArrayKernel(arrow::compute::FunctionContext* ctx,
                 std::shared_ptr<arrow::DataType> data_type);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class StddevSampPartialArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> data_type,
                            std::shared_ptr<KernalBase>* out);
  StddevSampPartialArrayKernel(arrow::compute::FunctionContext* ctx,
                               std::shared_ptr<arrow::DataType> data_type);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class StddevSampFinalArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> data_type,
                            std::shared_ptr<KernalBase>* out);
  StddevSampFinalArrayKernel(arrow::compute::FunctionContext* ctx,
                             std::shared_ptr<arrow::DataType> data_type);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class SortArraysToIndicesKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::Schema> result_schema,
                            gandiva::NodeVector sort_key_node,
                            std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                            std::vector<bool> sort_directions,
                            std::vector<bool> nulls_order,
                            std::shared_ptr<KernalBase>* out);
  SortArraysToIndicesKernel(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::Schema> result_schema,
                            gandiva::NodeVector sort_key_node,
                            std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                            std::vector<bool> sort_directions,
                            std::vector<bool> nulls_order);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;
  std::string GetSignature() override;

  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class WindowSortKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                            std::shared_ptr<arrow::Schema> result_schema,
                            std::shared_ptr<KernalBase>* out, bool nulls_first, bool asc);
  WindowSortKernel(arrow::compute::FunctionContext* ctx,
                   std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                   std::shared_ptr<arrow::Schema> result_schema, bool nulls_first,
                   bool asc);
  arrow::Status Evaluate(const ArrayList& in) override;
  std::string GetSignature() override;

  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class HashAggregateKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::vector<std::shared_ptr<arrow::Field>> input_field_list,
                            std::vector<std::shared_ptr<gandiva::Node>> action_list,
                            std::shared_ptr<arrow::Schema> result_schema,
                            std::shared_ptr<KernalBase>* out);
  HashAggregateKernel(arrow::compute::FunctionContext* ctx,
                      std::vector<std::shared_ptr<arrow::Field>> input_field_list,
                      std::vector<std::shared_ptr<gandiva::Node>> action_list,
                      std::shared_ptr<arrow::Schema> result_schema);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;
  std::string GetSignature() override;

  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class WindowRankKernel : public KernalBase {
 public:
  WindowRankKernel(arrow::compute::FunctionContext* ctx,
                   std::vector<std::shared_ptr<arrow::DataType>> type_list,
                   std::shared_ptr<WindowSortKernel::Impl> sorter, bool desc);
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::string function_name,
                            std::vector<std::shared_ptr<arrow::DataType>> type_list,
                            std::shared_ptr<KernalBase>* out, bool desc);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;

  arrow::Status SortToIndicesPrepare(std::vector<ArrayList> values);
  arrow::Status SortToIndicesFinish(
      std::vector<std::shared_ptr<ArrayItemIndex>> elements_to_sort,
      std::vector<std::shared_ptr<ArrayItemIndex>>* offsets);

  template <typename ArrayType>
  arrow::Status AreTheSameValue(std::vector<ArrayList> values, int column,
                                std::shared_ptr<ArrayItemIndex> i,
                                std::shared_ptr<ArrayItemIndex> j, bool* out);

 private:
  std::shared_ptr<WindowSortKernel::Impl> sorter_;
  arrow::compute::FunctionContext* ctx_;
  std::vector<ArrayList> input_cache_;
  std::vector<std::shared_ptr<arrow::DataType>> type_list_;
  bool desc_;
};

/*class UniqueArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<KernalBase>* out);
  UniqueArrayKernel(arrow::compute::FunctionContext* ctx);
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) override;
  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};*/

class ConditionedProbeArraysKernel : public KernalBase {
 public:
  static arrow::Status Make(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<arrow::Field>>& left_key_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_key_list,
      const std::shared_ptr<gandiva::Node>& func_node, int join_type,
      const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
      const std::shared_ptr<arrow::Schema>& result_schema,
      std::shared_ptr<KernalBase>* out);
  ConditionedProbeArraysKernel(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<arrow::Field>>& left_key_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_key_list,
      const std::shared_ptr<gandiva::Node>& func_node, int join_type,
      const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
      const std::shared_ptr<arrow::Schema>& result_schema);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;
  std::string GetSignature() override;
  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};
class ConditionedJoinArraysKernel : public KernalBase {
 public:
  static arrow::Status Make(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<arrow::Field>>& left_key_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_key_list,
      const std::shared_ptr<gandiva::Node>& func_node, int join_type,
      const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
      const std::shared_ptr<arrow::Schema>& result_schema,
      std::shared_ptr<KernalBase>* out);
  ConditionedJoinArraysKernel(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<arrow::Field>>& left_key_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_key_list,
      const std::shared_ptr<gandiva::Node>& func_node, int join_type,
      const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
      const std::shared_ptr<arrow::Schema>& result_schema);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;
  std::string GetSignature() override;
  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};
class WholeStageCodeGenKernel : public KernalBase {
 public:
  static arrow::Status Make(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
      std::shared_ptr<gandiva::Node> root_node,
      const std::vector<std::shared_ptr<arrow::Field>>& output_field_list,
      std::shared_ptr<KernalBase>* out);
  WholeStageCodeGenKernel(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
      std::shared_ptr<gandiva::Node> root_node,
      const std::vector<std::shared_ptr<arrow::Field>>& output_field_list);
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;
  std::string GetSignature() override;

  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};
class HashRelationKernel : public KernalBase {
 public:
  static arrow::Status Make(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
      std::shared_ptr<gandiva::Node> root_node,
      const std::vector<std::shared_ptr<arrow::Field>>& output_field_list,
      std::shared_ptr<KernalBase>* out);
  HashRelationKernel(arrow::compute::FunctionContext* ctx,
                     const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
                     std::shared_ptr<gandiva::Node> root_node,
                     const std::vector<std::shared_ptr<arrow::Field>>& output_field_list);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<HashRelation>>* out) override;
  std::string GetSignature() override;

  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};
class ConcatArrayListKernel : public KernalBase {
 public:
  static arrow::Status Make(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
      std::shared_ptr<gandiva::Node> root_node,
      const std::vector<std::shared_ptr<arrow::Field>>& output_field_list,
      std::shared_ptr<KernalBase>* out);
  ConcatArrayListKernel(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
      std::shared_ptr<gandiva::Node> root_node,
      const std::vector<std::shared_ptr<arrow::Field>>& output_field_list);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;

  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};
class ConditionedProbeKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            const gandiva::NodeVector& left_key_list,
                            const gandiva::NodeVector& right_key_list,
                            const gandiva::NodeVector& left_schema_list,
                            const gandiva::NodeVector& right_schema_list,
                            const gandiva::NodePtr& condition, int join_type,
                            const gandiva::NodeVector& result_schema,
                            const gandiva::NodeVector& hash_configuration_list,
                            int hash_relation_idx, std::shared_ptr<KernalBase>* out);
  ConditionedProbeKernel(arrow::compute::FunctionContext* ctx,
                         const gandiva::NodeVector& left_key_list,
                         const gandiva::NodeVector& right_key_list,
                         const gandiva::NodeVector& left_schema_list,
                         const gandiva::NodeVector& right_schema_list,
                         const gandiva::NodePtr& condition, int join_type,
                         const gandiva::NodeVector& result_schema,
                         const gandiva::NodeVector& hash_configuration_list,
                         int hash_relation_idx);
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;
  arrow::Status DoCodeGen(int level, std::vector<std::string> input,
                          std::shared_ptr<CodeGenContext>* codegen_ctx_out,
                          int* var_id) override;
  std::string GetSignature() override;
  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};
class ProjectKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            const gandiva::NodeVector& input_field_node_list,
                            const gandiva::NodeVector& project_list,
                            std::shared_ptr<KernalBase>* out);
  ProjectKernel(arrow::compute::FunctionContext* ctx,
                const gandiva::NodeVector& input_field_node_list,
                const gandiva::NodeVector& project_list);
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;
  arrow::Status DoCodeGen(int level, std::vector<std::string> input,
                          std::shared_ptr<CodeGenContext>* codegen_ctx,
                          int* var_id) override;
  std::string GetSignature() override;
  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};
class FilterKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            const gandiva::NodeVector& input_field_node_list,
                            const gandiva::NodePtr& condition,
                            std::shared_ptr<KernalBase>* out);
  FilterKernel(arrow::compute::FunctionContext* ctx,
               const gandiva::NodeVector& input_field_node_list,
               const gandiva::NodePtr& condition);
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;
  arrow::Status DoCodeGen(int level, std::vector<std::string> input,
                          std::shared_ptr<CodeGenContext>* codegen_ctx,
                          int* var_id) override;
  std::string GetSignature() override;
  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
