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

#include "codegen/common/result_iterator.h"

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
class KernalBase {
 public:
  KernalBase() {}
  ~KernalBase() {}
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
  virtual arrow::Status Finish(ArrayList* out) {
    return arrow::Status::NotImplemented("Finish is abstract interface for ",
                                         kernel_name_, ", output is arrayList");
  }
  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented("MakeResultIterator is abstract interface for ",
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

class SortArraysToIndicesKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                            std::shared_ptr<arrow::Schema> result_schema,
                            std::shared_ptr<KernalBase>* out, bool nulls_first, bool asc);
  SortArraysToIndicesKernel(arrow::compute::FunctionContext* ctx,
                            std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                            std::shared_ptr<arrow::Schema> result_schema,
                            bool nulls_first, bool asc);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override;

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

  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
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
  class Impl;

 private:
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
