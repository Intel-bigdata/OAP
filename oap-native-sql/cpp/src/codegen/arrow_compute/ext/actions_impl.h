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

#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/checked_cast.h>

#include <iostream>
#include <memory>
#include <sstream>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_signed_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_unsigned_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_floating_point<I>> {
  using Type = arrow::DoubleType;
};

class ActionBase {
 public:
  virtual int RequiredColNum() { return 1; }

  virtual arrow::Status Submit(ArrayList in, int max_group_id,
                               std::function<arrow::Status(int)>* on_valid,
                               std::function<arrow::Status()>* on_null) {
    return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
  }
  virtual arrow::Status Submit(std::vector<std::shared_ptr<arrow::Array>> in,
                               std::function<arrow::Status(uint64_t, uint64_t)>* on_valid,
                               std::function<arrow::Status()>* on_null) {
    return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
  }
  virtual arrow::Status Submit(const std::shared_ptr<arrow::Array>& in,
                               std::stringstream* ss,
                               std::function<arrow::Status(int)>* out) {
    return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
  }
  virtual arrow::Status Submit(const std::shared_ptr<arrow::Array>& in,
                               std::function<arrow::Status(uint32_t)>* on_valid,
                               std::function<arrow::Status()>* on_null) {
    return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
  }
  virtual arrow::Status Finish(ArrayList* out) {
    return arrow::Status::NotImplemented("ActionBase Finish is abstract.");
  }
  virtual arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) {
    return arrow::Status::NotImplemented("ActionBase Finish is abstract.");
  }
  virtual arrow::Status FinishAndReset(ArrayList* out) {
    return arrow::Status::NotImplemented("ActionBase FinishAndReset is abstract.");
  }
  virtual uint64_t GetResultLength() { return 0; }
};

//////////////// UniqueAction ///////////////
template <typename DataType, typename CType>
class UniqueAction : public ActionBase {
 public:
  UniqueAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct UniqueAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~UniqueAction() {
#ifdef DEBUG
    std::cout << "Destruct UniqueAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
    }

    in_ = std::dynamic_pointer_cast<ArrayType>(in_list[0]);
    row_id_ = 0;
    // prepare evaluate lambda
    auto type = arrow::TypeTraits<DataType>::type_singleton();
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_->IsNull(row_id_);
        if (!is_null && cache_validity_[dest_group_id] == false) {
          cache_validity_[dest_group_id] = true;
          cache_.emplace(cache_.begin() + dest_group_id, in_->GetView(row_id_));
        }
        row_id_++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        if (cache_validity_[dest_group_id] == false) {
          cache_validity_[dest_group_id] = true;
          cache_.emplace(cache_.begin() + dest_group_id, in_->GetView(row_id_));
        }
        row_id_++;
        return arrow::Status::OK();
      };
    }

    *on_null = [this]() {
      row_id_++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto length = GetResultLength();
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[i]) {
        builder_->Append(cache_[i]);
      } else {
        builder_->AppendNull();
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    // appendValues to builder_
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  int row_id_ = 0;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  // output
  std::unique_ptr<BuilderType> builder_;
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
};

//////////////// CountAction ///////////////
template <typename DataType>
class CountAction : public ActionBase {
 public:
  CountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct CountAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~CountAction() {
#ifdef DEBUG
    std::cout << "Destruct CountAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    row_id = 0;
    // prepare evaluate lambda
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_[dest_group_id] += 1;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += 1;
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ResArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  int32_t row_id;
  // result
  using CType = typename arrow::TypeTraits<DataType>::CType;
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<ResBuilderType> builder_;
};

//////////////// CountLiteralAction ///////////////
template <typename DataType>
class CountLiteralAction : public ActionBase {
 public:
  CountLiteralAction(arrow::compute::FunctionContext* ctx, int arg)
      : ctx_(ctx), arg_(arg) {
#ifdef DEBUG
    std::cout << "Construct CountLiteralAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~CountLiteralAction() {
#ifdef DEBUG
    std::cout << "Destruct CountLiteralAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 0; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    // prepare evaluate lambda
    *on_valid = [this](int dest_group_id) {
      cache_validity_[dest_group_id] = true;
      cache_[dest_group_id] += arg_;
      return arrow::Status::OK();
    };

    *on_null = [this]() { return arrow::Status::OK(); };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ResArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  int arg_;
  // result
  using CType = typename arrow::TypeTraits<DataType>::CType;
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<ResBuilderType> builder_;
};

//////////////// MinAction ///////////////
template <typename DataType>
class MinAction : public ActionBase {
 public:
  MinAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct MinAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(),
                       arrow::TypeTraits<ResDataType>::type_singleton(), &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~MinAction() {
#ifdef DEBUG
    std::cout << "Destruct MinAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
          cache_validity_[dest_group_id] = true;
        }
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          if (data_[row_id] < cache_[dest_group_id]) {
            cache_[dest_group_id] = data_[row_id];
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
          cache_validity_[dest_group_id] = true;
        }
        if (data_[row_id] < cache_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<ResBuilderType> builder_;
};

//////////////// MaxAction ///////////////
template <typename DataType>
class MaxAction : public ActionBase {
 public:
  MaxAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct MaxAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(),
                       arrow::TypeTraits<ResDataType>::type_singleton(), &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~MaxAction() {
#ifdef DEBUG
    std::cout << "Destruct MaxAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
          cache_validity_[dest_group_id] = true;
        }
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          if (data_[row_id] > cache_[dest_group_id]) {
            cache_[dest_group_id] = data_[row_id];
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
          cache_validity_[dest_group_id] = true;
        }
        if (data_[row_id] > cache_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<ResBuilderType> builder_;
};

//////////////// SumAction ///////////////
template <typename DataType>
class SumAction : public ActionBase {
 public:
  SumAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(),
                       arrow::TypeTraits<ResDataType>::type_singleton(), &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~SumAction() {
#ifdef DEBUG
    std::cout << "Destruct SumAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_[dest_group_id] += data_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += data_[row_id];
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<ResBuilderType> builder_;
};

//////////////// AvgAction ///////////////
template <typename DataType>
class AvgAction : public ActionBase {
 public:
  AvgAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct AvgAction" << std::endl;
#endif
  }
  ~AvgAction() {
#ifdef DEBUG
    std::cout << "Destruct AvgAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          cache_sum_[dest_group_id] += data_[row_id];
          cache_count_[dest_group_id] += 1;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_sum_[dest_group_id] += data_[row_id];
        cache_count_[dest_group_id] += 1;
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    for (int i = 0; i < cache_sum_.size(); i++) {
      cache_sum_[i] /= cache_count_[i];
    }
    std::shared_ptr<arrow::Array> arr_out;
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(builder->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_sum_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    for (int i = 0; i < length; i++) {
      cache_sum_[i + offset] /= cache_count_[i + offset];
    }
    std::shared_ptr<arrow::Array> arr_out;
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder->Append(cache_sum_[offset + i]));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }

    RETURN_NOT_OK(builder->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  CType* data_;
  std::shared_ptr<arrow::Array> in_;
  int row_id;
  // result
  std::vector<double> cache_sum_;
  std::vector<uint64_t> cache_count_;
  std::vector<bool> cache_validity_;
};

//////////////// SumCountAction ///////////////
template <typename DataType>
class SumCountAction : public ActionBase {
 public:
  SumCountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumCountAction" << std::endl;
#endif
  }
  ~SumCountAction() {
#ifdef DEBUG
    std::cout << "Destruct SumCountAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          cache_sum_[dest_group_id] += data_[row_id];
          cache_count_[dest_group_id] += 1;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_sum_[dest_group_id] += data_[row_id];
        cache_count_[dest_group_id] += 1;
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> sum_array;
    auto sum_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(sum_builder->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(sum_builder->Finish(&sum_array));

    // get count
    std::shared_ptr<arrow::Array> count_array;
    auto count_builder = new arrow::Int64Builder(ctx_->memory_pool());
    RETURN_NOT_OK(count_builder->AppendValues(cache_count_, cache_validity_));
    RETURN_NOT_OK(count_builder->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_sum_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    auto sum_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    auto count_builder = new arrow::Int64Builder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(sum_builder->Append(cache_sum_[offset + i]));
        RETURN_NOT_OK(count_builder->Append(cache_count_[offset + i]));
      } else {
        RETURN_NOT_OK(sum_builder->AppendNull());
        RETURN_NOT_OK(count_builder->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder->Finish(&sum_array));
    RETURN_NOT_OK(count_builder->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  CType* data_;
  std::shared_ptr<arrow::Array> in_;
  int row_id;
  // result
  std::vector<double> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
};

//////////////// AvgByCountAction ///////////////
template <typename DataType>
class AvgByCountAction : public ActionBase {
 public:
  AvgByCountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct AvgByCountAction" << std::endl;
#endif
  }
  ~AvgByCountAction() {
#ifdef DEBUG
    std::cout << "Destruct AvgByCountAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 2; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
    }

    in_sum_ = in_list[0];
    in_count_ = in_list[1];
    // prepare evaluate lambda
    data_sum_ = const_cast<double*>(in_sum_->data()->GetValues<double>(1));
    data_count_ = const_cast<int64_t*>(in_count_->data()->GetValues<int64_t>(1));
    row_id = 0;
    if (in_sum_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_sum_->IsNull(row_id);
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          cache_sum_[dest_group_id] += data_sum_[row_id];
          cache_count_[dest_group_id] += data_count_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_sum_[dest_group_id] += data_sum_[row_id];
        cache_count_[dest_group_id] += data_count_[row_id];
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> out_arr;
    for (int i = 0; i < cache_sum_.size(); i++) {
      cache_sum_[i] /= cache_count_[i];
    }
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(builder->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&out_arr));
    out->push_back(out_arr);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_sum_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    for (int i = 0; i < length; i++) {
      cache_sum_[i + offset] /= cache_count_[i + offset];
    }
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder->Append(cache_sum_[offset + i]));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> out_arr;
    RETURN_NOT_OK(builder->Finish(&out_arr));
    out->push_back(out_arr);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  double* data_sum_;
  int64_t* data_count_;
  int row_id;
  std::shared_ptr<arrow::Array> in_sum_;
  std::shared_ptr<arrow::Array> in_count_;
  // result
  std::vector<double> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
};

///////////////////// Public Functions //////////////////
#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)

arrow::Status MakeUniqueAction(arrow::compute::FunctionContext* ctx,
                               std::shared_ptr<arrow::DataType> type,
                               std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                   \
  case InType::type_id: {                                                 \
    using CType = typename arrow::TypeTraits<InType>::CType;              \
    auto action_ptr = std::make_shared<UniqueAction<InType, CType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);             \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    case arrow::StringType::type_id: {
      auto action_ptr =
          std::make_shared<UniqueAction<arrow::StringType, std::string>>(ctx);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Date32Type::type_id: {
      auto action_ptr =
          std::make_shared<UniqueAction<arrow::Date32Type, int32_t>>(ctx);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;

    default: {
      std::cout << "Not Found " << type->ToString() << ", type id is " << type->id()
                << std::endl;
    } break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeCountAction(arrow::compute::FunctionContext* ctx,
                              std::shared_ptr<ActionBase>* out) {
  auto action_ptr = std::make_shared<CountAction<arrow::UInt64Type>>(ctx);
  *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
  return arrow::Status::OK();
}

arrow::Status MakeCountLiteralAction(arrow::compute::FunctionContext* ctx, int arg,
                                     std::shared_ptr<ActionBase>* out) {
  auto action_ptr = std::make_shared<CountLiteralAction<arrow::UInt64Type>>(ctx, arg);
  *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
  return arrow::Status::OK();
}

arrow::Status MakeSumAction(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<SumAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeAvgAction(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<AvgAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeMinAction(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<MinAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeMaxAction(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<MaxAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeSumCountAction(arrow::compute::FunctionContext* ctx,
                                 std::shared_ptr<arrow::DataType> type,
                                 std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                              \
  case InType::type_id: {                                            \
    auto action_ptr = std::make_shared<SumCountAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);        \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeAvgByCountAction(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<arrow::DataType> type,
                                   std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                \
  case InType::type_id: {                                              \
    auto action_ptr = std::make_shared<AvgByCountAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);          \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

#undef PROCESS_SUPPORTED_TYPES

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
