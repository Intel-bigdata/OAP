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
#include <arrow/type_fwd.h>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "precompile/type_traits.h"

using sparkcolumnarplugin::codegen::arrowcompute::extra::ArrayItemIndex;
using sparkcolumnarplugin::precompile::enable_if_number;
using sparkcolumnarplugin::precompile::enable_if_string_like;
using sparkcolumnarplugin::precompile::StringArray;
using sparkcolumnarplugin::precompile::TypeTraits;

class HashRelationColumn {
 public:
  virtual bool IsNull(int array_id, int id) = 0;
  virtual arrow::Status AppendColumn(std::shared_ptr<arrow::Array> in) {
    return arrow::Status::NotImplemented("HashRelationColumn AppendColumn is abstract.");
  };
};

template <typename T, typename Enable = void>
class TypedHashRelationColumn {};

template <typename DataType>
class TypedHashRelationColumn<DataType, enable_if_number<DataType>>
    : public HashRelationColumn {
 public:
  using T = typename TypeTraits<DataType>::CType;
  TypedHashRelationColumn() {}
  bool IsNull(int array_id, int id) override {
    return array_vector_[array_id]->IsNull(id);
  }
  arrow::Status AppendColumn(std::shared_ptr<arrow::Array> in) override {
    auto typed_in = std::make_shared<ArrayType>(in);
    array_vector_.push_back(typed_in);
    return arrow::Status::OK();
  }
  T GetValue(int array_id, int id) { return array_vector_[array_id]->GetView(id); }

 private:
  using ArrayType = typename TypeTraits<DataType>::ArrayType;
  std::vector<std::shared_ptr<ArrayType>> array_vector_;
};

template <typename DataType>
class TypedHashRelationColumn<DataType, enable_if_string_like<DataType>>
    : public HashRelationColumn {
 public:
  TypedHashRelationColumn() {}
  bool IsNull(int array_id, int id) override {
    return array_vector_[array_id]->IsNull(id);
  }
  arrow::Status AppendColumn(std::shared_ptr<arrow::Array> in) override {
    auto typed_in = std::make_shared<StringArray>(in);
    array_vector_.push_back(typed_in);
    return arrow::Status::OK();
  }
  std::string GetValue(int array_id, int id) {
    return array_vector_[array_id]->GetString(id);
  }

 private:
  std::vector<std::shared_ptr<StringArray>> array_vector_;
};

/////////////////////////////////////////////////////////////////////////

class HashRelation {
 public:
  HashRelation(
      const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_list) {
    hash_relation_column_list_ = hash_relation_list;
  }

  virtual arrow::Status AppendKeyColumn(std::shared_ptr<arrow::Array> in) {
    return arrow::Status::NotImplemented("HashRelation::AppendKeyColumn is abstract.");
  }

  arrow::Status AppendPayloadColumn(int idx, std::shared_ptr<arrow::Array> in) {
    return hash_relation_column_list_[idx]->AppendColumn(in);
  }

  template <typename T>
  arrow::Status GetColumn(int idx, std::shared_ptr<T>* out) {
    *out = std::dynamic_pointer_cast<T>(hash_relation_column_list_[idx]);
    return arrow::Status::OK();
  }

  std::vector<ArrayItemIndex> GetItemListByIndex(int i) {
    return memo_index_to_arrayid_[i];
  }

  uint64_t GetHashRelationLength() { return num_items_; }

 protected:
  uint64_t num_items_ = 0;
  uint64_t num_arrays_ = 0;
  std::vector<std::vector<ArrayItemIndex>> memo_index_to_arrayid_;
  std::vector<std::shared_ptr<HashRelationColumn>> hash_relation_column_list_;
};

template <typename T, typename Enable = void>
class TypedHashRelation {};

arrow::Status MakeHashRelationColumn(uint32_t data_type_id,
                                     std::shared_ptr<HashRelationColumn>* out);

arrow::Status MakeHashRelation(
    uint32_t key_type_id, arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_column,
    std::shared_ptr<HashRelation>* out);