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

#include <arrow/util/string_view.h>

#include "codegen/common/hash_relation.h"
#include "precompile/hash_map.h"
using sparkcolumnarplugin::codegen::arrowcompute::extra::ArrayItemIndex;
using sparkcolumnarplugin::precompile::enable_if_string_like;
using sparkcolumnarplugin::precompile::StringArray;
using sparkcolumnarplugin::precompile::StringHashMap;
using sparkcolumnarplugin::precompile::TypeTraits;

/////////////////////////////////////////////////////////////////////////

template <typename DataType>
class TypedHashRelation<DataType, enable_if_string_like<DataType>> : public HashRelation {
 public:
  using T = std::string;
  TypedHashRelation(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_column)
      : HashRelation(hash_relation_column) {
    hash_table_ = std::make_shared<StringHashMap>(ctx->memory_pool());
  }
  arrow::Status AppendKeyColumn(std::shared_ptr<arrow::Array> in) override {
    auto typed_array = std::make_shared<ArrayType>(in);
    if (typed_array->null_count() == 0) {
      for (int i = 0; i < typed_array->length(); i++) {
        RETURN_NOT_OK(Insert(typed_array->GetView(i), num_arrays_, i));
      }
    } else {
      for (int i = 0; i < typed_array->length(); i++) {
        if (typed_array->IsNull(i)) {
          RETURN_NOT_OK(InsertNull(num_arrays_, i));

        } else {
          RETURN_NOT_OK(Insert(typed_array->GetView(i), num_arrays_, i));
        }
      }
    }
    num_arrays_++;
    return arrow::Status::OK();
  }

  int Get(T v) { return hash_table_->Get(arrow::util::string_view(v)); }

  int Get(arrow::util::string_view v) { return hash_table_->Get(v); }

  int GetNull() { return hash_table_->GetNull(); }

  std::vector<ArrayItemIndex> GetItemListByIndex(int i) override {
    return memo_index_to_arrayid_[i];
  }

 private:
  arrow::Status Insert(arrow::util::string_view v, uint32_t array_id, uint32_t id) {
    int i;
    RETURN_NOT_OK(hash_table_->GetOrInsert(
        v, [](int32_t i) {}, [](int32_t i) {}, &i));
    if (i < num_items_) {
      memo_index_to_arrayid_[i].emplace_back(array_id, id);
    } else {
      num_items_++;
      memo_index_to_arrayid_.push_back({ArrayItemIndex(array_id, id)});
    }
    return arrow::Status::OK();
  }

  arrow::Status InsertNull(uint32_t array_id, uint32_t id) {
    int i = hash_table_->GetOrInsertNull([](int32_t i) {}, [](int32_t i) {});
    if (i < num_items_) {
      memo_index_to_arrayid_[i].emplace_back(array_id, id);
    } else {
      num_items_++;
      memo_index_to_arrayid_.push_back({ArrayItemIndex(array_id, id)});
    }
    return arrow::Status::OK();
  }

  int num_items_ = 0;
  std::shared_ptr<StringHashMap> hash_table_;
  using ArrayType = sparkcolumnarplugin::precompile::StringArray;
  std::vector<std::vector<ArrayItemIndex>> memo_index_to_arrayid_;
};