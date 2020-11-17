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
#include "precompile/unsafe_array.h"
#include "third_party/row_wise_memory/hashMap.h"

using sparkcolumnarplugin::codegen::arrowcompute::extra::ArrayItemIndex;
using sparkcolumnarplugin::precompile::enable_if_number;
using sparkcolumnarplugin::precompile::enable_if_string_like;
using sparkcolumnarplugin::precompile::StringArray;
using sparkcolumnarplugin::precompile::TypeTraits;
using sparkcolumnarplugin::precompile::UnsafeArray;

class HashRelationColumn {
 public:
  virtual bool IsNull(int array_id, int id) = 0;
  virtual arrow::Status AppendColumn(std::shared_ptr<arrow::Array> in) {
    return arrow::Status::NotImplemented("HashRelationColumn AppendColumn is abstract.");
  };
  virtual arrow::Status GetArrayVector(std::vector<std::shared_ptr<arrow::Array>>* out) {
    return arrow::Status::NotImplemented(
        "HashRelationColumn GetArrayVector is abstract.");
  }
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
  arrow::Status GetArrayVector(std::vector<std::shared_ptr<arrow::Array>>* out) override {
    for (auto arr : array_vector_) {
      (*out).push_back(arr->cache_);
    }
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
  arrow::Status GetArrayVector(std::vector<std::shared_ptr<arrow::Array>>* out) override {
    for (auto arr : array_vector_) {
      (*out).push_back(arr->cache_);
    }
    return arrow::Status::OK();
  }
  std::string GetValue(int array_id, int id) {
    return array_vector_[array_id]->GetString(id);
  }

 private:
  std::vector<std::shared_ptr<StringArray>> array_vector_;
};

template <typename T>
using is_number_alike =
    std::integral_constant<bool, std::is_arithmetic<T>::value ||
                                     std::is_floating_point<T>::value>;

/////////////////////////////////////////////////////////////////////////

class HashRelation {
 public:
  HashRelation(arrow::compute::FunctionContext* ctx) {}

  HashRelation(
      const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_list) {
    hash_relation_column_list_ = hash_relation_list;
  }

  HashRelation(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_column,
      int key_size = -1)
      : HashRelation(hash_relation_column) {
    hash_table_ = createUnsafeHashMap(1024 * 1024, 256 * 1024 * 1024, key_size);
  }

  ~HashRelation() {
    if (hash_table_ != nullptr && !unsafe_set) {
      destroyHashMap(hash_table_);
      hash_table_ = nullptr;
    }
  }

  virtual arrow::Status AppendKeyColumn(std::shared_ptr<arrow::Array> in) {
    return arrow::Status::NotImplemented("HashRelation AppendKeyColumn is abstract.");
  }

  arrow::Status AppendKeyColumn(
      std::shared_ptr<arrow::Array> in,
      const std::vector<std::shared_ptr<UnsafeArray>>& payloads) {
    // This Key should be Hash Key
    auto typed_array = std::make_shared<ArrayType>(in);
    for (int i = 0; i < typed_array->length(); i++) {
      std::shared_ptr<UnsafeRow> payload = std::make_shared<UnsafeRow>(payloads.size());
      for (auto payload_arr : payloads) {
        payload_arr->Append(i, &payload);
      }
      RETURN_NOT_OK(Insert(typed_array->GetView(i), payload, num_arrays_, i));
    }

    num_arrays_++;
    // DumpHashMap();
    return arrow::Status::OK();
  }

  template <typename KeyArrayType>
  arrow::Status AppendKeyColumn(std::shared_ptr<arrow::Array> in,
                                std::shared_ptr<KeyArrayType> original_key) {
    // This Key should be Hash Key
    auto typed_array = std::make_shared<ArrayType>(in);
    for (int i = 0; i < typed_array->length(); i++) {
      RETURN_NOT_OK(
          Insert(typed_array->GetView(i), original_key->GetView(i), num_arrays_, i));
    }

    num_arrays_++;
    // DumpHashMap();
    return arrow::Status::OK();
  }

  arrow::Status AppendKeyColumn(std::shared_ptr<arrow::Array> in,
                                std::shared_ptr<StringArray> original_key) {
    // This Key should be Hash Key
    auto typed_array = std::make_shared<ArrayType>(in);
    for (int i = 0; i < typed_array->length(); i++) {
      RETURN_NOT_OK(
          Insert(typed_array->GetView(i), original_key->GetString(i), num_arrays_, i));
    }

    num_arrays_++;
    // DumpHashMap();
    return arrow::Status::OK();
  }

  template <typename CType,
            typename std::enable_if_t<is_number_alike<CType>::value>* = nullptr>
  int Get(int32_t v, CType payload) {
    if (hash_table_ == nullptr) {
      throw std::runtime_error("HashRelation Get failed, hash_table is null.");
    }
    std::vector<char*> res_out;
    auto res = safeLookup(hash_table_, payload, v, &res_out);
    if (res == -1) return -1;
    arrayid_list_.clear();
    for (auto index : res_out) {
      arrayid_list_.push_back(*((ArrayItemIndex*)index));
    }
    return 0;
  }

  int Get(int32_t v, std::string payload) {
    if (hash_table_ == nullptr) {
      throw std::runtime_error("HashRelation Get failed, hash_table is null.");
    }
    std::vector<char*> res_out;
    auto res = safeLookup(hash_table_, payload.data(), payload.size(), v, &res_out);
    if (res == -1) return -1;
    arrayid_list_.clear();
    for (auto index : res_out) {
      arrayid_list_.push_back(*((ArrayItemIndex*)index));
    }
    return 0;
  }

  int Get(int32_t v, std::shared_ptr<UnsafeRow> payload) {
    if (hash_table_ == nullptr) {
      throw std::runtime_error("HashRelation Get failed, hash_table is null.");
    }
    std::vector<char*> res_out;
    auto res = safeLookup(hash_table_, payload, v, &res_out);
    if (res == -1) return -1;
    arrayid_list_.clear();
    for (auto index : res_out) {
      arrayid_list_.push_back(*((ArrayItemIndex*)index));
    }
    return 0;
  }

  template <typename CType,
            typename std::enable_if_t<is_number_alike<CType>::value>* = nullptr>
  int IfExists(int32_t v, CType payload) {
    if (hash_table_ == nullptr) {
      throw std::runtime_error("HashRelation Get failed, hash_table is null.");
    }
    return safeLookup(hash_table_, payload, v);
  }

  int IfExists(int32_t v, std::string payload) {
    if (hash_table_ == nullptr) {
      throw std::runtime_error("HashRelation Get failed, hash_table is null.");
    }
    return safeLookup(hash_table_, payload.data(), payload.size(), v);
  }

  int IfExists(int32_t v, std::shared_ptr<UnsafeRow> payload) {
    if (hash_table_ == nullptr) {
      throw std::runtime_error("HashRelation Get failed, hash_table is null.");
    }
    return safeLookup(hash_table_, payload, v);
  }

  int GetNull() { return null_index_set_ ? 0 : HASH_NEW_KEY; }

  arrow::Status AppendPayloadColumn(int idx, std::shared_ptr<arrow::Array> in) {
    return hash_relation_column_list_[idx]->AppendColumn(in);
  }

  arrow::Status GetArrayVector(int idx, std::vector<std::shared_ptr<arrow::Array>>* out) {
    return hash_relation_column_list_[idx]->GetArrayVector(out);
  }

  template <typename T>
  arrow::Status GetColumn(int idx, std::shared_ptr<T>* out) {
    *out = std::dynamic_pointer_cast<T>(hash_relation_column_list_[idx]);
    return arrow::Status::OK();
  }

  arrow::Status UnsafeGetHashTableObject(int64_t* addrs, int* sizes) {
    if (hash_table_ == nullptr) {
      return arrow::Status::Invalid("UnsafeGetHashTableObject hash_table is null");
    }
    // dump(hash_table_);
    addrs[0] = (int64_t)hash_table_;
    sizes[0] = (int)sizeof(unsafeHashMap);

    addrs[1] = (int64_t)(hash_table_->keyArray);
    sizes[1] = (int)(hash_table_->arrayCapacity * hash_table_->bytesInKeyArray);

    addrs[2] = (int64_t)(hash_table_->bytesMap);
    sizes[2] = (int)(hash_table_->cursor);
    return arrow::Status::OK();
  }

  arrow::Status UnsafeSetHashTableObject(int len, int64_t* addrs, int* sizes) {
    assert(len == 3);
    hash_table_ = (unsafeHashMap*)addrs[0];
    hash_table_->cursor = sizes[2];
    hash_table_->keyArray = (char*)addrs[1];
    hash_table_->bytesMap = (char*)addrs[2];
    unsafe_set = true;
    // dump(hash_table_);
    return arrow::Status::OK();
  }

  arrow::Status DumpHashMap() {
    dump(hash_table_);
    return arrow::Status::OK();
  }

  virtual std::vector<ArrayItemIndex> GetItemListByIndex(int i) { return arrayid_list_; }

  void TESTGrowAndRehashKeyArray() { growAndRehashKeyArray(hash_table_); }

 protected:
  bool unsafe_set = false;
  uint64_t num_arrays_ = 0;
  std::vector<std::shared_ptr<HashRelationColumn>> hash_relation_column_list_;
  unsafeHashMap* hash_table_ = nullptr;
  using ArrayType = sparkcolumnarplugin::precompile::Int32Array;
  bool null_index_set_ = false;
  std::vector<ArrayItemIndex> null_index_list_;
  std::vector<ArrayItemIndex> arrayid_list_;

  arrow::Status Insert(int32_t v, std::shared_ptr<UnsafeRow> payload, uint32_t array_id,
                       uint32_t id) {
    assert(hash_table_ != nullptr);
    auto index = ArrayItemIndex(array_id, id);
    if (!append(hash_table_, payload.get(), v, (char*)&index, sizeof(ArrayItemIndex))) {
      return arrow::Status::CapacityError("Insert to HashMap failed.");
    }
    return arrow::Status::OK();
  }

  template <typename CType>
  arrow::Status Insert(int32_t v, CType payload, uint32_t array_id, uint32_t id) {
    assert(hash_table_ != nullptr);
    auto index = ArrayItemIndex(array_id, id);
    if (!append(hash_table_, payload, v, (char*)&index, sizeof(ArrayItemIndex))) {
      return arrow::Status::CapacityError("Insert to HashMap failed.");
    }
    return arrow::Status::OK();
  }

  arrow::Status InsertNull(uint32_t array_id, uint32_t id) {
    if (!null_index_set_) {
      null_index_set_ = true;
      null_index_list_ = {ArrayItemIndex(array_id, id)};
    } else {
      null_index_list_.emplace_back(array_id, id);
    }
    return arrow::Status::OK();
  }
};

template <typename T, typename Enable = void>
class TypedHashRelation {};

arrow::Status MakeHashRelationColumn(uint32_t data_type_id,
                                     std::shared_ptr<HashRelationColumn>* out);

arrow::Status MakeHashRelation(
    uint32_t key_type_id, arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_column,
    std::shared_ptr<HashRelation>* out);