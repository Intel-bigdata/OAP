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

#include "codegen/arrow_compute/ext/item_iterator.h"
#include <arrow/compare.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <memory>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using namespace arrow;
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

template <typename T, typename C>
class ItemIteratorTypedImpl;

//////////////// ItemIterator ///////////////
class ItemIterator::Impl {
 public:
#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(UInt8Type)                     \
  PROCESS(Int8Type)                      \
  PROCESS(UInt16Type)                    \
  PROCESS(Int16Type)                     \
  PROCESS(UInt32Type)                    \
  PROCESS(Int32Type)                     \
  PROCESS(UInt64Type)                    \
  PROCESS(Int64Type)                     \
  PROCESS(FloatType)                     \
  PROCESS(DoubleType)                    \
  PROCESS(Date32Type)

  static arrow::Status MakeItemIteratorImpl(std::shared_ptr<arrow::DataType> type,
                                            bool is_array_list,
                                            std::shared_ptr<Impl>* out) {
    switch (type->id()) {
#define PROCESS(InType)                                                               \
  case InType::type_id: {                                                             \
    using CType = typename arrow::TypeTraits<InType>::CType;                          \
    auto res = std::make_shared<ItemIteratorTypedImpl<InType, CType>>(is_array_list); \
    *out = std::dynamic_pointer_cast<Impl>(res);                                      \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      case arrow::StringType::type_id: {
        auto res = std::make_shared<ItemIteratorTypedImpl<StringType, std::string>>(
            is_array_list);
        *out = std::dynamic_pointer_cast<Impl>(res);
      } break;
      default: {
        std::cout << "Not Found " << type->ToString() << ", type id is " << type->id()
                  << std::endl;
      } break;
    }
    return arrow::Status::OK();
  }

#undef PROCESS_SUPPORTED_TYPES
  Impl() {}
  virtual ~Impl() {}
  virtual arrow::Status Submit(ArrayList in_arr_list,
                               std::shared_ptr<arrow::Array> selection,
                               std::function<arrow::Status()>* next,
                               std::function<bool()>* is_null,
                               std::function<void*()>* get) {
    throw 1;
  }
  virtual arrow::Status Submit(std::shared_ptr<arrow::Array> in_arr,
                               std::shared_ptr<arrow::Array> selection,
                               std::function<arrow::Status()>* next,
                               std::function<bool()>* is_null,
                               std::function<void*()>* get) {
    throw 1;
  }
  virtual arrow::Status Submit(ArrayList in_arr_list,
                               std::function<bool(ArrayItemIndex)>* is_null,
                               std::function<void*(ArrayItemIndex)>* get) {
    throw 1;
  }
  virtual arrow::Status Submit(std::shared_ptr<arrow::Array> in_arr,
                               std::function<bool(int)>* is_null,
                               std::function<void*(int)>* get) {
    throw 1;
  }
};

template <typename DataType, typename CType>
class ItemIteratorTypedImpl : public ItemIterator::Impl {
 public:
  ItemIteratorTypedImpl(bool is_array_list) {
#ifdef DEBUG
    std::cout << "ItemIteratorTypedImpl constructed" << std::endl;
#endif
    is_array_list_ = is_array_list;
    if (is_array_list_) {
      next_ = [this]() {
        row_id_++;
        is_null_cache_ = true;
        if (!selection_->IsNull(row_id_)) {
          auto item = structed_selection_[row_id_];
          is_null_cache_ = typed_in_arr_list_[item.array_id]->IsNull(item.id);
          if (!is_null_cache_) {
            CType res(typed_in_arr_list_[item.array_id]->GetView(item.id));
            res_cache_ = res;
          }
        }
        return arrow::Status::OK();
      };

    } else {
      next_ = [this]() {
        row_id_++;
        is_null_cache_ = true;
        if (!selection_->IsNull(row_id_)) {
          auto item = uint32_selection_[row_id_];
          is_null_cache_ = typed_in_arr_->IsNull(item);
          if (!is_null_cache_) {
            CType res(typed_in_arr_->GetView(item));
            res_cache_ = res;
          }
        }
        return arrow::Status::OK();
      };
    }
    is_null_ = [this]() { return is_null_cache_; };
    get_ = [this]() { return (void*)&res_cache_; };
    is_null_with_item_index_ = [this](ArrayItemIndex item) {
      return typed_in_arr_list_[item.array_id]->IsNull(item.id);
    };
    is_null_with_index_ = [this](int item) { return typed_in_arr_->IsNull(item); };
    get_with_item_index_ = [this](ArrayItemIndex item) {
      CType res(typed_in_arr_list_[item.array_id]->GetView(item.id));
      res_cache_ = res;
      return (void*)&res_cache_;
    };
    get_with_index_ = [this](int item) {
      CType res(typed_in_arr_->GetView(item));
      res_cache_ = res;
      return (void*)&res_cache_;
    };
  }

  ~ItemIteratorTypedImpl() {
#ifdef DEBUG
    std::cout << "ItemIteratorTypedImpl destructed" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_arr_list, std::shared_ptr<arrow::Array> selection,
                       std::function<arrow::Status()>* next,
                       std::function<bool()>* is_null, std::function<void*()>* get) {
    if (typed_in_arr_list_.size() == 0) {
      for (auto arr : in_arr_list) {
        typed_in_arr_list_.push_back(std::dynamic_pointer_cast<ArrayType>(arr));
      }
    }
    row_id_ = -1;
    selection_ = selection;
    structed_selection_ =
        (ArrayItemIndex*)std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(selection)
            ->raw_values();
    *next = next_;
    *is_null = is_null_;
    *get = get_;
    return arrow::Status::OK();
  }

  arrow::Status Submit(std::shared_ptr<arrow::Array> in_arr,
                       std::shared_ptr<arrow::Array> selection,
                       std::function<arrow::Status()>* next,
                       std::function<bool()>* is_null, std::function<void*()>* get) {
    typed_in_arr_ = std::dynamic_pointer_cast<ArrayType>(in_arr);
    row_id_ = -1;
    selection_ = selection;
    uint32_selection_ =
        (uint32_t*)std::dynamic_pointer_cast<arrow::UInt32Array>(selection)->raw_values();
    *next = next_;
    *is_null = is_null_;
    *get = get_;
    return arrow::Status::OK();
  }

  arrow::Status Submit(ArrayList in_arr_list,
                       std::function<bool(ArrayItemIndex)>* is_null,
                       std::function<void*(ArrayItemIndex)>* get) {
    if (typed_in_arr_list_.size() == 0) {
      for (auto arr : in_arr_list) {
        typed_in_arr_list_.push_back(std::dynamic_pointer_cast<ArrayType>(arr));
      }
    }
    *is_null = is_null_with_item_index_;
    *get = get_with_item_index_;
    return arrow::Status::OK();
  }

  arrow::Status Submit(std::shared_ptr<arrow::Array> in_arr,
                       std::function<bool(int)>* is_null,
                       std::function<void*(int)>* get) {
    typed_in_arr_ = std::dynamic_pointer_cast<ArrayType>(in_arr);
    *is_null = is_null_with_index_;
    *get = get_with_index_;
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  std::vector<std::shared_ptr<ArrayType>> typed_in_arr_list_;
  std::shared_ptr<ArrayType> typed_in_arr_;
  ArrayItemIndex* structed_selection_;
  std::shared_ptr<arrow::Array> selection_;
  uint32_t* uint32_selection_;
  uint32_t row_id_ = -1;
  std::function<bool()> is_null_;
  std::function<void*()> get_;
  std::function<bool(ArrayItemIndex)> is_null_with_item_index_;
  std::function<void*(ArrayItemIndex)> get_with_item_index_;
  std::function<bool(int)> is_null_with_index_;
  std::function<void*(int)> get_with_index_;
  std::function<arrow::Status()> next_;
  bool is_array_list_;
  // cache result when calling Next() for optimization purpose
  uint64_t total_length_;
  CType res_cache_;
  bool is_null_cache_;
};

///////////////////// Public Functions //////////////////
ItemIterator::ItemIterator(std::shared_ptr<arrow::DataType> type, bool is_array_list) {
  auto status = Impl::MakeItemIteratorImpl(type, is_array_list, &impl_);
}

arrow::Status ItemIterator::Submit(ArrayList in_arr_list,
                                   std::shared_ptr<arrow::Array> selection,
                                   std::function<arrow::Status()>* next,
                                   std::function<bool()>* is_null,
                                   std::function<void*()>* get) {
  return impl_->Submit(in_arr_list, selection, next, is_null, get);
}
arrow::Status ItemIterator::Submit(std::shared_ptr<arrow::Array> in_arr,
                                   std::shared_ptr<arrow::Array> selection,
                                   std::function<arrow::Status()>* next,
                                   std::function<bool()>* is_null,
                                   std::function<void*()>* get) {
  return impl_->Submit(in_arr, selection, next, is_null, get);
}
arrow::Status ItemIterator::Submit(ArrayList in_arr_list,
                                   std::function<bool(ArrayItemIndex)>* is_null,
                                   std::function<void*(ArrayItemIndex)>* get) {
  return impl_->Submit(in_arr_list, is_null, get);
}
arrow::Status ItemIterator::Submit(std::shared_ptr<arrow::Array> in_arr,
                                   std::function<bool(int)>* is_null,
                                   std::function<void*(int)>* get) {
  return impl_->Submit(in_arr, is_null, get);
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
