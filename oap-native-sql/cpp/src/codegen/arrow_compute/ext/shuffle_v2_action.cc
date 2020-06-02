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

#include "codegen/arrow_compute/ext/shuffle_v2_action.h"

#include <memory>

#include "codegen/arrow_compute/ext/array_item_index.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using namespace arrow;

template <typename DataType, typename CType>
class ShuffleV2ActionTypedImpl;

class ShuffleV2Action::Impl {
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
  static arrow::Status MakeShuffleV2ActionImpl(arrow::compute::FunctionContext* ctx,
                                               std::shared_ptr<arrow::DataType> type,
                                               bool is_arr_list,
                                               std::shared_ptr<Impl>* out) {
    switch (type->id()) {
#define PROCESS(InType)                                                              \
  case InType::type_id: {                                                            \
    using CType = typename TypeTraits<InType>::CType;                                \
    auto res =                                                                       \
        std::make_shared<ShuffleV2ActionTypedImpl<InType, CType>>(ctx, is_arr_list); \
    *out = std::dynamic_pointer_cast<Impl>(res);                                     \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      case arrow::StringType::type_id: {
        auto res = std::make_shared<ShuffleV2ActionTypedImpl<StringType, std::string>>(
            ctx, is_arr_list);
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
  virtual arrow::Status Submit(std::shared_ptr<arrow::Array> in_arr,
                               std::shared_ptr<arrow::Array> selection,
                               std::function<arrow::Status()>* func) = 0;
  virtual arrow::Status Submit(ArrayList in_arr_list,
                               std::shared_ptr<arrow::Array> selection,
                               std::function<arrow::Status()>* func) = 0;
  virtual arrow::Status FinishAndReset(ArrayList* out) = 0;
};

template <typename DataType, typename CType>
class ShuffleV2ActionTypedImpl : public ShuffleV2Action::Impl {
 public:
  ShuffleV2ActionTypedImpl(arrow::compute::FunctionContext* ctx, bool is_arr_list)
      : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct ShuffleV2ActionTypedImpl" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(builder.release()));
    if (is_arr_list) {
      exec_ = [this]() {
        auto item = structed_selection_[row_id_];
        if (!typed_in_arr_list_[item.array_id]->IsNull(item.id)) {
          RETURN_NOT_OK(
              builder_->Append(typed_in_arr_list_[item.array_id]->GetView(item.id)));
        } else {
          RETURN_NOT_OK(builder_->AppendNull());
        }
        row_id_++;
        return arrow::Status::OK();
      };

      nullable_exec_ = [this]() {
        if (!selection_->IsNull(row_id_)) {
          auto item = structed_selection_[row_id_];
          if (!typed_in_arr_list_[item.array_id]->IsNull(item.id)) {
            RETURN_NOT_OK(
                builder_->Append(typed_in_arr_list_[item.array_id]->GetView(item.id)));
          } else {
            RETURN_NOT_OK(builder_->AppendNull());
          }
        } else {
          RETURN_NOT_OK(builder_->AppendNull());
        }
        row_id_++;
        return arrow::Status::OK();
      };

    } else {
      exec_ = [this]() {
        auto item = uint32_selection_[row_id_];
        if (!typed_in_arr_->IsNull(item)) {
          RETURN_NOT_OK(builder_->Append(typed_in_arr_->GetView(item)));
        } else {
          RETURN_NOT_OK(builder_->AppendNull());
        }
        row_id_++;
        return arrow::Status::OK();
      };
      nullable_exec_ = [this]() {
        if (!selection_->IsNull(row_id_)) {
          auto item = uint32_selection_[row_id_];
          if (!typed_in_arr_->IsNull(item)) {
            RETURN_NOT_OK(builder_->Append(typed_in_arr_->GetView(item)));
          } else {
            RETURN_NOT_OK(builder_->AppendNull());
          }
        } else {
          RETURN_NOT_OK(builder_->AppendNull());
        }
        row_id_++;
        return arrow::Status::OK();
      };
    }
  }
  ~ShuffleV2ActionTypedImpl() {
#ifdef DEBUG
    std::cout << "Destruct ShuffleV2ActionTypedImpl" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_arr_list, std::shared_ptr<arrow::Array> selection,
                       std::function<arrow::Status()>* exec) {
    row_id_ = 0;
    if (typed_in_arr_list_.size() == 0) {
      for (auto arr : in_arr_list) {
        typed_in_arr_list_.push_back(std::dynamic_pointer_cast<ArrayType>(arr));
      }
    }
    selection_ = selection;
    structed_selection_ =
        (ArrayItemIndex*)std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(selection)
            ->raw_values();
    if (selection->null_count() == 0) {
      *exec = exec_;
    } else {
      *exec = nullable_exec_;
    }
    return arrow::Status::OK();
  }

  arrow::Status Submit(std::shared_ptr<arrow::Array> in_arr,
                       std::shared_ptr<arrow::Array> selection,
                       std::function<arrow::Status()>* exec) {
    row_id_ = 0;
    typed_in_arr_ = std::dynamic_pointer_cast<ArrayType>(in_arr);
    selection_ = selection;
    uint32_selection_ =
        (uint32_t*)std::dynamic_pointer_cast<arrow::UInt32Array>(selection)->raw_values();
    if (selection->null_count() == 0) {
      *exec = exec_;
    } else {
      *exec = nullable_exec_;
    }
    return arrow::Status::OK();
  }

  arrow::Status FinishAndReset(ArrayList* out) {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    builder_->Reset();
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  int arg_id_;
  uint64_t row_id_ = 0;
  std::vector<std::shared_ptr<ArrayType>> typed_in_arr_list_;
  std::shared_ptr<ArrayType> typed_in_arr_;
  std::shared_ptr<arrow::Array> selection_;
  ArrayItemIndex* structed_selection_;
  uint32_t* uint32_selection_;
  // result
  std::function<arrow::Status()> exec_;
  std::function<arrow::Status()> nullable_exec_;
  std::shared_ptr<BuilderType> builder_;
};

ShuffleV2Action::ShuffleV2Action(arrow::compute::FunctionContext* ctx,
                                 std::shared_ptr<arrow::DataType> type,
                                 bool is_arr_list) {
  auto status = Impl::MakeShuffleV2ActionImpl(ctx, type, is_arr_list, &impl_);
}

ShuffleV2Action::~ShuffleV2Action() {}

arrow::Status ShuffleV2Action::Submit(ArrayList in_arr_list,
                                      std::shared_ptr<arrow::Array> selection,
                                      std::function<arrow::Status()>* func) {
  RETURN_NOT_OK(impl_->Submit(in_arr_list, selection, func));
  return arrow::Status::OK();
}

arrow::Status ShuffleV2Action::Submit(std::shared_ptr<arrow::Array> in_arr,
                                      std::shared_ptr<arrow::Array> selection,
                                      std::function<arrow::Status()>* func) {
  RETURN_NOT_OK(impl_->Submit(in_arr, selection, func));
  return arrow::Status::OK();
}

arrow::Status ShuffleV2Action::FinishAndReset(ArrayList* out) {
  RETURN_NOT_OK(impl_->FinishAndReset(out));
  return arrow::Status::OK();
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
