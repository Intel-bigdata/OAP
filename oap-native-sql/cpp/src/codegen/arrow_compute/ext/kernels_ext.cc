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

#include "codegen/arrow_compute/ext/kernels_ext.h"

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/concatenate.h>
#include <arrow/compute/context.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/count.h>
#include <arrow/compute/kernels/hash.h>
#include <arrow/compute/kernels/mean.h>
#include <arrow/compute/kernels/minmax.h>
#include <arrow/compute/kernels/sum.h>
#include <arrow/pretty_print.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <arrow/visitor_inline.h>
#include <dlfcn.h>
#include <gandiva/configuration.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <unordered_map>

#include "codegen/arrow_compute/ext/actions_impl.h"
#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
//#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include "third_party/arrow/utils/hashing.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  SplitArrayListWithAction  ////////////////
class SplitArrayListWithActionKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::vector<std::string> action_name_list,
       std::vector<std::shared_ptr<arrow::DataType>> type_list)
      : ctx_(ctx), action_name_list_(action_name_list) {
    InitActionList(type_list);
  }
  virtual ~Impl() {}

  arrow::Status InitActionList(std::vector<std::shared_ptr<arrow::DataType>> type_list) {
    int type_id = 0;
#ifdef DEBUG
    std::cout << "action_name_list_ has " << action_name_list_.size()
              << " elements, and type_list has " << type_list.size() << " elements."
              << std::endl;
#endif
    for (int action_id = 0; action_id < action_name_list_.size(); action_id++) {
      std::shared_ptr<ActionBase> action;
      if (action_name_list_[action_id].compare("action_unique") == 0) {
        RETURN_NOT_OK(MakeUniqueAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_count") == 0) {
        RETURN_NOT_OK(MakeCountAction(ctx_, &action));
      } else if (action_name_list_[action_id].compare("action_sum") == 0) {
        RETURN_NOT_OK(MakeSumAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_avg") == 0) {
        RETURN_NOT_OK(MakeAvgAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_min") == 0) {
        RETURN_NOT_OK(MakeMinAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_max") == 0) {
        RETURN_NOT_OK(MakeMaxAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_sum_count") == 0) {
        RETURN_NOT_OK(MakeSumCountAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_sum_count_merge") == 0) {
        RETURN_NOT_OK(MakeSumCountMergeAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_avgByCount") == 0) {
        RETURN_NOT_OK(MakeAvgByCountAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare(0, 20, "action_countLiteral_") ==
                 0) {
        int arg = std::stoi(action_name_list_[action_id].substr(20));
        RETURN_NOT_OK(MakeCountLiteralAction(ctx_, arg, &action));
      } else if (action_name_list_[action_id].compare("action_stddev_samp_partial") ==
                 0) {
        RETURN_NOT_OK(MakeStddevSampPartialAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_stddev_samp_final") == 0) {
        RETURN_NOT_OK(MakeStddevSampFinalAction(ctx_, type_list[type_id], &action));
      } else {
        return arrow::Status::NotImplemented(action_name_list_[action_id],
                                             " is not implementetd.");
      }
      type_id += action->RequiredColNum();
      action_list_.push_back(action);
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const ArrayList& in,
                         const std::shared_ptr<arrow::Array>& in_dict) {
    if (!in_dict) {
      return arrow::Status::Invalid("input data is invalid");
    }

    // TODO: We used to use arrow::Minmax, while I noticed when there is null or -1 data
    // inside array, max will output incorrect result, change to handmade function for now
    int32_t max_group_id = 0;
    auto typed_in_dict = std::dynamic_pointer_cast<arrow::Int32Array>(in_dict);
    for (int i = 0; i < typed_in_dict->length(); i++) {
      if (typed_in_dict->IsValid(i)) {
        if (typed_in_dict->GetView(i) > max_group_id) {
          max_group_id = typed_in_dict->GetView(i);
        }
      }
    }

    std::vector<std::function<arrow::Status(int)>> eval_func_list;
    std::vector<std::function<arrow::Status()>> eval_null_func_list;
    int col_id = 0;
    ArrayList cols;
    for (int i = 0; i < action_list_.size(); i++) {
      cols.clear();
      auto action = action_list_[i];
      for (int j = 0; j < action->RequiredColNum(); j++) {
        cols.push_back(in[col_id++]);
      }
      std::function<arrow::Status(int)> func;
      std::function<arrow::Status()> null_func;
      action->Submit(cols, max_group_id, &func, &null_func);
      eval_func_list.push_back(func);
      eval_null_func_list.push_back(null_func);
    }

    for (int row_id = 0; row_id < in_dict->length(); row_id++) {
      if (in_dict->IsValid(row_id)) {
        auto group_id = typed_in_dict->GetView(row_id);
        for (auto eval_func : eval_func_list) {
          eval_func(group_id);
        }
      } else {
        for (auto eval_func : eval_null_func_list) {
          eval_func();
        }
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    for (auto action : action_list_) {
      RETURN_NOT_OK(action->Finish(out));
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    uint64_t total_length = action_list_[0]->GetResultLength();
    auto eval_func = [this, schema](uint64_t offset, uint64_t length,
                                    std::shared_ptr<arrow::RecordBatch>* out) {
      ArrayList arr_list;
      for (auto action : action_list_) {
        RETURN_NOT_OK(action->Finish(offset, length, &arr_list));
      }
      *out = arrow::RecordBatch::Make(schema, length, arr_list);
      return arrow::Status::OK();
    };
    *out = std::make_shared<SplitArrayWithActionResultIterator>(ctx_, total_length,
                                                                eval_func);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::string> action_name_list_;
  std::vector<std::shared_ptr<extra::ActionBase>> action_list_;

  class SplitArrayWithActionResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SplitArrayWithActionResultIterator(
        arrow::compute::FunctionContext* ctx, uint64_t total_length,
        std::function<arrow::Status(uint64_t offset, uint64_t length,
                                    std::shared_ptr<arrow::RecordBatch>* out)>
            eval_func)
        : ctx_(ctx), total_length_(total_length), eval_func_(eval_func) {}
    ~SplitArrayWithActionResultIterator() {}

    std::string ToString() override { return "SplitArrayWithActionResultIterator"; }

    bool HasNext() override {
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      if (offset_ >= total_length_) {
        *out = nullptr;
        return arrow::Status::OK();
      }
      auto length = (total_length_ - offset_) > GetBatchSize()
                        ? GetBatchSize()
                        : (total_length_ - offset_);
      TIME_MICRO_OR_RAISE(elapse_time_, eval_func_(offset_, length, out));
      offset_ += length;
      // arrow::PrettyPrint(*(*out).get(), 2, &std::cout);
      return arrow::Status::OK();
    }

   private:
    arrow::compute::FunctionContext* ctx_;
    std::function<arrow::Status(uint64_t offset, uint64_t length,
                                std::shared_ptr<arrow::RecordBatch>* out)>
        eval_func_;
    uint64_t offset_ = 0;
    const uint64_t total_length_;
    uint64_t elapse_time_ = 0;
  };
};

arrow::Status SplitArrayListWithActionKernel::Make(
    arrow::compute::FunctionContext* ctx, std::vector<std::string> action_name_list,
    std::vector<std::shared_ptr<arrow::DataType>> type_list,
    std::shared_ptr<KernalBase>* out) {
  *out =
      std::make_shared<SplitArrayListWithActionKernel>(ctx, action_name_list, type_list);
  return arrow::Status::OK();
}

SplitArrayListWithActionKernel::SplitArrayListWithActionKernel(
    arrow::compute::FunctionContext* ctx, std::vector<std::string> action_name_list,
    std::vector<std::shared_ptr<arrow::DataType>> type_list) {
  impl_.reset(new Impl(ctx, action_name_list, type_list));
  kernel_name_ = "SplitArrayListWithActionKernel";
}

arrow::Status SplitArrayListWithActionKernel::Evaluate(
    const ArrayList& in, const std::shared_ptr<arrow::Array>& dict) {
  return impl_->Evaluate(in, dict);
}

arrow::Status SplitArrayListWithActionKernel::Finish(ArrayList* out) {
  return impl_->Finish(out);
}

arrow::Status SplitArrayListWithActionKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

///////////////  UniqueArray  ////////////////
/*class UniqueArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    std::shared_ptr<arrow::Array> out;
    if (in->length() == 0) {
      return arrow::Status::OK();
    }
    arrow::compute::Datum input_datum(in);
    RETURN_NOT_OK(arrow::compute::Unique(ctx_, input_datum, &out));
    if (!builder) {
      RETURN_NOT_OK(MakeArrayBuilder(out->type(), ctx_->memory_pool(), &builder));
    }

    RETURN_NOT_OK(builder->AppendArrayItem(&(*out.get()), 0, 0));

    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    RETURN_NOT_OK(builder->Finish(out));
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayBuilderImplBase> builder;
};

arrow::Status UniqueArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                      std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<UniqueArrayKernel>(ctx);
  return arrow::Status::OK();
}

UniqueArrayKernel::UniqueArrayKernel(arrow::compute::FunctionContext* ctx) {
  impl_.reset(new Impl(ctx));
  kernel_name_ = "UniqueArrayKernel";
}

arrow::Status UniqueArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status UniqueArrayKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}*/

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

///////////////  SumArray  ////////////////
class SumArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  virtual ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum output;
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[0].get(), &output));
    res_data_type_ = output.scalar()->type;
    scalar_list_.push_back(output.scalar());
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    switch (res_data_type_->id()) {
#define PROCESS(DataType)                         \
  case DataType::type_id: {                       \
    RETURN_NOT_OK(FinishInternal<DataType>(out)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    }
    return arrow::Status::OK();
  }

  template <typename DataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
    using CType = typename arrow::TypeTraits<DataType>::CType;
    CType res = 0;
    for (auto scalar_item : scalar_list_) {
      auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_item);
      res += typed_scalar->value;
    }
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    scalar_out = arrow::MakeScalar(res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::shared_ptr<arrow::DataType> res_data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> scalar_list_;
};

arrow::Status SumArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<arrow::DataType> data_type,
                                   std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<SumArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

SumArrayKernel::SumArrayKernel(arrow::compute::FunctionContext* ctx,
                               std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "SumArrayKernel";
}

arrow::Status SumArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status SumArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  CountArray  ////////////////
class CountArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  virtual ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum output;
    arrow::compute::CountOptions option(arrow::compute::CountOptions::COUNT_ALL);
    RETURN_NOT_OK(arrow::compute::Count(ctx_, option, *in[0].get(), &output));
    scalar_list_.push_back(output.scalar());
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    RETURN_NOT_OK(FinishInternal<arrow::Int64Type>(out));
    return arrow::Status::OK();
  }

  template <typename DataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using CType = typename arrow::TypeTraits<DataType>::CType;
    using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
    CType res = 0;
    for (auto scalar_item : scalar_list_) {
      auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_item);
      res += typed_scalar->value;
    }
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    scalar_out = arrow::MakeScalar(res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> scalar_list_;
};

arrow::Status CountArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                     std::shared_ptr<arrow::DataType> data_type,
                                     std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<CountArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

CountArrayKernel::CountArrayKernel(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "CountArrayKernel";
}

arrow::Status CountArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status CountArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  SumCountArray  ////////////////
class SumCountArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  virtual ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum sum_out;
    arrow::compute::Datum cnt_out;
    arrow::compute::CountOptions option(arrow::compute::CountOptions::COUNT_ALL);
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[0].get(), &sum_out));
    RETURN_NOT_OK(arrow::compute::Count(ctx_, option, *in[0].get(), &cnt_out));
    res_data_type_ = sum_out.scalar()->type;
    sum_scalar_list_.push_back(sum_out.scalar());
    cnt_scalar_list_.push_back(cnt_out.scalar());
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    switch (res_data_type_->id()) {
#define PROCESS(DataType)                         \
  case DataType::type_id: {                       \
    RETURN_NOT_OK(FinishInternal<DataType>(out)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    }
    return arrow::Status::OK();
  }

  template <typename DataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using CType = typename arrow::TypeTraits<DataType>::CType;
    using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
    using CntScalarType = typename arrow::TypeTraits<arrow::Int64Type>::ScalarType;
    double sum_res = 0;
    int64_t cnt_res = 0;
    for (size_t i = 0; i < sum_scalar_list_.size(); i++) {
      auto sum_typed_scalar = std::dynamic_pointer_cast<ScalarType>(sum_scalar_list_[i]);
      auto cnt_typed_scalar =
          std::dynamic_pointer_cast<CntScalarType>(cnt_scalar_list_[i]);
      sum_res += sum_typed_scalar->value;
      cnt_res += cnt_typed_scalar->value;
    }
    std::shared_ptr<arrow::Array> sum_out;
    std::shared_ptr<arrow::Scalar> sum_scalar_out;
    sum_scalar_out = arrow::MakeScalar(sum_res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*sum_scalar_out.get(), 1, &sum_out));

    std::shared_ptr<arrow::Array> cnt_out;
    std::shared_ptr<arrow::Scalar> cnt_scalar_out;
    cnt_scalar_out = arrow::MakeScalar(cnt_res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*cnt_scalar_out.get(), 1, &cnt_out));

    out->push_back(sum_out);
    out->push_back(cnt_out);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> res_data_type_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> sum_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> cnt_scalar_list_;
};

arrow::Status SumCountArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                        std::shared_ptr<arrow::DataType> data_type,
                                        std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<SumCountArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

SumCountArrayKernel::SumCountArrayKernel(arrow::compute::FunctionContext* ctx,
                                         std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "SumCountArrayKernel";
}

arrow::Status SumCountArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status SumCountArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  AvgByCountArray  ////////////////
class AvgByCountArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  virtual ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum sum_out;
    arrow::compute::Datum cnt_out;
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[0].get(), &sum_out));
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[1].get(), &cnt_out));
    sum_scalar_list_.push_back(sum_out.scalar());
    cnt_scalar_list_.push_back(cnt_out.scalar());
    sum_res_data_type_ = sum_out.scalar()->type;
    cnt_res_data_type_ = cnt_out.scalar()->type;
    return arrow::Status::OK();
  }

#define PROCESS_INTERNAL(SumDataType, CntDataType) \
  case CntDataType::type_id: {                     \
    FinishInternal<SumDataType, CntDataType>(out); \
  } break;

  arrow::Status Finish(ArrayList* out) {
    switch (sum_res_data_type_->id()) {
#define PROCESS(SumDataType)                           \
  case SumDataType::type_id: {                         \
    switch (cnt_res_data_type_->id()) {                \
      PROCESS_INTERNAL(SumDataType, arrow::UInt64Type) \
      PROCESS_INTERNAL(SumDataType, arrow::Int64Type)  \
      PROCESS_INTERNAL(SumDataType, arrow::DoubleType) \
    }                                                  \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
#undef PROCESS_INTERNAL
    }
    return arrow::Status::OK();
  }

  template <typename SumDataType, typename CntDataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using SumCType = typename arrow::TypeTraits<SumDataType>::CType;
    using CntCType = typename arrow::TypeTraits<CntDataType>::CType;
    using SumScalarType = typename arrow::TypeTraits<SumDataType>::ScalarType;
    using CntScalarType = typename arrow::TypeTraits<CntDataType>::ScalarType;
    SumCType sum_res = 0;
    CntCType cnt_res = 0;
    for (size_t i = 0; i < sum_scalar_list_.size(); i++) {
      auto sum_typed_scalar =
          std::dynamic_pointer_cast<SumScalarType>(sum_scalar_list_[i]);
      auto cnt_typed_scalar =
          std::dynamic_pointer_cast<CntScalarType>(cnt_scalar_list_[i]);
      sum_res += sum_typed_scalar->value;
      cnt_res += cnt_typed_scalar->value;
    }
    double res = sum_res * 1.0 / cnt_res;
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    scalar_out = arrow::MakeScalar(res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));

    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::shared_ptr<arrow::DataType> cnt_res_data_type_;
  std::shared_ptr<arrow::DataType> sum_res_data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> sum_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> cnt_scalar_list_;
};  // namespace extra

arrow::Status AvgByCountArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                          std::shared_ptr<arrow::DataType> data_type,
                                          std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<AvgByCountArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

AvgByCountArrayKernel::AvgByCountArrayKernel(arrow::compute::FunctionContext* ctx,
                                             std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "AvgByCountArrayKernel";
}

arrow::Status AvgByCountArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status AvgByCountArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  MinArray  ////////////////
class MinArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  virtual ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum minMaxOut;
    arrow::compute::MinMaxOptions option;
    RETURN_NOT_OK(arrow::compute::MinMax(ctx_, option, *in[0].get(), &minMaxOut));
    if (!minMaxOut.is_collection()) {
      return arrow::Status::Invalid("MinMax return an invalid result.");
    }
    auto col = minMaxOut.collection();
    if (col.size() < 2) {
      return arrow::Status::Invalid("MinMax return an invalid result.");
    }
    auto min = col[0].scalar();
    scalar_list_.push_back(min);
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    switch (data_type_->id()) {
#define PROCESS(DataType)                         \
  case DataType::type_id: {                       \
    RETURN_NOT_OK(FinishInternal<DataType>(out)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    }
    return arrow::Status::OK();
  }

  template <typename DataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using CType = typename arrow::TypeTraits<DataType>::CType;
    using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_list_[0]);
    CType res = typed_scalar->value;
    for (size_t i = 1; i < scalar_list_.size(); i++) {
      auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_list_[i]);
      if (typed_scalar->value < res) res = typed_scalar->value;
    }
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    scalar_out = arrow::MakeScalar(res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> scalar_list_;
  std::unique_ptr<arrow::ArrayBuilder> array_builder_;
};

arrow::Status MinArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<arrow::DataType> data_type,
                                   std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<MinArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

MinArrayKernel::MinArrayKernel(arrow::compute::FunctionContext* ctx,
                               std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "MinArrayKernel";
}

arrow::Status MinArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status MinArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  MaxArray  ////////////////
class MaxArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  virtual ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum minMaxOut;
    arrow::compute::MinMaxOptions option;
    RETURN_NOT_OK(arrow::compute::MinMax(ctx_, option, *in[0].get(), &minMaxOut));
    if (!minMaxOut.is_collection()) {
      return arrow::Status::Invalid("MinMax return an invalid result.");
    }
    auto col = minMaxOut.collection();
    if (col.size() < 2) {
      return arrow::Status::Invalid("MinMax return an invalid result.");
    }
    auto max = col[1].scalar();
    scalar_list_.push_back(max);
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    switch (data_type_->id()) {
#define PROCESS(DataType)                         \
  case DataType::type_id: {                       \
    RETURN_NOT_OK(FinishInternal<DataType>(out)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    }
    return arrow::Status::OK();
  }

  template <typename DataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using CType = typename arrow::TypeTraits<DataType>::CType;
    using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_list_[0]);
    CType res = typed_scalar->value;
    for (size_t i = 1; i < scalar_list_.size(); i++) {
      auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_list_[i]);
      if (typed_scalar->value > res) res = typed_scalar->value;
    }
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    scalar_out = arrow::MakeScalar(res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> scalar_list_;
  std::unique_ptr<arrow::ArrayBuilder> array_builder_;
};

arrow::Status MaxArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<arrow::DataType> data_type,
                                   std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<MaxArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

MaxArrayKernel::MaxArrayKernel(arrow::compute::FunctionContext* ctx,
                               std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "MaxArrayKernel";
}

arrow::Status MaxArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status MaxArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  StddevSampPartialArray  ////////////////
class StddevSampPartialArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  virtual ~Impl() {}

  template <typename ValueType>
  arrow::Status getM2(arrow::compute::FunctionContext* ctx,
                      const arrow::compute::Datum& value,
                      const arrow::compute::Datum& mean, arrow::compute::Datum* out) {
    using MeanCType = typename arrow::TypeTraits<arrow::DoubleType>::CType;
    using MeanScalarType = typename arrow::TypeTraits<arrow::DoubleType>::ScalarType;
    using ValueCType = typename arrow::TypeTraits<ValueType>::CType;
    std::shared_ptr<arrow::Scalar> mean_scalar = mean.scalar();
    auto mean_typed_scalar = std::dynamic_pointer_cast<MeanScalarType>(mean_scalar);
    double mean_res = mean_typed_scalar->value * 1.0;
    double m2_res = 0;

    if (!value.is_array()) {
      return arrow::Status::Invalid("AggregateKernel expects Array");
    }
    auto array = value.make_array();
    auto typed_array = std::static_pointer_cast<arrow::NumericArray<ValueType>>(array);
    const ValueCType* input = typed_array->raw_values();
    for (int64_t i = 0; i < (*array).length(); i++) {
      auto val = input[i];
      if (val) {
        m2_res += (input[i] * 1.0 - mean_res) * (input[i] * 1.0 - mean_res);
      }
    }
    *out = arrow::MakeScalar(m2_res);
    return arrow::Status::OK();
  }

  arrow::Status M2(arrow::compute::FunctionContext* ctx, const arrow::Array& array,
                   const arrow::compute::Datum& mean, arrow::compute::Datum* out) {
    arrow::compute::Datum value = array.data();
    auto data_type = value.type();

    if (data_type == nullptr)
      return arrow::Status::Invalid("Datum must be array-like");
    else if (!is_integer(data_type->id()) && !is_floating(data_type->id()))
      return arrow::Status::Invalid("Datum must contain a NumericType");
    switch (data_type_->id()) {
#define PROCESS(DataType)                                  \
  case DataType::type_id: {                                \
    RETURN_NOT_OK(getM2<DataType>(ctx, value, mean, out)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum sum_out;
    arrow::compute::Datum cnt_out;
    arrow::compute::Datum mean_out;
    arrow::compute::Datum m2_out;
    arrow::compute::CountOptions option(arrow::compute::CountOptions::COUNT_ALL);
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[0].get(), &sum_out));
    RETURN_NOT_OK(arrow::compute::Count(ctx_, option, *in[0].get(), &cnt_out));
    RETURN_NOT_OK(arrow::compute::Mean(ctx_, *in[0].get(), &mean_out));
    RETURN_NOT_OK(M2(ctx_, *in[0].get(), mean_out, &m2_out));
    sum_scalar_list_.push_back(sum_out.scalar());
    cnt_scalar_list_.push_back(cnt_out.scalar());
    mean_scalar_list_.push_back(mean_out.scalar());
    m2_scalar_list_.push_back(m2_out.scalar());
    sum_res_data_type_ = sum_out.scalar()->type;
    cnt_res_data_type_ = cnt_out.scalar()->type;
    mean_res_data_type_ = mean_out.scalar()->type;
    m2_res_data_type_ = m2_out.scalar()->type;
    return arrow::Status::OK();
  }

#define PROCESS_INTERNAL(SumDataType, CntDataType) \
  case CntDataType::type_id: {                     \
    FinishInternal<SumDataType, CntDataType>(out); \
  } break;

  arrow::Status Finish(ArrayList* out) {
    switch (sum_res_data_type_->id()) {
#define PROCESS(SumDataType)                           \
  case SumDataType::type_id: {                         \
    switch (cnt_res_data_type_->id()) {                \
      PROCESS_INTERNAL(SumDataType, arrow::UInt64Type) \
      PROCESS_INTERNAL(SumDataType, arrow::Int64Type)  \
      PROCESS_INTERNAL(SumDataType, arrow::DoubleType) \
    }                                                  \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
#undef PROCESS_INTERNAL
    }
    return arrow::Status::OK();
  }

  template <typename SumDataType, typename CntDataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using SumCType = typename arrow::TypeTraits<SumDataType>::CType;
    using CntCType = typename arrow::TypeTraits<CntDataType>::CType;
    using DoubleCType = typename arrow::TypeTraits<arrow::DoubleType>::CType;
    using SumScalarType = typename arrow::TypeTraits<SumDataType>::ScalarType;
    using CntScalarType = typename arrow::TypeTraits<CntDataType>::ScalarType;
    using DoubleScalarType = typename arrow::TypeTraits<arrow::DoubleType>::ScalarType;
    SumCType sum_res = 0;
    DoubleCType cnt_res = 0;
    DoubleCType mean_res = 0;
    DoubleCType m2_res = 0;
    for (size_t i = 0; i < sum_scalar_list_.size(); i++) {
      auto sum_typed_scalar =
          std::dynamic_pointer_cast<SumScalarType>(sum_scalar_list_[i]);
      auto cnt_typed_scalar =
          std::dynamic_pointer_cast<CntScalarType>(cnt_scalar_list_[i]);
      auto mean_typed_scalar =
          std::dynamic_pointer_cast<DoubleScalarType>(mean_scalar_list_[i]);
      auto m2_typed_scalar =
          std::dynamic_pointer_cast<DoubleScalarType>(m2_scalar_list_[i]);
      if (cnt_typed_scalar->value > 0) {
        double pre_avg = sum_res * 1.0 / (cnt_res > 0 ? cnt_res : 1);
        double delta = mean_typed_scalar->value - pre_avg;
        double newN = (cnt_res + cnt_typed_scalar->value) * 1.0;
        double deltaN = newN > 0 ? delta / newN : 0.0;
        m2_res +=
            m2_typed_scalar->value + delta * deltaN * cnt_res * cnt_typed_scalar->value;
        sum_res += sum_typed_scalar->value;
        cnt_res += cnt_typed_scalar->value * 1.0;
      }
    }
    double avg = 0;
    if (cnt_res > 0) {
      avg = sum_res * 1.0 / cnt_res;
    } else {
      m2_res = 0;
    }
    std::shared_ptr<arrow::Array> cnt_out;
    std::shared_ptr<arrow::Scalar> cnt_scalar_out;
    cnt_scalar_out = arrow::MakeScalar(cnt_res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*cnt_scalar_out.get(), 1, &cnt_out));

    std::shared_ptr<arrow::Array> avg_out;
    std::shared_ptr<arrow::Scalar> avg_scalar_out;
    avg_scalar_out = arrow::MakeScalar(avg);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*avg_scalar_out.get(), 1, &avg_out));

    std::shared_ptr<arrow::Array> m2_out;
    std::shared_ptr<arrow::Scalar> m2_scalar_out;
    m2_scalar_out = arrow::MakeScalar(m2_res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*m2_scalar_out.get(), 1, &m2_out));

    out->push_back(cnt_out);
    out->push_back(avg_out);
    out->push_back(m2_out);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::shared_ptr<arrow::DataType> sum_res_data_type_;
  std::shared_ptr<arrow::DataType> cnt_res_data_type_;
  std::shared_ptr<arrow::DataType> mean_res_data_type_;
  std::shared_ptr<arrow::DataType> m2_res_data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> sum_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> cnt_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> mean_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> m2_scalar_list_;
};

arrow::Status StddevSampPartialArrayKernel::Make(
    arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<StddevSampPartialArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

StddevSampPartialArrayKernel::StddevSampPartialArrayKernel(
    arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "StddevSampPartialArrayKernel";
}

arrow::Status StddevSampPartialArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status StddevSampPartialArrayKernel::Finish(ArrayList* out) {
  return impl_->Finish(out);
}

///////////////  StddevSampFinalArray  ////////////////
class StddevSampFinalArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  virtual ~Impl() {}

  arrow::Status getAvgM2(arrow::compute::FunctionContext* ctx,
                         const arrow::compute::Datum& cnt_value,
                         const arrow::compute::Datum& avg_value,
                         const arrow::compute::Datum& m2_value,
                         arrow::compute::Datum* avg_out, arrow::compute::Datum* m2_out) {
    using MeanCType = typename arrow::TypeTraits<arrow::DoubleType>::CType;
    using MeanScalarType = typename arrow::TypeTraits<arrow::DoubleType>::ScalarType;
    using ValueCType = typename arrow::TypeTraits<arrow::DoubleType>::CType;

    if (!(cnt_value.is_array() && avg_value.is_array() && m2_value.is_array())) {
      return arrow::Status::Invalid("AggregateKernel expects Array datum");
    }

    auto cnt_array = cnt_value.make_array();
    auto avg_array = avg_value.make_array();
    auto m2_array = m2_value.make_array();

    auto cnt_typed_array = std::static_pointer_cast<arrow::DoubleArray>(cnt_array);
    auto avg_typed_array = std::static_pointer_cast<arrow::DoubleArray>(avg_array);
    auto m2_typed_array = std::static_pointer_cast<arrow::DoubleArray>(m2_array);
    const ValueCType* cnt_input = cnt_typed_array->raw_values();
    const MeanCType* avg_input = avg_typed_array->raw_values();
    const MeanCType* m2_input = m2_typed_array->raw_values();

    double cnt_res = 0;
    double avg_res = 0;
    double m2_res = 0;
    for (int64_t i = 0; i < (*cnt_array).length(); i++) {
      double cnt_val = cnt_input[i];
      double avg_val = avg_input[i];
      double m2_val = m2_input[i];
      if (i == 0) {
        cnt_res = cnt_val;
        avg_res = avg_val;
        m2_res = m2_val;
      } else {
        if (cnt_val > 0) {
          double delta = avg_val - avg_res;
          double deltaN = (cnt_res + cnt_val) > 0 ? delta / (cnt_res + cnt_val) : 0;
          avg_res += deltaN * cnt_val;
          m2_res += (m2_val + delta * deltaN * cnt_res * cnt_val);
          cnt_res += cnt_val;
        }
      }
    }
    *avg_out = arrow::MakeScalar(avg_res);
    *m2_out = arrow::MakeScalar(m2_res);
    return arrow::Status::OK();
  }

  arrow::Status updateValue(arrow::compute::FunctionContext* ctx,
                            const arrow::Array& cnt_array, const arrow::Array& avg_array,
                            const arrow::Array& m2_array, arrow::compute::Datum* avg_out,
                            arrow::compute::Datum* m2_out) {
    arrow::compute::Datum cnt_value = cnt_array.data();
    arrow::compute::Datum avg_value = avg_array.data();
    arrow::compute::Datum m2_value = m2_array.data();
    auto cnt_data_type = cnt_value.type();
    if (cnt_data_type == nullptr)
      return arrow::Status::Invalid("Datum must be array-like");
    else if (!is_integer(cnt_data_type->id()) && !is_floating(cnt_data_type->id()))
      return arrow::Status::Invalid("Datum must contain a NumericType");
    RETURN_NOT_OK(getAvgM2(ctx, cnt_value, avg_value, m2_value, avg_out, m2_out));
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum cnt_out;
    arrow::compute::Datum avg_out;
    arrow::compute::Datum m2_out;
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[0].get(), &cnt_out));
    RETURN_NOT_OK(
        updateValue(ctx_, *in[0].get(), *in[1].get(), *in[2].get(), &avg_out, &m2_out));
    cnt_scalar_list_.push_back(cnt_out.scalar());
    avg_scalar_list_.push_back(avg_out.scalar());
    m2_scalar_list_.push_back(m2_out.scalar());
    cnt_res_data_type_ = cnt_out.scalar()->type;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    using DoubleCType = typename arrow::TypeTraits<arrow::DoubleType>::CType;
    using DoubleScalarType = typename arrow::TypeTraits<arrow::DoubleType>::ScalarType;

    DoubleCType cnt_res = 0;
    DoubleCType avg_res = 0;
    DoubleCType m2_res = 0;
    for (size_t i = 0; i < cnt_scalar_list_.size(); i++) {
      auto cnt_typed_scalar =
          std::dynamic_pointer_cast<DoubleScalarType>(cnt_scalar_list_[i]);
      auto avg_typed_scalar =
          std::dynamic_pointer_cast<DoubleScalarType>(avg_scalar_list_[i]);
      auto m2_typed_scalar =
          std::dynamic_pointer_cast<DoubleScalarType>(m2_scalar_list_[i]);
      if (i == 0) {
        cnt_res = cnt_typed_scalar->value;
        avg_res = avg_typed_scalar->value;
        m2_res = m2_typed_scalar->value;
      } else {
        if (cnt_typed_scalar->value > 0) {
          double delta = avg_typed_scalar->value - avg_res;
          double newN = cnt_res + cnt_typed_scalar->value;
          double deltaN = newN > 0 ? delta / newN : 0;
          avg_res += deltaN * cnt_typed_scalar->value;
          m2_res += (m2_typed_scalar->value +
                     delta * deltaN * cnt_res * cnt_typed_scalar->value);
          cnt_res += cnt_typed_scalar->value;
        }
      }
    }

    std::shared_ptr<arrow::Array> stddev_samp_out;
    std::shared_ptr<arrow::Scalar> stddev_samp_scalar_out;
    if (cnt_res - 1 < 0.00001) {
      // double stddev_samp = std::numeric_limits<double>::quiet_NaN();
      double stddev_samp = std::numeric_limits<double>::infinity();
      stddev_samp_scalar_out = arrow::MakeScalar(stddev_samp);
    } else if (cnt_res < 0.00001) {
      stddev_samp_scalar_out = MakeNullScalar(arrow::float64());
    } else {
      double stddev_samp = sqrt(m2_res / (cnt_res > 1 ? (cnt_res - 1) : 1));
      stddev_samp_scalar_out = arrow::MakeScalar(stddev_samp);
    }
    RETURN_NOT_OK(
        arrow::MakeArrayFromScalar(*stddev_samp_scalar_out.get(), 1, &stddev_samp_out));
    out->push_back(stddev_samp_out);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::shared_ptr<arrow::DataType> cnt_res_data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> cnt_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> avg_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> m2_scalar_list_;
};

arrow::Status StddevSampFinalArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                               std::shared_ptr<arrow::DataType> data_type,
                                               std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<StddevSampFinalArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

StddevSampFinalArrayKernel::StddevSampFinalArrayKernel(
    arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "StddevSampFinalArrayKernel";
}

arrow::Status StddevSampFinalArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status StddevSampFinalArrayKernel::Finish(ArrayList* out) {
  return impl_->Finish(out);
}

#undef PROCESS_SUPPORTED_TYPES

///////////////  EncodeArray  ////////////////
class EncodeArrayKernel::Impl {
 public:
  Impl() {}
  virtual ~Impl() {}
  virtual arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                                 std::shared_ptr<arrow::Array>* out) = 0;
};

template <typename InType, typename MemoTableType>
class EncodeArrayTypedImpl : public EncodeArrayKernel::Impl {
 public:
  EncodeArrayTypedImpl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
    hash_table_ = std::make_shared<MemoTableType>(ctx_->memory_pool());
    builder_ = std::make_shared<arrow::Int32Builder>(ctx_->memory_pool());
  }
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                         std::shared_ptr<arrow::Array>* out) {
    // arrow::compute::Datum input_datum(in);
    // RETURN_NOT_OK(arrow::compute::Group<InType>(ctx_, input_datum, hash_table_, out));
    // we should put items into hashmap
    builder_->Reset();
    auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
    auto insert_on_found = [this](int32_t i) { builder_->Append(i); };
    auto insert_on_not_found = [this](int32_t i) { builder_->Append(i); };

    int cur_id = 0;
    int memo_index = 0;
    if (typed_array->null_count() == 0) {
      for (; cur_id < typed_array->length(); cur_id++) {
        hash_table_->GetOrInsert(typed_array->GetView(cur_id), insert_on_found,
                                 insert_on_not_found, &memo_index);
      }
    } else {
      for (; cur_id < typed_array->length(); cur_id++) {
        if (typed_array->IsNull(cur_id)) {
          hash_table_->GetOrInsertNull(insert_on_found, insert_on_not_found);
        } else {
          hash_table_->GetOrInsert(typed_array->GetView(cur_id), insert_on_found,
                                   insert_on_not_found, &memo_index);
        }
      }
    }
    RETURN_NOT_OK(builder_->Finish(out));
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<MemoTableType> hash_table_;
  std::shared_ptr<arrow::Int32Builder> builder_;
};

arrow::Status EncodeArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                      std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<EncodeArrayKernel>(ctx);
  return arrow::Status::OK();
}

EncodeArrayKernel::EncodeArrayKernel(arrow::compute::FunctionContext* ctx) {
  ctx_ = ctx;
  kernel_name_ = "EncodeArrayKernel";
}

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::Time32Type)             \
  PROCESS(arrow::Time64Type)             \
  PROCESS(arrow::TimestampType)          \
  PROCESS(arrow::BinaryType)             \
  PROCESS(arrow::StringType)             \
  PROCESS(arrow::FixedSizeBinaryType)    \
  PROCESS(arrow::Decimal128Type)
arrow::Status EncodeArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in,
                                          std::shared_ptr<arrow::Array>* out) {
  if (!impl_) {
    switch (in->type_id()) {
#define PROCESS(InType)                                                                \
  case InType::type_id: {                                                              \
    using MemoTableType = typename arrow::internal::HashTraits<InType>::MemoTableType; \
    impl_.reset(new EncodeArrayTypedImpl<InType, MemoTableType>(ctx_));                \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      default: {
        std::cout << "EncodeArrayKernel type not found, type is " << in->type()
                  << std::endl;
      } break;
    }
  }
  return impl_->Evaluate(in, out);
}
#undef PROCESS_SUPPORTED_TYPES

///////////////  HashAggrArray  ////////////////
class HashArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       std::vector<std::shared_ptr<arrow::DataType>> type_list)
      : ctx_(ctx) {
    // create a new result array type here
    std::vector<std::shared_ptr<gandiva::Node>> func_node_list = {};
    std::vector<std::shared_ptr<arrow::Field>> field_list = {};

    gandiva::ExpressionPtr expr;
    int index = 0;
    for (auto type : type_list) {
      auto field = arrow::field(std::to_string(index), type);
      field_list.push_back(field);
      auto field_node = gandiva::TreeExprBuilder::MakeField(field);
      auto func_node =
          gandiva::TreeExprBuilder::MakeFunction("hash64", {field_node}, arrow::int64());
      func_node_list.push_back(func_node);
      if (func_node_list.size() == 2) {
        auto shift_func_node = gandiva::TreeExprBuilder::MakeFunction(
            "multiply",
            {func_node_list[0], gandiva::TreeExprBuilder::MakeLiteral((int64_t)10)},
            arrow::int64());
        auto tmp_func_node = gandiva::TreeExprBuilder::MakeFunction(
            "add", {shift_func_node, func_node_list[1]}, arrow::int64());
        func_node_list.clear();
        func_node_list.push_back(tmp_func_node);
      }
      index++;
    }
    expr = gandiva::TreeExprBuilder::MakeExpression(func_node_list[0],
                                                    arrow::field("res", arrow::int64()));
#ifdef DEBUG
    std::cout << expr->ToString() << std::endl;
#endif
    schema_ = arrow::schema(field_list);
    auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
    auto status = gandiva::Projector::Make(schema_, {expr}, configuration, &projector);
    pool_ = ctx_->memory_pool();
  }

  arrow::Status Evaluate(const ArrayList& in, std::shared_ptr<arrow::Array>* out) {
    auto length = in[0]->length();
    auto num_columns = in.size();

    auto in_batch = arrow::RecordBatch::Make(schema_, length, in);
    // arrow::PrettyPrintOptions print_option(2, 500);
    // arrow::PrettyPrint(*in_batch.get(), print_option, &std::cout);

    arrow::ArrayVector outputs;
    RETURN_NOT_OK(projector->Evaluate(*in_batch, pool_, &outputs));
    *out = outputs[0];

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<gandiva::Projector> projector;
  std::shared_ptr<arrow::Schema> schema_;
  arrow::MemoryPool* pool_;
};

arrow::Status HashArrayKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::DataType>> type_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<HashArrayKernel>(ctx, type_list);
  return arrow::Status::OK();
}

HashArrayKernel::HashArrayKernel(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::DataType>> type_list) {
  impl_.reset(new Impl(ctx, type_list));
  kernel_name_ = "HashArrayKernel";
}

arrow::Status HashArrayKernel::Evaluate(const ArrayList& in,
                                        std::shared_ptr<arrow::Array>* out) {
  return impl_->Evaluate(in, out);
}

///////////////  ConcatArrayList  ////////////////
class ConcatArrayListKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
       std::shared_ptr<gandiva::Node> root_node,
       const std::vector<std::shared_ptr<arrow::Field>>& output_field_list)
      : ctx_(ctx) {}
  virtual ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    if (cached_.size() == 0) {
      for (int i = 0; i < in.size(); i++) {
        cached_.push_back({in[i]});
      }
    } else {
      for (int i = 0; i < in.size(); i++) {
        cached_[i].push_back(in[i]);
      }
    }
    total_num_row_ += in[0]->length();
    total_num_batch_ += 1;
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    *out = std::make_shared<ConcatArrayListResultIterator>(ctx_, schema, cached_);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::vector<arrow::ArrayVector> cached_;
  int total_num_row_ = 0;
  int total_num_batch_ = 0;
  class ConcatArrayListResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    ConcatArrayListResultIterator(arrow::compute::FunctionContext* ctx,
                                  std::shared_ptr<arrow::Schema> result_schema,
                                  const std::vector<arrow::ArrayVector>& cached)
        : ctx_(ctx), result_schema_(result_schema), cached_(cached) {
      batch_size_ = GetBatchSize();
      if (cached.size() > 0) {
        cached_num_batches_ = cached[0].size();
      }
    }

    std::string ToString() override { return "ConcatArrayListResultIterator"; }

    bool HasNext() override {
      if (cached_.size() == 0 || cur_batch_idx_ >= cached_num_batches_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      if (cached_.size() == 0 || cur_batch_idx_ >= cached_num_batches_) {
        return arrow::Status::OK();
      }
      // check array length
      int tmp_len = 0;
      int end_arr_idx = cur_batch_idx_;
      for (int i = cur_batch_idx_; i < cached_num_batches_; i++) {
        auto arr = cached_[0][i];
        tmp_len += arr->length();
        end_arr_idx++;
        if (tmp_len > 20480) {
          break;
        }
      }
      arrow::ArrayVector concatenated_array_list;
      for (auto arr_list : cached_) {
        std::shared_ptr<arrow::Array> concatenated_array;
        arrow::ArrayVector to_be_concat_arr(arr_list.begin() + cur_batch_idx_,
                                            arr_list.begin() + end_arr_idx);

        RETURN_NOT_OK(arrow::Concatenate(to_be_concat_arr, ctx_->memory_pool(),
                                         &concatenated_array));
        concatenated_array_list.push_back(concatenated_array);
      }
      int length = concatenated_array_list[0]->length();

      *out = arrow::RecordBatch::Make(result_schema_, length, concatenated_array_list);
      cur_batch_idx_ = end_arr_idx;
      return arrow::Status::OK();
    }

   private:
    arrow::compute::FunctionContext* ctx_;
    std::vector<arrow::ArrayVector> cached_;
    std::shared_ptr<arrow::Schema> result_schema_;
    int batch_size_;
    int cached_num_batches_ = 0;
    int cur_batch_idx_ = 0;
  };
};

arrow::Status ConcatArrayListKernel::Make(
    arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
    std::shared_ptr<gandiva::Node> root_node,
    const std::vector<std::shared_ptr<arrow::Field>>& output_field_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ConcatArrayListKernel>(ctx, input_field_list, root_node,
                                                 output_field_list);
  return arrow::Status::OK();
}

ConcatArrayListKernel::ConcatArrayListKernel(
    arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
    std::shared_ptr<gandiva::Node> root_node,
    const std::vector<std::shared_ptr<arrow::Field>>& output_field_list) {
  impl_.reset(new Impl(ctx, input_field_list, root_node, output_field_list));
  kernel_name_ = "ConcatArrayListKernel";
}

arrow::Status ConcatArrayListKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status ConcatArrayListKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
