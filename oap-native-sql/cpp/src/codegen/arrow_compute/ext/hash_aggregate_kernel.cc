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

#include <arrow/compute/context.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <vector>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include "codegen/arrow_compute/ext/codegen_register.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/typed_action_codegen_impl.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  SortArraysToIndices  ////////////////
class HashAggregateKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       std::vector<std::shared_ptr<arrow::Field>> input_field_list,
       std::vector<std::shared_ptr<gandiva::Node>> action_list,
       std::shared_ptr<arrow::Schema> result_schema)
      : ctx_(ctx),
        input_field_list_(input_field_list),
        action_list_(action_list),
        result_schema_(result_schema) {
    // if there is projection inside aggregate, we need to extract them into
    // projector_list
    THROW_NOT_OK(PrepareActionCodegen());
    THROW_NOT_OK(LoadJITFunction());
  }
  virtual arrow::Status LoadJITFunction() {
    // generate ddl signature
    std::stringstream func_args_ss;
    func_args_ss << "[HashAggregate]";
    func_args_ss << "[input_schema]";
    for (auto field : input_field_list_) {
      func_args_ss << field->type()->ToString() << "|";
    }
    func_args_ss << "[actions]";
    for (auto action : action_list_) {
      std::shared_ptr<CodeGenRegister> node_tmp;
      RETURN_NOT_OK(MakeCodeGenRegister(action, &node_tmp));
      func_args_ss << node_tmp->GetFingerprint() << "|";
    }
    func_args_ss << "[output_schema]";
    for (auto field : result_schema_->fields()) {
      func_args_ss << field->type()->ToString() << "|";
    }

    //#ifdef DEBUG
    std::cout << "func_args_ss is " << func_args_ss.str() << std::endl;
    //#endif

    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(func_args_ss.str());
    std::string signature = signature_ss.str();
    std::cout << "signature is " << signature << std::endl;

    auto file_lock = FileSpinLock();
    auto status = LoadLibrary(signature, ctx_, &hash_aggregater_);
    if (!status.ok()) {
      // process
      auto codes = ProduceCodes();
      // compile codes
      RETURN_NOT_OK(CompileCodes(codes, signature));
      RETURN_NOT_OK(LoadLibrary(signature, ctx_, &hash_aggregater_));
    }
    FileSpinUnLock(file_lock);
    return arrow::Status::OK();
  }

  virtual arrow::Status Evaluate(const ArrayList& in) {
    if (projector_) {
      auto length = in.size() > 0 ? in[0]->length() : 0;
      arrow::ArrayVector outputs;
      auto in_batch = arrow::RecordBatch::Make(original_input_schema_, length, in);
      RETURN_NOT_OK(projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      auto out_batch = arrow::RecordBatch::Make(projected_input_schema_, length, outputs);
      RETURN_NOT_OK(hash_aggregater_->Evaluate(in, out_batch));
    } else {
      std::shared_ptr<arrow::RecordBatch> empty_batch;
      RETURN_NOT_OK(hash_aggregater_->Evaluate(in, empty_batch));
    }
    return arrow::Status::OK();
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    RETURN_NOT_OK(hash_aggregater_->MakeResultIterator(schema, out));
    return arrow::Status::OK();
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::OK();
  }

 protected:
  std::vector<std::shared_ptr<arrow::Field>> input_field_list_;
  std::shared_ptr<arrow::Schema> original_input_schema_;
  std::shared_ptr<arrow::Schema> projected_input_schema_;
  std::vector<std::shared_ptr<gandiva::Node>> action_list_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<CodeGenBase> hash_aggregater_;
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::shared_ptr<arrow::Field>> field_list_;
  std::shared_ptr<gandiva::Projector> projector_;
  std::vector<std::shared_ptr<ActionCodeGen>> action_impl_list_;
  std::vector<std::pair<std::shared_ptr<arrow::Field>, std::string>> key_list_;

  arrow::Status PrepareActionCodegen() {
    std::vector<gandiva::ExpressionPtr> expr_list;
    std::vector<gandiva::FieldPtr> output_field_list;
    for (auto func_node : action_list_) {
      std::shared_ptr<CodeGenNodeVisitor> codegen_visitor;
      std::shared_ptr<ActionCodeGen> action_codegen;
      RETURN_NOT_OK(MakeCodeGenNodeVisitor(func_node, input_field_list_, &action_codegen,
                                           &codegen_visitor));
      action_impl_list_.push_back(action_codegen);
      if (action_codegen->IsPreProjected()) {
        auto expr = action_codegen->GetProjectorExpr();
        output_field_list.push_back(expr->result());
        expr_list.push_back(expr);
      }
    }
    RETURN_NOT_OK(GetGroupKey(action_impl_list_, &key_list_));
    if (key_list_.size() > 1) {
      auto expr = GetConcatedKernel(key_list_);
      output_field_list.push_back(expr->result());
      expr_list.push_back(expr);
    }
    if (!expr_list.empty()) {
      original_input_schema_ = arrow::schema(input_field_list_);
      projected_input_schema_ = arrow::schema(output_field_list);
      auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
      RETURN_NOT_OK(gandiva::Projector::Make(original_input_schema_, expr_list,
                                             configuration, &projector_));
    }
    return arrow::Status::OK();
  }

  virtual std::string ProduceCodes() {
    int indice = 0;

    std::vector<std::string> cached_list;
    std::vector<std::string> typed_input_list;
    std::vector<std::string> result_cached_list;

    std::stringstream evaluate_get_typed_array_ss;
    std::stringstream compute_on_exists_ss;
    std::stringstream compute_on_new_ss;
    std::stringstream cached_define_ss;
    std::stringstream on_finish_ss;
    std::stringstream result_cached_define_ss;
    std::vector<std::string> result_cached_parameter_list;
    std::vector<std::string> result_cached_array_list;
    std::stringstream result_cached_prepare_ss;
    std::stringstream result_cached_to_builder_ss;
    std::stringstream result_cached_to_array_ss;
    std::stringstream result_cached_array_ss;
    for (auto action : action_impl_list_) {
      auto func_sig_list = action->GetVariablesList();
      auto evaluate_get_typed_array_list = action->GetTypedInputList();
      auto compute_on_exists_prepare_list = action->GetOnExistsPrepareCodesList();
      auto compute_on_new_prepare_list = action->GetOnNewPrepareCodesList();
      auto compute_on_exists_list = action->GetOnExistsCodesList();
      auto compute_on_new_list = action->GetOnNewCodesList();
      auto cached_define_list = action->GetVariablesDefineList();
      int i = 0;
      for (auto sig : func_sig_list) {
        if (std::find(cached_list.begin(), cached_list.end(), sig) == cached_list.end()) {
          cached_list.push_back(sig);
          if (evaluate_get_typed_array_list[i].first != "" &&
              std::find(typed_input_list.begin(), typed_input_list.end(),
                        evaluate_get_typed_array_list[i].first) ==
                  typed_input_list.end()) {
            typed_input_list.push_back(evaluate_get_typed_array_list[i].first);
            evaluate_get_typed_array_ss << evaluate_get_typed_array_list[i].second
                                        << std::endl;
            compute_on_exists_ss << compute_on_exists_prepare_list[i] << std::endl;
            compute_on_new_ss << compute_on_new_prepare_list[i] << std::endl;
          }
          compute_on_exists_ss << compute_on_exists_list[i] << std::endl;
          compute_on_new_ss << compute_on_new_list[i] << std::endl;
          cached_define_ss << cached_define_list[i] << std::endl;
        }
        i++;
      }
      auto result_variables_list = action->GetFinishVariablesList();
      auto on_finish_list = action->GetOnFinishCodesList();
      auto result_cached_define_list = action->GetFinishVariablesDefineList();
      auto result_cached_parameter_list_tmp = action->GetFinishVariablesParameterList();
      auto result_cached_prepare_list = action->GetFinishVariablesPrepareList();
      auto result_cached_to_builder_list = action->GetFinishVariablesToBuilderList();
      auto result_cached_to_array_list = action->GetFinishVariablesToArrayList();
      auto result_cached_array_list_tmp = action->GetFinishVariablesArrayList();
      i = 0;
      for (auto sig : result_variables_list) {
        if (std::find(result_cached_list.begin(), result_cached_list.end(), sig) ==
            result_cached_list.end()) {
          result_cached_list.push_back(sig);
          on_finish_ss << on_finish_list[i] << std::endl;
          result_cached_define_ss << result_cached_define_list[i] << std::endl;
          result_cached_parameter_list.push_back(result_cached_parameter_list_tmp[i]);
          result_cached_prepare_ss << result_cached_prepare_list[i] << std::endl;
          result_cached_to_builder_ss << result_cached_to_builder_list[i] << std::endl;
          result_cached_to_array_ss << result_cached_to_array_list[i] << std::endl;
        }
        result_cached_array_list.push_back(result_cached_array_list_tmp[i]);
        i++;
      }
    }
    auto evaluate_get_typed_array_str = evaluate_get_typed_array_ss.str();
    auto typed_input_parameter_str = GetParameterList(typed_input_list);
    auto compute_on_exists_str = compute_on_exists_ss.str();
    auto compute_on_new_str = compute_on_new_ss.str();
    auto impl_cached_define_str = cached_define_ss.str();
    auto on_finish_str = on_finish_ss.str();
    auto on_finish_cached_parameter_str = GetParameterList(result_cached_list, false);
    auto result_cached_parameter_str =
        GetParameterList(result_cached_parameter_list, false);
    auto result_cached_prepare_str = result_cached_prepare_ss.str();
    auto result_cached_define_str = result_cached_define_ss.str();
    auto result_cached_to_builder_str = result_cached_to_builder_ss.str();
    auto result_cached_to_array_str = result_cached_to_array_ss.str();
    auto result_cached_array_str = GetParameterList(result_cached_array_list, false);

    bool multiple_cols = (key_list_.size() > 1);
    std::string concat_kernel;
    std::string hash_map_type_str = "typename arrow::internal::HashTraits<arrow::" +
                                    GetTypeString(arrow::int32(), "Type") +
                                    ">::MemoTableType";
    ;
    std::string evaluate_get_typed_key_array_str;
    std::string evaluate_get_typed_key_method_str;
    if (!multiple_cols) {
      hash_map_type_str = "typename arrow::internal::HashTraits<arrow::" +
                          GetTypeString(key_list_[0].first->type(), "Type") +
                          ">::MemoTableType";
      evaluate_get_typed_key_array_str =
          "auto typed_array = std::dynamic_pointer_cast<arrow::" +
          GetTypeString(key_list_[0].first->type(), "Array") + ">(" +
          key_list_[0].second + ");";
      if (key_list_[0].first->type()->id() != arrow::Type::STRING) {
        evaluate_get_typed_key_method_str = "GetView";
      } else {
        evaluate_get_typed_key_method_str = "GetString";
      }
    } else {
      evaluate_get_typed_key_array_str =
          "auto typed_array = "
          "std::dynamic_pointer_cast<arrow::Int32Array>(projected_batch->GetColumnByName("
          "\"projection_key\"));";
      evaluate_get_typed_key_method_str = "GetView";
    }

    return BaseCodes() + R"(
using HashMap = )" +
           hash_map_type_str + R"(;

class TypedGroupbyHashAggregateImpl : public CodeGenBase {
 public:
  TypedGroupbyHashAggregateImpl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
    hash_table_ = std::make_shared<HashMap>(ctx_->memory_pool());
  }

  arrow::Status Evaluate(const ArrayList& in, const std::shared_ptr<arrow::RecordBatch>& projected_batch) override {
    )" + evaluate_get_typed_key_array_str +
           evaluate_get_typed_array_str +
           R"(
    auto insert_on_found = [this)" +
           typed_input_parameter_str + R"(](int32_t i) {
      )" + compute_on_exists_str +
           R"(
    };
    auto insert_on_not_found = [this)" +
           typed_input_parameter_str + R"(](int32_t i) {
      num_groups_ ++;
      )" + compute_on_new_str +
           R"(
    };

    cur_id_ = 0;
    int memo_index = 0;
    if (typed_array->null_count() == 0) {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        hash_table_->GetOrInsert(typed_array->)" +
           evaluate_get_typed_key_method_str + R"((cur_id_), insert_on_found,
                                 insert_on_not_found, &memo_index);
      }
    } else {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        if (typed_array->IsNull(cur_id_)) {
          //hash_table_->GetOrInsertNull(insert_on_found, insert_on_not_found);
        } else {
          hash_table_->GetOrInsert(typed_array->)" +
           evaluate_get_typed_key_method_str + R"((cur_id_),
                                   insert_on_found, insert_on_not_found,
                                   &memo_index);
        }
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
        for (auto i = 0; i < num_groups_; i++) {)" +
           on_finish_str + R"(
        }
      *out = std::make_shared<HashAggregationResultIterator>(
        ctx_, schema, num_groups_,)" +
           on_finish_cached_parameter_str + R"(
    );
    return arrow::Status::OK();
  }

 private:
  )" + impl_cached_define_str +
           R"(
  arrow::compute::FunctionContext* ctx_;
  uint64_t num_groups_ = 0;
  uint64_t cur_id_ = 0;
  std::shared_ptr<HashMap> hash_table_;

  class HashAggregationResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    HashAggregationResultIterator(
      arrow::compute::FunctionContext* ctx,
      std::shared_ptr<arrow::Schema> schema,
      uint64_t num_groups,
   )" + result_cached_parameter_str +
           R"(): ctx_(ctx), result_schema_(schema), total_length_(num_groups) {
     )" + result_cached_prepare_str +
           R"(
    }

    std::string ToString() override { return "HashAggregationResultIterator"; }

    bool HasNext() override {
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      auto length = (total_length_ - offset_) > )" +
           std::to_string(GetBatchSize()) + R"( ? )" + std::to_string(GetBatchSize()) +
           R"( : (total_length_ - offset_);
      uint64_t count = 0;
      while (count < length) {
        )" +
           result_cached_to_builder_str + R"(
        count ++;
      }
      offset_ += length;
      )" + result_cached_to_array_str +
           R"(
      *out = arrow::RecordBatch::Make(result_schema_, length, {)" +
           result_cached_array_str + R"(});
      return arrow::Status::OK();
    }

   private:
   )" + result_cached_define_str +
           R"(
    std::shared_ptr<arrow::Array> indices_in_cache_;
    uint64_t offset_ = 0;
    ArrayItemIndex* indices_begin_;
    const uint64_t total_length_;
    std::shared_ptr<arrow::Schema> result_schema_;
    arrow::compute::FunctionContext* ctx_;
  };
};

extern "C" void MakeCodeGen(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<CodeGenBase>* out) {
  *out = std::make_shared<TypedGroupbyHashAggregateImpl>(ctx);
}

    )";
  }

  arrow::Status GetGroupKey(
      std::vector<std::shared_ptr<ActionCodeGen>> action_impl_list,
      std::vector<std::pair<std::shared_ptr<arrow::Field>, std::string>>*
          key_index_list) {
    for (auto action : action_impl_list) {
      if (action->IsGroupBy()) {
        key_index_list->push_back(std::make_pair(action->GetInputFieldList()[0],
                                                 action->GetInputDataNameList()[0]));
        std::cout << action->GetInputFieldList()[0]->ToString() << std::endl;
      }
    }
    return arrow::Status::OK();
  }

  std::string GetParameterList(const std::vector<std::string>& cached_list,
                               bool pre_comma = true) {
    std::stringstream ss;
    for (int i = 0; i < cached_list.size(); i++) {
      if (i != (cached_list.size() - 1)) {
        ss << cached_list[i] << ", ";
      } else {
        ss << cached_list[i];
      }
    }
    auto ret = ss.str();
    if (!pre_comma) {
      return ret;
    }
    if (ret.empty()) {
      return ret;
    } else {
      return ", " + ret;
    }
  }

  gandiva::ExpressionPtr GetConcatedKernel(
      std::vector<std::pair<std::shared_ptr<arrow::Field>, std::string>> key_list) {
    int index = 0;
    std::vector<std::shared_ptr<gandiva::Node>> func_node_list = {};
    std::vector<std::shared_ptr<arrow::Field>> field_list = {};
    for (auto type_name_pair : key_list) {
      auto type = type_name_pair.first->type();
      auto name = type_name_pair.first->name();
      auto field = arrow::field(name, type);
      field_list.push_back(field);
      auto field_node = gandiva::TreeExprBuilder::MakeField(field);
      auto func_node =
          gandiva::TreeExprBuilder::MakeFunction("hash32", {field_node}, arrow::int32());
      func_node_list.push_back(func_node);
      if (func_node_list.size() == 2) {
        auto shift_func_node = gandiva::TreeExprBuilder::MakeFunction(
            "multiply",
            {func_node_list[0], gandiva::TreeExprBuilder::MakeLiteral((int32_t)10)},
            arrow::int32());
        auto tmp_func_node = gandiva::TreeExprBuilder::MakeFunction(
            "add", {shift_func_node, func_node_list[1]}, arrow::int32());
        func_node_list.clear();
        func_node_list.push_back(tmp_func_node);
      }
      index++;
    }
    return gandiva::TreeExprBuilder::MakeExpression(
        func_node_list[0], arrow::field("projection_key", arrow::int32()));
  }
};

arrow::Status HashAggregateKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> input_field_list,
    std::vector<std::shared_ptr<gandiva::Node>> action_list,
    std::shared_ptr<arrow::Schema> result_schema, std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<HashAggregateKernel>(ctx, input_field_list, action_list,
                                               result_schema);
  return arrow::Status::OK();
}

HashAggregateKernel::HashAggregateKernel(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> input_field_list,
    std::vector<std::shared_ptr<gandiva::Node>> action_list,
    std::shared_ptr<arrow::Schema> result_schema) {
  impl_.reset(new Impl(ctx, input_field_list, action_list, result_schema));
  kernel_name_ = "HashAggregateKernelKernel";
}
#undef PROCESS_SUPPORTED_TYPES

arrow::Status HashAggregateKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status HashAggregateKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
