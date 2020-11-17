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

#include <arrow/array.h>
#include <arrow/compute/context.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <unordered_map>

#include "codegen/arrow_compute/ext/array_appender.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/expression_codegen_visitor.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/typed_node_visitor.h"
#include "codegen/common/hash_relation_number.h"
#include "codegen/common/hash_relation_string.h"
#include "precompile/unsafe_array.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  ConditionedProbe  ////////////////
class ConditionedProbeKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       const gandiva::NodeVector& left_key_node_list,
       const gandiva::NodeVector& right_key_node_list,
       const gandiva::NodeVector& left_schema_node_list,
       const gandiva::NodeVector& right_schema_node_list,
       const gandiva::NodePtr& condition, int join_type,
       const gandiva::NodeVector& result_node_list,
       const gandiva::NodeVector& hash_configuration_list, int hash_relation_idx)
      : ctx_(ctx),
        join_type_(join_type),
        condition_(condition),
        hash_relation_id_(hash_relation_idx) {
    for (auto node : left_schema_node_list) {
      left_field_list_.push_back(
          std::dynamic_pointer_cast<gandiva::FieldNode>(node)->field());
    }
    for (auto node : right_schema_node_list) {
      right_field_list_.push_back(
          std::dynamic_pointer_cast<gandiva::FieldNode>(node)->field());
    }
    for (auto node : result_node_list) {
      result_schema_.push_back(
          std::dynamic_pointer_cast<gandiva::FieldNode>(node)->field());
    }

    auto hash_map_type_str = gandiva::ToString(
        std::dynamic_pointer_cast<gandiva::LiteralNode>(hash_configuration_list[0])
            ->holder());
    hash_map_type_ = std::stoi(hash_map_type_str);
    /////////// right_key_list may need to do precodegen /////////////
    gandiva::FieldVector right_key_list;
    /** two scenarios:
     *  1. hash_map_type 0 => SHJ probe with no condition and single join
     *  2. hash_map_type 1 => BHJ probe with no condition and single join
     **/
    pre_processed_key_ = true;
    if (hash_map_type_ == 0 && right_key_node_list.size() == 1) {
      auto key_node = right_key_node_list[0];
      std::shared_ptr<TypedNodeVisitor> node_visitor;
      THROW_NOT_OK(MakeTypedNodeVisitor(key_node, &node_visitor));
      if (node_visitor->GetResultType() == TypedNodeVisitor::FieldNode) {
        pre_processed_key_ = false;
        std::shared_ptr<gandiva::FieldNode> field_node;
        node_visitor->GetTypedNode(&field_node);
        right_key_list.push_back(field_node->field());
        THROW_NOT_OK(
            GetIndexList(right_key_list, right_field_list_, &right_key_index_list_));
      }
    }
    /* *
     * Since we support two scenario here
     * 1. hash_map_type == 0 will use right_key_project_
     * 2. hash_map_type == 1 will use right_key_project_codegen_ and
     * right_key_hash_codegen_
     * */
    if (pre_processed_key_ && hash_map_type_ == 0) {
      right_key_project_expr_ = GetConcatedKernel(right_key_node_list);
      right_key_project_ = right_key_project_expr_->root();
    }
    if (hash_map_type_ == 1) {
      right_key_project_codegen_ = GetGandivaKernel(right_key_node_list);
      right_key_hash_codegen_ = GetHash32Kernel(right_key_node_list);
      for (auto expr : right_key_project_codegen_) {
        key_hash_field_list_.push_back(expr->result());
      }
    }

    /////////// map result_schema to input schema /////////////
    THROW_NOT_OK(
        GetIndexList(result_schema_, left_field_list_, &left_shuffle_index_list_));
    THROW_NOT_OK(
        GetIndexList(result_schema_, right_field_list_, &right_shuffle_index_list_));
    if (join_type != 4) {
      THROW_NOT_OK(GetIndexList(result_schema_, left_field_list_, right_field_list_,
                                false, &exist_index_, &result_schema_index_list_));
    } else {
      THROW_NOT_OK(GetIndexList(result_schema_, left_field_list_, right_field_list_, true,
                                &exist_index_, &result_schema_index_list_));
    }
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    if (condition_) {
      return arrow::Status::NotImplemented(
          "ConditionedProbeKernel(Non-Codegen) doesn't support condition.");
    }
    std::vector<gandiva::ExpressionVector> right_key_projector_list;
    std::shared_ptr<arrow::DataType> key_type;
    if (right_key_project_) {
      // hash_map_type == 0
      key_type = right_key_project_->return_type();
      right_key_projector_list.push_back({right_key_project_expr_});
    } else if (right_key_hash_codegen_) {
      // hash_map_type == 1
      key_type = right_key_hash_codegen_->result()->type();
      right_key_projector_list.push_back({right_key_hash_codegen_});
      right_key_projector_list.push_back(right_key_project_codegen_);
    } else {
      key_type = right_field_list_[right_key_index_list_[0]]->type();
    }
    *out = std::make_shared<ConditionedProbeResultIterator>(
        ctx_, right_key_index_list_, key_type, join_type_, right_key_projector_list,
        result_schema_, result_schema_index_list_, exist_index_, left_field_list_,
        right_field_list_);
    return arrow::Status::OK();
  }

  std::string GetSignature() { return ""; }

  arrow::Status DoCodeGen(int level, std::vector<std::string> input,
                          std::shared_ptr<CodeGenContext>* codegen_ctx_out, int* var_id) {
    auto codegen_ctx = std::make_shared<CodeGenContext>();

    codegen_ctx->header_codes.push_back(
        R"(#include "codegen/arrow_compute/ext/array_item_index.h")");

    std::vector<std::string> prepare_list;
    bool cond_check = false;
    if (condition_) cond_check = true;
    // 1.0 prepare hash relation columns
    std::stringstream hash_prepare_ss;
    std::stringstream hash_define_ss;
    hash_prepare_ss << "RETURN_NOT_OK(typed_dependent_iter_list[" << hash_relation_id_
                    << "]->Next("
                    << "&hash_relation_list_[" << hash_relation_id_ << "]));"
                    << std::endl;
    codegen_ctx->header_codes.push_back(R"(#include "codegen/common/hash_relation.h")");

    hash_prepare_ss << "hash_relation_list_" << hash_relation_id_
                    << "_ = hash_relation_list_[" << hash_relation_id_ << "];"
                    << std::endl;
    hash_define_ss << "std::shared_ptr<HashRelation> hash_relation_list_"
                   << hash_relation_id_ << "_;" << std::endl;
    for (int i = 0; i < left_field_list_.size(); i++) {
      std::stringstream hash_relation_col_name_ss;
      hash_relation_col_name_ss << "hash_relation_" << hash_relation_id_ << "_" << i;
      auto hash_relation_col_name = hash_relation_col_name_ss.str();
      auto hash_relation_col_type = left_field_list_[i]->type();
      hash_define_ss << "std::shared_ptr<"
                     << GetTemplateString(hash_relation_col_type,
                                          "TypedHashRelationColumn", "Type", "arrow::")
                     << "> " << hash_relation_col_name << ";" << std::endl;
      hash_prepare_ss << "RETURN_NOT_OK(hash_relation_list_[" << hash_relation_id_
                      << "]->GetColumn(" << i << ", &" << hash_relation_col_name << "));"
                      << std::endl;
    }
    codegen_ctx->hash_relation_prepare_codes = hash_prepare_ss.str();

    // define output list here, which will also be defined in class variables definition
    int idx = 0;
    for (auto field : result_schema_) {
      auto output_name = "hash_relation_" + std::to_string(hash_relation_id_) +
                         "_output_col_" + std::to_string(idx++);
      auto output_validity = output_name + "_validity";
      codegen_ctx->output_list.push_back(std::make_pair(output_name, field->type()));
      hash_define_ss << GetCTypeString(field->type()) << " " << output_name << ";"
                     << std::endl;
      hash_define_ss << "bool " << output_validity << ";" << std::endl;
    }

    codegen_ctx->definition_codes = hash_define_ss.str();
    // 1.1 prepare probe key column, name is key_0 and key_0_validity
    std::stringstream prepare_ss;

    std::vector<std::string> input_list;
    std::vector<std::string> project_output_list;
    auto unsafe_row_name = "unsafe_row_" + std::to_string(hash_relation_id_);
    bool do_unsafe_row = true;
    if (right_key_project_codegen_.size() == 1) {
      // when right_key is single and not string, we don't need to use unsafeRow
      // chendi: But we still use name unsafe_row_${id} to pass key data
      prepare_ss << GetCTypeString(right_key_project_codegen_[0]->result()->type()) << " "
                 << unsafe_row_name << ";" << std::endl;
      do_unsafe_row = false;
    } else {
      prepare_ss << "std::shared_ptr<UnsafeRow> " << unsafe_row_name
                 << " = std::make_shared<UnsafeRow>(" << right_key_project_codegen_.size()
                 << ");" << std::endl;
    }
    idx = 0;
    for (auto expr : right_key_project_codegen_) {
      std::shared_ptr<ExpressionCodegenVisitor> project_node_visitor;
      RETURN_NOT_OK(MakeExpressionCodegenVisitor(expr->root(), input, {right_field_list_},
                                                 -1, var_id, &input_list,
                                                 &project_node_visitor));
      prepare_ss << project_node_visitor->GetPrepare();
      auto key_name = project_node_visitor->GetResult();
      auto validity_name = project_node_visitor->GetPreCheck();
      if (do_unsafe_row) {
        prepare_ss << "if (" << validity_name << ") {" << std::endl;
        prepare_ss << "appendToUnsafeRow(" << unsafe_row_name << ".get(), " << idx << ", "
                   << key_name << ");" << std::endl;
        prepare_ss << "} else {" << std::endl;
        prepare_ss << "setNullAt(" << unsafe_row_name << ".get(), " << idx << ");"
                   << std::endl;
        prepare_ss << "}" << std::endl;
      } else {
        prepare_ss << "if (" << validity_name << ") {" << std::endl;
        prepare_ss << unsafe_row_name << " = " << key_name << ";" << std::endl;
        prepare_ss << "}" << std::endl;
      }

      project_output_list.push_back(project_node_visitor->GetResult());
      for (auto header : project_node_visitor->GetHeaders()) {
        if (std::find(codegen_ctx->header_codes.begin(), codegen_ctx->header_codes.end(),
                      header) == codegen_ctx->header_codes.end()) {
          codegen_ctx->header_codes.push_back(header);
        }
      }
      idx++;
    }
    std::shared_ptr<ExpressionCodegenVisitor> hash_node_visitor;
    RETURN_NOT_OK(MakeExpressionCodegenVisitor(
        right_key_hash_codegen_->root(), project_output_list, {key_hash_field_list_}, -1,
        var_id, &input_list, &hash_node_visitor));
    prepare_ss << hash_node_visitor->GetPrepare();
    auto key_name = hash_node_visitor->GetResult();
    auto validity_name = hash_node_visitor->GetPreCheck();
    prepare_ss << "auto key_" << hash_relation_id_ << " = " << key_name << ";"
               << std::endl;
    prepare_ss << "auto key_" << hash_relation_id_ << "_validity = " << validity_name
               << ";" << std::endl;
    for (auto header : hash_node_visitor->GetHeaders()) {
      if (std::find(codegen_ctx->header_codes.begin(), codegen_ctx->header_codes.end(),
                    header) == codegen_ctx->header_codes.end()) {
        codegen_ctx->header_codes.push_back(header);
      }
    }
    codegen_ctx->prepare_codes = prepare_ss.str();
    /////   inside loop  //////
    // 2. probe in hash_relation
    RETURN_NOT_OK(GetProcessProbe(input, join_type_, cond_check, &codegen_ctx));
    // 3. do continue if not exists
    if (cond_check) {
      std::shared_ptr<ExpressionCodegenVisitor> condition_node_visitor;
      RETURN_NOT_OK(MakeExpressionCodegenVisitor(
          condition_, input, {left_field_list_, right_field_list_}, hash_relation_id_,
          var_id, &prepare_list, &condition_node_visitor));
      auto function_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
      std::stringstream function_define_ss;
      function_define_ss << "bool " << function_name << "(ArrayItemIndex x, int y) {"
                         << std::endl;
      function_define_ss << condition_node_visitor->GetPrepare() << std::endl;
      function_define_ss << "return " << condition_node_visitor->GetResult() << ";"
                         << std::endl;
      function_define_ss << "}" << std::endl;
      codegen_ctx->function_list.push_back(function_define_ss.str());
      for (auto header : condition_node_visitor->GetHeaders()) {
        if (std::find(codegen_ctx->header_codes.begin(), codegen_ctx->header_codes.end(),
                      header) == codegen_ctx->header_codes.end()) {
          codegen_ctx->header_codes.push_back(header);
        }
      }
    }
    // set join output list for next kernel.
    ///////////////////////////////
    *codegen_ctx_out = codegen_ctx;
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  arrow::MemoryPool* pool_;
  std::string signature_;
  int join_type_;

  gandiva::NodePtr condition_;
  int hash_map_type_;

  // only be used when hash_map_type_ == 0
  gandiva::ExpressionPtr right_key_project_expr_;
  gandiva::NodePtr right_key_project_;
  // only be used when hash_map_type_ == 1
  gandiva::ExpressionPtr right_key_hash_codegen_;
  gandiva::ExpressionVector right_key_project_codegen_;
  gandiva::FieldVector key_hash_field_list_;

  bool pre_processed_key_ = false;
  gandiva::FieldVector left_field_list_;
  gandiva::FieldVector right_field_list_;
  gandiva::FieldVector result_schema_;
  std::vector<int> right_key_index_list_;
  std::vector<int> left_shuffle_index_list_;
  std::vector<int> right_shuffle_index_list_;
  std::vector<std::pair<int, int>> result_schema_index_list_;
  int exist_index_ = -1;
  int hash_relation_id_;
  std::vector<arrow::ArrayVector> cached_;

  class ConditionedProbeResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    ConditionedProbeResultIterator(
        arrow::compute::FunctionContext* ctx, std::vector<int> right_key_index_list,
        std::shared_ptr<arrow::DataType> key_type, int join_type,
        std::vector<gandiva::ExpressionVector> right_key_project_list,
        gandiva::FieldVector result_schema,
        std::vector<std::pair<int, int>> result_schema_index_list, int exist_index,
        gandiva::FieldVector left_field_list, gandiva::FieldVector right_field_list)
        : ctx_(ctx),
          right_key_index_list_(right_key_index_list),
          key_type_(key_type),
          join_type_(join_type),
          result_schema_index_list_(result_schema_index_list),
          exist_index_(exist_index),
          left_field_list_(left_field_list),
          right_field_list_(right_field_list) {
      result_schema_ = arrow::schema(result_schema);
      hash_map_type_ = right_key_project_list.size() == 2 ? 1 : 0;
      if (hash_map_type_ == 0) {
        if (right_key_project_list.size() == 1) {
          auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
          THROW_NOT_OK(gandiva::Projector::Make(arrow::schema(right_field_list_),
                                                right_key_project_list[0], configuration,
                                                &right_hash_key_project_));
        }
      } else if (hash_map_type_ == 1) {
        auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
        for (auto expr : right_key_project_list[1]) {
          right_projected_field_list_.push_back(expr->result());
        }
        THROW_NOT_OK(gandiva::Projector::Make(arrow::schema(right_projected_field_list_),
                                              right_key_project_list[0], configuration,
                                              &right_hash_key_project_));
        THROW_NOT_OK(gandiva::Projector::Make(arrow::schema(right_field_list_),
                                              right_key_project_list[1], configuration,
                                              &right_keys_project_));
      }
    }

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
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::StringType)
    arrow::Status SetDependencies(
        const std::vector<std::shared_ptr<ResultIteratorBase>>& dependent_iter_list) {
      auto iter = dependent_iter_list[0];
      auto typed_dependent =
          std::dynamic_pointer_cast<ResultIterator<HashRelation>>(iter);
      RETURN_NOT_OK(typed_dependent->Next(&hash_relation_));

      // chendi: previous result_schema_index_list design is little tricky, it put
      // existentce col at the back of all col while exists_index_ may be at middle out
      // real result. Add two index here.
      auto result_schema_length =
          (exist_index_ == -1 || exist_index_ == right_field_list_.size())
              ? result_schema_index_list_.size()
              : (result_schema_index_list_.size() - 1);
      int result_idx = 0;
      for (int i = 0; i < result_schema_length; i++) {
        auto pair = result_schema_index_list_[i];
        std::shared_ptr<arrow::DataType> type;
        AppenderBase::AppenderType appender_type;
        if (result_idx++ == exist_index_) {
          appender_type = AppenderBase::exist;
          type = arrow::boolean();
          if (result_idx < result_schema_index_list_.size()) i -= 1;
        } else {
          appender_type = pair.first == 0 ? AppenderBase::left : AppenderBase::right;
          if (pair.first == 0) {
            type = left_field_list_[pair.second]->type();
          } else {
            type = right_field_list_[pair.second]->type();
          }
        }

        std::shared_ptr<AppenderBase> appender;
        RETURN_NOT_OK(MakeAppender(ctx_, type, appender_type, &appender));
        // insert all left arrays
        if (pair.first == 0) {
          arrow::ArrayVector cached;
          RETURN_NOT_OK(hash_relation_->GetArrayVector(pair.second, &cached));
          for (auto arr : cached) {
            appender->AddArray(arr);
          }
        }
        appender_list_.push_back(appender);
      }

      // prepare probe function
      if (hash_map_type_ == 1) {
        // if hash_map_type == 1, we will simply use HashRelation
        switch (join_type_) {
          case 0: { /*Inner Join*/
            auto func = std::make_shared<UnsafeInnerProbeFunction>(hash_relation_,
                                                                   appender_list_);
            probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
          } break;
          case 1: { /*Outer Join*/
            auto func = std::make_shared<UnsafeOuterProbeFunction>(hash_relation_,
                                                                   appender_list_);
            probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
          } break;
          case 2: { /*Anti Join*/
            auto func =
                std::make_shared<UnsafeAntiProbeFunction>(hash_relation_, appender_list_);
            probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
          } break;
          case 3: { /*Semi Join*/
            auto func =
                std::make_shared<UnsafeSemiProbeFunction>(hash_relation_, appender_list_);
            probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
          } break;
          case 4: { /*Existence Join*/
            auto func = std::make_shared<UnsafeExistenceProbeFunction>(hash_relation_,
                                                                       appender_list_);
            probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
          } break;
          default:
            return arrow::Status::NotImplemented(
                "ConditionedProbeArraysTypedImpl only support join type: InnerJoin, "
                "RightJoin");
        }
      } else {
        // if hash_map_type == 0, we use TypedHashRelation
        switch (key_type_->id()) {
#define PROCESS(InType)                                                                  \
  case InType::type_id: {                                                                \
    switch (join_type_) {                                                                \
      case 0: { /*Inner Join*/                                                           \
        auto func = std::make_shared<InnerProbeFunction<InType>>(hash_relation_,         \
                                                                 appender_list_);        \
        probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);                \
      } break;                                                                           \
      case 1: { /*Outer Join*/                                                           \
        auto func = std::make_shared<OuterProbeFunction<InType>>(hash_relation_,         \
                                                                 appender_list_);        \
        probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);                \
      } break;                                                                           \
      case 2: { /*Anti Join*/                                                            \
        auto func =                                                                      \
            std::make_shared<AntiProbeFunction<InType>>(hash_relation_, appender_list_); \
        probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);                \
      } break;                                                                           \
      case 3: { /*Semi Join*/                                                            \
        auto func =                                                                      \
            std::make_shared<SemiProbeFunction<InType>>(hash_relation_, appender_list_); \
        probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);                \
      } break;                                                                           \
      case 4: { /*Existence Join*/                                                       \
        auto func = std::make_shared<ExistenceProbeFunction<InType>>(hash_relation_,     \
                                                                     appender_list_);    \
        probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);                \
      } break;                                                                           \
      default:                                                                           \
        return arrow::Status::NotImplemented(                                            \
            "ConditionedProbeArraysTypedImpl only support join type: InnerJoin, "        \
            "RightJoin");                                                                \
    }                                                                                    \
  } break;
          PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
          default: {
            std::cout << "ConditionedProbeArraysTypedImpl does not support key type as "
                      << key_type_ << std::endl;
          } break;
        }
      }
      return arrow::Status::OK();
    }
#undef PROCESS_SUPPORTED_TYPES

    arrow::Status Process(
        const std::vector<std::shared_ptr<arrow::Array>>& in,
        std::shared_ptr<arrow::RecordBatch>* out,
        const std::shared_ptr<arrow::Array>& selection = nullptr) override {
      // Get key array, which should be typed
      std::shared_ptr<arrow::Array> key_array;
      arrow::ArrayVector projected_keys_outputs;
      /**
       * if hash_map_type_ == 0, we only need to build a single-column hashArray for key
       * if hash_map_type_ == 1, we need to both get a single-column hashArray and
       *projected result of original keys for hashmap
       **/
      arrow::ArrayVector outputs;
      auto length = in.size() > 0 ? in[0]->length() : 0;
      std::shared_ptr<arrow::RecordBatch> in_batch =
          arrow::RecordBatch::Make(arrow::schema(right_field_list_), length, in);
      if (hash_map_type_ == 1) {
        RETURN_NOT_OK(right_keys_project_->Evaluate(*in_batch, ctx_->memory_pool(),
                                                    &projected_keys_outputs));
        in_batch = arrow::RecordBatch::Make(arrow::schema(right_projected_field_list_),
                                            in_batch->num_rows(), projected_keys_outputs);
        RETURN_NOT_OK(
            right_hash_key_project_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
        key_array = outputs[0];
      } else {
        if (right_hash_key_project_) {
          RETURN_NOT_OK(right_hash_key_project_->Evaluate(*in_batch, ctx_->memory_pool(),
                                                          &outputs));
          key_array = outputs[0];
        } else {
          key_array = in[right_key_index_list_[0]];
        }
      }
      // put in to ArrayAppender then doing evaluate
      for (int tmp_idx = 0; tmp_idx < appender_list_.size(); tmp_idx++) {
        auto appender = appender_list_[tmp_idx];
        if (appender->GetType() == AppenderBase::right) {
          auto idx_exclude_exist =
              (exist_index_ == -1 || tmp_idx < exist_index_) ? tmp_idx : (tmp_idx - 1);
          auto right_in_idx = result_schema_index_list_[idx_exclude_exist].second;
          RETURN_NOT_OK(appender->AddArray(in[right_in_idx]));
        }
      }
      uint64_t out_length = 0;
      if (hash_map_type_ == 0) {
        out_length = probe_func_->Evaluate(key_array);
      } else if (hash_map_type_ == 1) {
        out_length = probe_func_->Evaluate(key_array, projected_keys_outputs);
      }
      arrow::ArrayVector out_arr_list;
      for (auto appender : appender_list_) {
        std::shared_ptr<arrow::Array> out_arr;
        RETURN_NOT_OK(appender->Finish(&out_arr));
        out_arr_list.push_back(out_arr);
        if (appender->GetType() == AppenderBase::right) {
          RETURN_NOT_OK(appender->PopArray());
        }
        RETURN_NOT_OK(appender->Reset());
      }
      *out = arrow::RecordBatch::Make(result_schema_, out_length, out_arr_list);
      return arrow::Status::OK();
    }

   private:
    class ProbeFunctionBase {
     public:
      virtual uint64_t Evaluate(std::shared_ptr<arrow::Array>) { return 0; }
      virtual uint64_t Evaluate(std::shared_ptr<arrow::Array>,
                                const arrow::ArrayVector&) {
        return 0;
      }
    };
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
  PROCESS(arrow::Date64Type)
    class UnsafeInnerProbeFunction : public ProbeFunctionBase {
     public:
      UnsafeInnerProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                               std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : hash_relation_(hash_relation), appender_list_(appender_list) {}
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array,
                        const arrow::ArrayVector& key_payloads) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        bool do_unsafe_row = true;
        std::function<int(int i)> fast_probe;
        /* for single key case, we don't need to create unsafeRow */
        if (key_payloads.size() == 1) {
          do_unsafe_row = false;
          switch (key_payloads[0]->type_id()) {
#define PROCESS(InType)                                                      \
  case TypeTraits<InType>::type_id: {                                        \
    using ArrayType = precompile::TypeTraits<InType>::ArrayType;             \
    auto typed_first_key_arr = std::make_shared<ArrayType>(key_payloads[0]); \
    fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {       \
      return hash_relation_->Get(typed_key_array->GetView(i),                \
                                 typed_first_key_arr->GetView(i));           \
    };                                                                       \
  } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
            case TypeTraits<arrow::StringType>::type_id: {
              auto typed_first_key_arr = std::make_shared<StringArray>(key_payloads[0]);
              fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                return hash_relation_->Get(typed_key_array->GetView(i),
                                           typed_first_key_arr->GetString(i));
              };
            } break;
            default: {
              throw std::runtime_error(
                  "UnsafeInnerProbeFunction Evaluate doesn't support single key type ");
            } break;
          }
#undef PROCESS_SUPPORTED_TYPES
        } else {
          for (auto arr : key_payloads) {
            std::shared_ptr<UnsafeArray> payload;
            MakeUnsafeArray(arr->type(), i++, arr, &payload);
            payloads.push_back(payload);
          }
        }
        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          int index;
          if (!do_unsafe_row) {
            index = fast_probe(i);
          } else {
            auto unsafe_key_row = std::make_shared<UnsafeRow>(payloads.size());
            for (auto payload_arr : payloads) {
              payload_arr->Append(i, &unsafe_key_row);
            }
            index = hash_relation_->Get(typed_key_array->GetView(i), unsafe_key_row);
          }
          if (index == -1) {
            continue;
          }
          for (auto tmp : hash_relation_->GetItemListByIndex(index)) {
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->Append(tmp.array_id, tmp.id));
              } else {
                THROW_NOT_OK(appender->Append(0, i));
              }
            }
            out_length += 1;
          }
        }
        return out_length;
      }

     private:
      using ArrayType = arrow::Int32Array;
      std::shared_ptr<HashRelation> hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };
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
  PROCESS(arrow::Date64Type)
    class UnsafeOuterProbeFunction : public ProbeFunctionBase {
     public:
      UnsafeOuterProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                               std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : hash_relation_(hash_relation), appender_list_(appender_list) {}
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array,
                        const arrow::ArrayVector& key_payloads) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        bool do_unsafe_row = true;
        std::function<int(int i)> fast_probe;
        /* for single key case, we don't need to create unsafeRow */
        if (key_payloads.size() == 1) {
          do_unsafe_row = false;
          switch (key_payloads[0]->type_id()) {
#define PROCESS(InType)                                                      \
  case TypeTraits<InType>::type_id: {                                        \
    using ArrayType = precompile::TypeTraits<InType>::ArrayType;             \
    auto typed_first_key_arr = std::make_shared<ArrayType>(key_payloads[0]); \
    fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {       \
      return hash_relation_->Get(typed_key_array->GetView(i),                \
                                 typed_first_key_arr->GetView(i));           \
    };                                                                       \
  } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
            case TypeTraits<arrow::StringType>::type_id: {
              auto typed_first_key_arr = std::make_shared<StringArray>(key_payloads[0]);
              fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                return hash_relation_->Get(typed_key_array->GetView(i),
                                           typed_first_key_arr->GetString(i));
              };
            } break;
            default: {
              throw std::runtime_error(
                  "UnsafeOuterProbeFunction Evaluate doesn't support single key type ");
            } break;
          }
#undef PROCESS_SUPPORTED_TYPES
        } else {
          for (auto arr : key_payloads) {
            std::shared_ptr<UnsafeArray> payload;
            MakeUnsafeArray(arr->type(), i++, arr, &payload);
            payloads.push_back(payload);
          }
        }
        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          int index;
          if (!do_unsafe_row) {
            index = fast_probe(i);
          } else {
            auto unsafe_key_row = std::make_shared<UnsafeRow>(payloads.size());
            for (auto payload_arr : payloads) {
              payload_arr->Append(i, &unsafe_key_row);
            }
            index = hash_relation_->Get(typed_key_array->GetView(i), unsafe_key_row);
          }
          if (index == -1) {
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->AppendNull());
              } else {
                THROW_NOT_OK(appender->Append(0, i));
              }
            }
            out_length += 1;
            continue;
          }
          for (auto tmp : hash_relation_->GetItemListByIndex(index)) {
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->Append(tmp.array_id, tmp.id));
              } else {
                THROW_NOT_OK(appender->Append(0, i));
              }
            }
            out_length += 1;
          }
        }
        return out_length;
      }

     private:
      using ArrayType = arrow::Int32Array;
      std::shared_ptr<HashRelation> hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };
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
  PROCESS(arrow::Date64Type)
    class UnsafeAntiProbeFunction : public ProbeFunctionBase {
     public:
      UnsafeAntiProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                              std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : hash_relation_(hash_relation), appender_list_(appender_list) {}
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array,
                        const arrow::ArrayVector& key_payloads) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        bool do_unsafe_row = true;
        std::function<int(int i)> fast_probe;
        /* for single key case, we don't need to create unsafeRow */
        if (key_payloads.size() == 1) {
          do_unsafe_row = false;
          switch (key_payloads[0]->type_id()) {
#define PROCESS(InType)                                                      \
  case TypeTraits<InType>::type_id: {                                        \
    using ArrayType = precompile::TypeTraits<InType>::ArrayType;             \
    auto typed_first_key_arr = std::make_shared<ArrayType>(key_payloads[0]); \
    fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {       \
      return hash_relation_->IfExists(typed_key_array->GetView(i),           \
                                      typed_first_key_arr->GetView(i));      \
    };                                                                       \
  } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
            case TypeTraits<arrow::StringType>::type_id: {
              auto typed_first_key_arr = std::make_shared<StringArray>(key_payloads[0]);
              fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                return hash_relation_->IfExists(typed_key_array->GetView(i),
                                                typed_first_key_arr->GetString(i));
              };
            } break;
            default: {
              throw std::runtime_error(
                  "UnsafeAntiProbeFunction Evaluate doesn't support single key type ");
            } break;
          }
#undef PROCESS_SUPPORTED_TYPES
        } else {
          for (auto arr : key_payloads) {
            std::shared_ptr<UnsafeArray> payload;
            MakeUnsafeArray(arr->type(), i++, arr, &payload);
            payloads.push_back(payload);
          }
        }
        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          int index;
          if (!do_unsafe_row) {
            index = fast_probe(i);
          } else {
            auto unsafe_key_row = std::make_shared<UnsafeRow>(payloads.size());
            for (auto payload_arr : payloads) {
              payload_arr->Append(i, &unsafe_key_row);
            }
            index = hash_relation_->IfExists(typed_key_array->GetView(i), unsafe_key_row);
          }
          if (index == -1) {
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->AppendNull());
              } else {
                THROW_NOT_OK(appender->Append(0, i));
              }
            }
            out_length += 1;
          }
        }
        return out_length;
      }

     private:
      using ArrayType = arrow::Int32Array;
      std::shared_ptr<HashRelation> hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };

    class UnsafeSemiProbeFunction : public ProbeFunctionBase {
     public:
      UnsafeSemiProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                              std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : hash_relation_(hash_relation), appender_list_(appender_list) {}
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
  PROCESS(arrow::Date64Type)
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array,
                        const arrow::ArrayVector& key_payloads) override {
        auto typed_key_array = std::dynamic_pointer_cast<arrow::Int32Array>(key_array);
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        bool do_unsafe_row = true;
        std::function<int(int i)> fast_probe;
        /* for single key case, we don't need to create unsafeRow */
        if (key_payloads.size() == 1) {
          do_unsafe_row = false;
          switch (key_payloads[0]->type_id()) {
#define PROCESS(InType)                                                      \
  case TypeTraits<InType>::type_id: {                                        \
    using ArrayType = precompile::TypeTraits<InType>::ArrayType;             \
    auto typed_first_key_arr = std::make_shared<ArrayType>(key_payloads[0]); \
    fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {       \
      return hash_relation_->IfExists(typed_key_array->GetView(i),           \
                                      typed_first_key_arr->GetView(i));      \
    };                                                                       \
  } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
            case TypeTraits<arrow::StringType>::type_id: {
              auto typed_first_key_arr = std::make_shared<StringArray>(key_payloads[0]);
              fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                return hash_relation_->IfExists(typed_key_array->GetView(i),
                                                typed_first_key_arr->GetString(i));
              };
            } break;
            default: {
              throw std::runtime_error(
                  "UnsafeSemiProbeFunction Evaluate doesn't support single key type ");
            } break;
          }
#undef PROCESS_SUPPORTED_TYPES
        } else {
          for (auto arr : key_payloads) {
            std::shared_ptr<UnsafeArray> payload;
            MakeUnsafeArray(arr->type(), i++, arr, &payload);
            payloads.push_back(payload);
          }
        }

        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          int index;
          if (!do_unsafe_row) {
            index = fast_probe(i);
          } else {
            auto unsafe_key_row = std::make_shared<UnsafeRow>(payloads.size());
            for (auto payload_arr : payloads) {
              payload_arr->Append(i, &unsafe_key_row);
            }
            auto make_unsafe_row_end = std::chrono::steady_clock::now();
            index = hash_relation_->IfExists(typed_key_array->GetView(i), unsafe_key_row);
          }

          if (index == -1) {
            continue;
          }
          for (auto appender : appender_list_) {
            if (appender->GetType() == AppenderBase::left) {
              THROW_NOT_OK(appender->AppendNull());
            } else {
              THROW_NOT_OK(appender->Append(0, i));
            }
          }
          out_length += 1;
        }
        return out_length;
      }

     private:
      std::shared_ptr<HashRelation> hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };
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
  PROCESS(arrow::Date64Type)
    class UnsafeExistenceProbeFunction : public ProbeFunctionBase {
     public:
      UnsafeExistenceProbeFunction(
          std::shared_ptr<HashRelation> hash_relation,
          std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : hash_relation_(hash_relation), appender_list_(appender_list) {}
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array,
                        const arrow::ArrayVector& key_payloads) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        bool do_unsafe_row = true;
        std::function<int(int i)> fast_probe;
        /* for single key case, we don't need to create unsafeRow */
        if (key_payloads.size() == 1) {
          do_unsafe_row = false;
          switch (key_payloads[0]->type_id()) {
#define PROCESS(InType)                                                      \
  case TypeTraits<InType>::type_id: {                                        \
    using ArrayType = precompile::TypeTraits<InType>::ArrayType;             \
    auto typed_first_key_arr = std::make_shared<ArrayType>(key_payloads[0]); \
    fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {       \
      return hash_relation_->IfExists(typed_key_array->GetView(i),           \
                                      typed_first_key_arr->GetView(i));      \
    };                                                                       \
  } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
            case TypeTraits<arrow::StringType>::type_id: {
              auto typed_first_key_arr = std::make_shared<StringArray>(key_payloads[0]);
              fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                return hash_relation_->IfExists(typed_key_array->GetView(i),
                                                typed_first_key_arr->GetString(i));
              };
            } break;
            default: {
              throw std::runtime_error(
                  "UnsafeSemiProbeFunction Evaluate doesn't support single key type ");
            } break;
          }
#undef PROCESS_SUPPORTED_TYPES
        } else {
          for (auto arr : key_payloads) {
            std::shared_ptr<UnsafeArray> payload;
            MakeUnsafeArray(arr->type(), i++, arr, &payload);
            payloads.push_back(payload);
          }
        }
        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          int index;
          if (!do_unsafe_row) {
            index = fast_probe(i);
          } else {
            auto unsafe_key_row = std::make_shared<UnsafeRow>(payloads.size());
            for (auto payload_arr : payloads) {
              payload_arr->Append(i, &unsafe_key_row);
            }
            index = hash_relation_->IfExists(typed_key_array->GetView(i), unsafe_key_row);
          }
          bool exists = true;
          if (index == -1) {
            exists = false;
          }
          for (auto appender : appender_list_) {
            if (appender->GetType() == AppenderBase::exist) {
              THROW_NOT_OK(appender->AppendExistence(exists));
            } else if (appender->GetType() == AppenderBase::right) {
              THROW_NOT_OK(appender->Append(0, i));
            } else {
              THROW_NOT_OK(appender->AppendNull());
            }
          }
          out_length += 1;
        }
        return out_length;
      }

     private:
      using ArrayType = arrow::Int32Array;
      std::shared_ptr<HashRelation> hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };

    template <typename DataType>
    class InnerProbeFunction : public ProbeFunctionBase {
     public:
      InnerProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                         std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : appender_list_(appender_list) {
        typed_hash_relation_ =
            std::dynamic_pointer_cast<TypedHashRelation<DataType>>(hash_relation);
      }
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);

        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          int index;
          if (key_array->IsNull(i)) {
            index = typed_hash_relation_->GetNull();
          } else {
            index = typed_hash_relation_->Get(typed_key_array->GetView(i));
          }
          if (index == -1) {
            continue;
          }
          for (auto tmp : typed_hash_relation_->GetItemListByIndex(index)) {
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->Append(tmp.array_id, tmp.id));
              } else {
                THROW_NOT_OK(appender->Append(0, i));
              }
            }
            out_length += 1;
          }
        }
        return out_length;
      }

     private:
      using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
      std::shared_ptr<TypedHashRelation<DataType>> typed_hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };

    template <typename DataType>
    class OuterProbeFunction : public ProbeFunctionBase {
     public:
      OuterProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                         std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : appender_list_(appender_list) {
        typed_hash_relation_ =
            std::dynamic_pointer_cast<TypedHashRelation<DataType>>(hash_relation);
      }
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          int index;
          if (key_array->IsNull(i)) {
            index = typed_hash_relation_->GetNull();
          } else {
            index = typed_hash_relation_->Get(typed_key_array->GetView(i));
          }
          if (index == -1) {
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->AppendNull());
              } else {
                THROW_NOT_OK(appender->Append(0, i));
              }
            }
            out_length += 1;
            continue;
          }
          for (auto tmp : typed_hash_relation_->GetItemListByIndex(index)) {
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->Append(tmp.array_id, tmp.id));
              } else {
                THROW_NOT_OK(appender->Append(0, i));
              }
            }
            out_length += 1;
          }
        }
        return out_length;
      }

     private:
      using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
      std::shared_ptr<TypedHashRelation<DataType>> typed_hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };

    template <typename DataType>
    class AntiProbeFunction : public ProbeFunctionBase {
     public:
      AntiProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                        std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : appender_list_(appender_list) {
        typed_hash_relation_ =
            std::dynamic_pointer_cast<TypedHashRelation<DataType>>(hash_relation);
      }
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          int index;
          if (key_array->IsNull(i)) {
            index = typed_hash_relation_->GetNull();
          } else {
            index = typed_hash_relation_->Get(typed_key_array->GetView(i));
          }
          if (index == -1) {
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->AppendNull());
              } else {
                THROW_NOT_OK(appender->Append(0, i));
              }
            }
            out_length += 1;
          }
        }
        return out_length;
      }

     private:
      using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
      std::shared_ptr<TypedHashRelation<DataType>> typed_hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };

    template <typename DataType>
    class SemiProbeFunction : public ProbeFunctionBase {
     public:
      SemiProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                        std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : appender_list_(appender_list) {
        typed_hash_relation_ =
            std::dynamic_pointer_cast<TypedHashRelation<DataType>>(hash_relation);
      }
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          int index;
          if (key_array->IsNull(i)) {
            index = typed_hash_relation_->GetNull();
          } else {
            index = typed_hash_relation_->Get(typed_key_array->GetView(i));
          }
          if (index == -1) {
            continue;
          }
          for (auto appender : appender_list_) {
            if (appender->GetType() == AppenderBase::left) {
              THROW_NOT_OK(appender->AppendNull());
            } else {
              THROW_NOT_OK(appender->Append(0, i));
            }
          }
          out_length += 1;
        }
        return out_length;
      }

     private:
      using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
      std::shared_ptr<TypedHashRelation<DataType>> typed_hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };

    template <typename DataType>
    class ExistenceProbeFunction : public ProbeFunctionBase {
     public:
      ExistenceProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                             std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : appender_list_(appender_list) {
        typed_hash_relation_ =
            std::dynamic_pointer_cast<TypedHashRelation<DataType>>(hash_relation);
      }
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          int index;
          if (key_array->IsNull(i)) {
            index = typed_hash_relation_->GetNull();
          } else {
            index = typed_hash_relation_->Get(typed_key_array->GetView(i));
          }
          bool exists = true;
          if (index == -1) {
            exists = false;
          }
          for (auto appender : appender_list_) {
            if (appender->GetType() == AppenderBase::exist) {
              THROW_NOT_OK(appender->AppendExistence(exists));
            } else if (appender->GetType() == AppenderBase::right) {
              THROW_NOT_OK(appender->Append(0, i));
            } else {
              THROW_NOT_OK(appender->AppendNull());
            }
          }
          out_length += 1;
        }
        return out_length;
      }

     private:
      using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
      std::shared_ptr<TypedHashRelation<DataType>> typed_hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };

    arrow::compute::FunctionContext* ctx_;
    int join_type_;
    std::vector<int> right_key_index_list_;
    // used for hash key to hashMap probe
    int hash_map_type_ = 0;
    std::shared_ptr<gandiva::Projector> right_hash_key_project_;
    std::shared_ptr<gandiva::Projector> right_keys_project_;

    std::shared_ptr<arrow::DataType> key_type_;
    std::shared_ptr<HashRelation> hash_relation_;

    std::shared_ptr<arrow::Schema> result_schema_;
    std::vector<std::pair<int, int>> result_schema_index_list_;
    int exist_index_;
    std::vector<std::shared_ptr<AppenderBase>> appender_list_;

    gandiva::FieldVector left_field_list_;
    gandiva::FieldVector right_field_list_;
    gandiva::FieldVector right_projected_field_list_;
    std::shared_ptr<ProbeFunctionBase> probe_func_;
  };

  arrow::Status GetInnerJoin(const std::vector<std::string> input, bool cond_check,
                             std::string set_value, std::string index_name,
                             std::string hash_relation_name,
                             std::shared_ptr<CodeGenContext>* output) {
    std::stringstream shuffle_ss;
    std::stringstream codes_ss;
    std::stringstream finish_codes_ss;
    codes_ss << "int32_t " << index_name << ";" << std::endl;
    codes_ss << index_name << " = " << hash_relation_name << "->Get(key_"
             << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
             << std::endl;
    codes_ss << "if (" << index_name << " == -1) { continue; }" << std::endl;
    codes_ss << "for (auto tmp : " << hash_relation_name << "->GetItemListByIndex("
             << index_name << ")) {" << std::endl;
    if (cond_check) {
      auto condition_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
      codes_ss << "if (!" << condition_name << "(tmp, i)) {" << std::endl;
      codes_ss << "  continue;" << std::endl;
      codes_ss << "}" << std::endl;
      codes_ss << set_value << std::endl;
    } else {
      codes_ss << set_value << std::endl;
    }
    finish_codes_ss << "} // end of Inner Join" << std::endl;
    (*output)->process_codes += codes_ss.str();
    (*output)->finish_codes += finish_codes_ss.str();
    return arrow::Status::OK();
  }
  arrow::Status GetOuterJoin(const std::vector<std::string> input, bool cond_check,
                             std::string set_value, std::string index_name,
                             std::string hash_relation_name,
                             std::shared_ptr<CodeGenContext>* output) {
    std::stringstream codes_ss;
    std::stringstream finish_codes_ss;
    auto condition_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
    auto matched_index_list_name =
        "hash_relation_matched_" + std::to_string(hash_relation_id_);
    codes_ss << "int32_t " << index_name << ";" << std::endl;
    codes_ss << index_name << " = " << hash_relation_name << "->Get(key_"
             << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
             << std::endl;
    codes_ss << "std::vector<ArrayItemIndex> " << matched_index_list_name << ";"
             << std::endl;
    codes_ss << "if (" << index_name << " == -1) {" << std::endl;
    codes_ss << matched_index_list_name << " = {ArrayItemIndex(false)};" << std::endl;
    codes_ss << "} else {" << std::endl;
    codes_ss << matched_index_list_name << " = " << hash_relation_name
             << "->GetItemListByIndex(" << index_name << ");" << std::endl;
    codes_ss << "}" << std::endl;
    codes_ss << "for (auto tmp : " << matched_index_list_name << ") {" << std::endl;
    if (cond_check) {
      codes_ss << "if (!" << condition_name << "(tmp, i)) {" << std::endl;
      codes_ss << "  continue;" << std::endl;
      codes_ss << "}" << std::endl;
      codes_ss << set_value << std::endl;
    } else {
      codes_ss << set_value << std::endl;
    }
    finish_codes_ss << "} // end of Outer Join" << std::endl;
    (*output)->process_codes += codes_ss.str();
    (*output)->finish_codes += finish_codes_ss.str();
    return arrow::Status::OK();
  }
  arrow::Status GetAntiJoin(const std::vector<std::string> input, bool cond_check,
                            std::string set_value, std::string index_name,
                            std::string hash_relation_name,
                            std::shared_ptr<CodeGenContext>* output) {
    std::stringstream codes_ss;
    std::stringstream finish_codes_ss;
    auto condition_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
    auto matched_index_list_name =
        "hash_relation_matched_" + std::to_string(hash_relation_id_);
    codes_ss << "int32_t " << index_name << ";" << std::endl;
    codes_ss << index_name << " = " << hash_relation_name << "->Get(key_"
             << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
             << std::endl;
    codes_ss << "std::vector<ArrayItemIndex> " << matched_index_list_name << ";"
             << std::endl;
    codes_ss << "if (" << index_name << " == -1) {" << std::endl;
    codes_ss << matched_index_list_name << " = {ArrayItemIndex(false)};" << std::endl;
    if (cond_check) {
      codes_ss << "} else {" << std::endl;
      codes_ss << "  bool found = false;" << std::endl;
      codes_ss << "  for (auto tmp : " << hash_relation_name << "->GetItemListByIndex("
               << index_name << ")"
               << ") {" << std::endl;
      codes_ss << "    if (" << condition_name << "(tmp, i)) {" << std::endl;
      codes_ss << "      found = true;" << std::endl;
      codes_ss << "      break;" << std::endl;
      codes_ss << "    }" << std::endl;
      codes_ss << "  }" << std::endl;
      codes_ss << "  if (!found) {" << std::endl;
      codes_ss << matched_index_list_name << " = {ArrayItemIndex(false)};" << std::endl;
      codes_ss << "  }" << std::endl;
    }
    codes_ss << "}" << std::endl;
    codes_ss << "for (auto tmp : " << matched_index_list_name << ") {" << std::endl;
    codes_ss << set_value << std::endl;
    finish_codes_ss << "} // end of Anti Join" << std::endl;
    (*output)->process_codes += codes_ss.str();
    (*output)->finish_codes += finish_codes_ss.str();
    return arrow::Status::OK();
  }
  arrow::Status GetSemiJoin(const std::vector<std::string> input, bool cond_check,
                            std::string set_value, std::string index_name,
                            std::string hash_relation_name,
                            std::shared_ptr<CodeGenContext>* output) {
    std::stringstream shuffle_ss;
    std::stringstream codes_ss;
    std::stringstream finish_codes_ss;
    auto condition_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
    auto matched_index_list_name =
        "hash_relation_matched_" + std::to_string(hash_relation_id_);
    codes_ss << "int32_t " << index_name << ";" << std::endl;
    codes_ss << index_name << " = " << hash_relation_name << "->Get(key_"
             << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
             << std::endl;
    codes_ss << "std::vector<ArrayItemIndex> " << matched_index_list_name << ";"
             << std::endl;
    codes_ss << "if (" << index_name << " == -1) {" << std::endl;
    codes_ss << "continue;" << std::endl;
    if (cond_check) {
      codes_ss << "} else {" << std::endl;
      codes_ss << "  bool found = false;" << std::endl;
      codes_ss << "  for (auto tmp : " << hash_relation_name << "->GetItemListByIndex("
               << index_name << ")"
               << ") {" << std::endl;
      codes_ss << "    if (" << condition_name << "(tmp, i)) {" << std::endl;
      codes_ss << "      found = true;" << std::endl;
      codes_ss << "      break;" << std::endl;
      codes_ss << "    }" << std::endl;
      codes_ss << "  }" << std::endl;
      codes_ss << "  if (found) {" << std::endl;
      codes_ss << matched_index_list_name << " = {ArrayItemIndex(false)};" << std::endl;
      codes_ss << "  }" << std::endl;
    } else {
      codes_ss << "} else {" << std::endl;
      codes_ss << matched_index_list_name << " = {ArrayItemIndex(false)};" << std::endl;
    }
    codes_ss << "}" << std::endl;
    codes_ss << "for (auto tmp : " << matched_index_list_name << ") {" << std::endl;
    codes_ss << set_value << std::endl;
    finish_codes_ss << "} // end of Semi Join" << std::endl;
    (*output)->process_codes += codes_ss.str();
    (*output)->finish_codes += finish_codes_ss.str();
    return arrow::Status::OK();
  }
  arrow::Status GetExistenceJoin(const std::vector<std::string> input, bool cond_check,
                                 std::string set_value, std::string index_name,
                                 std::string hash_relation_name,
                                 std::shared_ptr<CodeGenContext>* output) {
    std::stringstream shuffle_ss;
    std::stringstream codes_ss;
    std::stringstream finish_codes_ss;
    auto condition_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
    auto matched_index_list_name =
        "hash_relation_matched_" + std::to_string(hash_relation_id_);
    auto exist_name =
        "hash_relation_" + std::to_string(hash_relation_id_) + "_existence_value";
    auto exist_validity = exist_name + "_validity";
    codes_ss << "int32_t " << index_name << ";" << std::endl;
    codes_ss << index_name << " = " << hash_relation_name << "->Get(key_"
             << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
             << std::endl;
    codes_ss << "bool " << exist_name << " = false;" << std::endl;
    codes_ss << "bool " << exist_validity << " = true;" << std::endl;
    codes_ss << "if (" << index_name << " == -1) {" << std::endl;
    codes_ss << exist_name << " = false;" << std::endl;
    if (cond_check) {
      codes_ss << "} else {" << std::endl;
      codes_ss << "  for (auto tmp : " << hash_relation_name << "->GetItemListByIndex("
               << index_name << ")"
               << ") {" << std::endl;
      codes_ss << "    if (" << condition_name << "(tmp, i)) {" << std::endl;
      codes_ss << "      " << exist_name << " = true;" << std::endl;
      codes_ss << "      break;" << std::endl;
      codes_ss << "    }" << std::endl;
      codes_ss << "  }" << std::endl;
    } else {
      codes_ss << "} else {" << std::endl;
      codes_ss << exist_name << " = true;" << std::endl;
    }
    codes_ss << "}" << std::endl;
    codes_ss << "std::vector<ArrayItemIndex> " << matched_index_list_name
             << " = {ArrayItemIndex(false)};" << std::endl;
    codes_ss << "for (auto tmp : " << matched_index_list_name << ") {" << std::endl;
    codes_ss << set_value << std::endl;
    finish_codes_ss << "} // end of Existence Join" << std::endl;
    (*output)->process_codes += codes_ss.str();
    (*output)->finish_codes += finish_codes_ss.str();
    return arrow::Status::OK();
  }
  arrow::Status GetProcessProbe(const std::vector<std::string> input, int join_type,
                                bool cond_check,
                                std::shared_ptr<CodeGenContext>* output) {
    auto hash_relation_name =
        "hash_relation_list_" + std::to_string(hash_relation_id_) + "_";
    auto index_name = "hash_relation_" + std::to_string(hash_relation_id_) + "_index";
    std::vector<std::vector<std::string>> output_name_list = {{}, {}};
    std::stringstream valid_ss;
    for (int i = 0; i < left_field_list_.size(); i++) {
      auto type = left_field_list_[i]->type();
      auto name =
          "hash_relation_" + std::to_string(hash_relation_id_) + "_" + std::to_string(i);
      auto output_name = name + "_value";
      auto output_validity = output_name + "_validity";
      valid_ss << "auto " << output_validity << " = tmp.valid ? !" << name
               << "->IsNull(tmp.array_id, tmp.id) : false;" << std::endl;
      valid_ss << GetCTypeString(type) << " " << output_name << ";" << std::endl;
      valid_ss << "if (" << output_validity << ") {" << std::endl;
      valid_ss << output_name << " = " << name << "->GetValue(tmp.array_id, tmp.id);"
               << std::endl;
      valid_ss << "}" << std::endl;

      output_name_list[0].push_back(output_name);
    }
    for (int i = 0; i < right_field_list_.size(); i++) {
      if (exist_index_ != -1 && exist_index_ == i) {
        auto exist_name =
            "hash_relation_" + std::to_string(hash_relation_id_) + "_existence_value";
        output_name_list[1].push_back(exist_name);
      }
      output_name_list[1].push_back(input[i]);
    }
    if (exist_index_ != -1 && exist_index_ == right_field_list_.size()) {
      auto exist_name =
          "hash_relation_" + std::to_string(hash_relation_id_) + "_existence_value";
      output_name_list[1].push_back(exist_name);
    }

    int output_idx = 0;
    std::stringstream ss;
    for (auto pair : result_schema_index_list_) {
      // set result to output list
      auto name = (*output)->output_list[output_idx++].first;
      ss << name << " = " << output_name_list[pair.first][pair.second] << ";"
         << std::endl;
      ss << name << "_validity = " << output_name_list[pair.first][pair.second]
         << "_validity;" << std::endl;
    }
    valid_ss << ss.str();

    switch (join_type) {
      case 0: { /*Inner Join*/
        return GetInnerJoin(input, cond_check, valid_ss.str(), index_name,
                            hash_relation_name, output);
      } break;
      case 1: { /*Outer Join*/
        return GetOuterJoin(input, cond_check, valid_ss.str(), index_name,
                            hash_relation_name, output);
      } break;
      case 2: { /*Anti Join*/
        return GetAntiJoin(input, cond_check, valid_ss.str(), index_name,
                           hash_relation_name, output);
      } break;
      case 3: { /*Semi Join*/
        return GetSemiJoin(input, cond_check, valid_ss.str(), index_name,
                           hash_relation_name, output);
      } break;
      case 4: { /*Existence Join*/
        return GetExistenceJoin(input, cond_check, valid_ss.str(), index_name,
                                hash_relation_name, output);
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ConditionedProbeArraysTypedImpl only support join type: InnerJoin, "
            "RightJoin");
    }
    return arrow::Status::OK();
  }
};  // namespace extra

arrow::Status ConditionedProbeKernel::Make(
    arrow::compute::FunctionContext* ctx, const gandiva::NodeVector& left_key_list,
    const gandiva::NodeVector& right_key_list,
    const gandiva::NodeVector& left_schema_list,
    const gandiva::NodeVector& right_schema_list, const gandiva::NodePtr& condition,
    int join_type, const gandiva::NodeVector& result_schema,
    const gandiva::NodeVector& hash_configuration_list, int hash_relation_idx,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ConditionedProbeKernel>(
      ctx, left_key_list, right_key_list, left_schema_list, right_schema_list, condition,
      join_type, result_schema, hash_configuration_list, hash_relation_idx);
  return arrow::Status::OK();
}

ConditionedProbeKernel::ConditionedProbeKernel(
    arrow::compute::FunctionContext* ctx, const gandiva::NodeVector& left_key_list,
    const gandiva::NodeVector& right_key_list,
    const gandiva::NodeVector& left_schema_list,
    const gandiva::NodeVector& right_schema_list, const gandiva::NodePtr& condition,
    int join_type, const gandiva::NodeVector& result_schema,
    const gandiva::NodeVector& hash_configuration_list, int hash_relation_idx) {
  impl_.reset(new Impl(ctx, left_key_list, right_key_list, left_schema_list,
                       right_schema_list, condition, join_type, result_schema,
                       hash_configuration_list, hash_relation_idx));
  kernel_name_ = "ConditionedProbeKernel";
}

arrow::Status ConditionedProbeKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

std::string ConditionedProbeKernel::GetSignature() { return impl_->GetSignature(); }

arrow::Status ConditionedProbeKernel::DoCodeGen(
    int level, std::vector<std::string> input,
    std::shared_ptr<CodeGenContext>* codegen_ctx_out, int* var_id) {
  return impl_->DoCodeGen(level, input, codegen_ctx_out, var_id);
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin