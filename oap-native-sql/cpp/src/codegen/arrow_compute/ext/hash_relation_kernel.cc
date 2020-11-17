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
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <unordered_map>

#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/typed_node_visitor.h"
#include "codegen/common/hash_relation_number.h"
#include "codegen/common/hash_relation_string.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  WholeStageCodeGen  ////////////////
class HashRelationKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
       std::shared_ptr<gandiva::Node> root_node,
       const std::vector<std::shared_ptr<arrow::Field>>& output_field_list)
      : ctx_(ctx), input_field_list_(input_field_list) {
    std::vector<std::shared_ptr<HashRelationColumn>> hash_relation_list;
    for (auto field : input_field_list) {
      std::shared_ptr<HashRelationColumn> hash_relation_column;
      THROW_NOT_OK(MakeHashRelationColumn(field->type()->id(), &hash_relation_column));
      hash_relation_list.push_back(hash_relation_column);
    }

    bool need_project = true;
    std::vector<gandiva::FieldPtr> key_fields;
    auto children =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(root_node)->children();
    auto key_nodes =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[0])->children();

    if (children.size() > 1) {
      auto parameter_nodes =
          std::dynamic_pointer_cast<gandiva::FunctionNode>(children[1])->children();
      auto builder_type_str = gandiva::ToString(
          std::dynamic_pointer_cast<gandiva::LiteralNode>(parameter_nodes[0])->holder());
      builder_type_ = std::stoi(builder_type_str);
    }
    if (builder_type_ == 0) {
      if (key_nodes.size() == 1) {
        auto key_node = key_nodes[0];
        std::shared_ptr<TypedNodeVisitor> node_visitor;
        THROW_NOT_OK(MakeTypedNodeVisitor(key_node, &node_visitor));
        if (node_visitor->GetResultType() == TypedNodeVisitor::FieldNode) {
          std::shared_ptr<gandiva::FieldNode> field_node;
          node_visitor->GetTypedNode(&field_node);
          key_fields.push_back(field_node->field());
          need_project = false;
        }
      }
      if (!need_project) {
        THROW_NOT_OK(GetIndexList(key_fields, input_field_list, &key_indices_));
        THROW_NOT_OK(MakeHashRelation(key_fields[0]->type()->id(), ctx_,
                                      hash_relation_list, &hash_relation_));
      } else {
        gandiva::ExpressionPtr project_expr;
        project_expr = GetConcatedKernel(key_nodes);
        auto schema = arrow::schema(input_field_list);
        auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
        THROW_NOT_OK(gandiva::Projector::Make(schema, {project_expr}, configuration,
                                              &key_projector_));
        THROW_NOT_OK(MakeHashRelation(project_expr->result()->type()->id(), ctx_,
                                      hash_relation_list, &hash_relation_));
      }
    } else if (builder_type_ == 1) {
      // we will use unsafe_row and new unsafe_hash_map
      gandiva::ExpressionVector key_project_expr = GetGandivaKernel(key_nodes);
      gandiva::ExpressionPtr key_hash_expr = GetHash32Kernel(key_nodes);

      auto schema = arrow::schema(input_field_list);
      auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
      THROW_NOT_OK(gandiva::Projector::Make(schema, key_project_expr, configuration,
                                            &key_prepare_projector_));
      gandiva::FieldVector key_hash_field_list;
      for (auto expr : key_project_expr) {
        key_hash_field_list.push_back(expr->result());
      }
      hash_input_schema_ = arrow::schema(key_hash_field_list);
      THROW_NOT_OK(gandiva::Projector::Make(hash_input_schema_, {key_hash_expr},
                                            configuration, &key_projector_));
      if (key_hash_field_list.size() == 1 &&
          key_hash_field_list[0]->type()->id() != arrow::Type::STRING) {
        // If single key case, we can put key in KeyArray
        auto key_type = std::dynamic_pointer_cast<arrow::FixedWidthType>(
            key_hash_field_list[0]->type());
        auto key_size = key_type->bit_width() / 8;
        hash_relation_ =
            std::make_shared<HashRelation>(ctx_, hash_relation_list, key_size);
      } else {
        hash_relation_ = std::make_shared<HashRelation>(ctx_, hash_relation_list);
      }
    } else {
      hash_relation_ = std::make_shared<HashRelation>(hash_relation_list);
    }
  }

  arrow::Status Evaluate(const ArrayList& in) {
    for (int i = 0; i < in.size(); i++) {
      RETURN_NOT_OK(hash_relation_->AppendPayloadColumn(i, in[i]));
    }
    if (builder_type_ == 2) return arrow::Status::OK();
    std::shared_ptr<arrow::Array> key_array;
    if (builder_type_ == 0) {
      if (key_projector_) {
        arrow::ArrayVector outputs;
        auto length = in.size() > 0 ? in[0]->length() : 0;
        auto in_batch =
            arrow::RecordBatch::Make(arrow::schema(input_field_list_), length, in);
        RETURN_NOT_OK(key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
        key_array = outputs[0];
      } else {
        key_array = in[key_indices_[0]];
      }
      return hash_relation_->AppendKeyColumn(key_array);
    } else {
      /* Process original key projection */
      arrow::ArrayVector project_outputs;
      auto length = in.size() > 0 ? in[0]->length() : 0;
      auto in_batch =
          arrow::RecordBatch::Make(arrow::schema(input_field_list_), length, in);
      RETURN_NOT_OK(key_prepare_projector_->Evaluate(*in_batch, ctx_->memory_pool(),
                                                     &project_outputs));

      /* Process key Hash projection */
      arrow::ArrayVector hash_outputs;
      auto hash_in_batch =
          arrow::RecordBatch::Make(hash_input_schema_, length, project_outputs);
      RETURN_NOT_OK(
          key_projector_->Evaluate(*hash_in_batch, ctx_->memory_pool(), &hash_outputs));
      key_array = hash_outputs[0];

/* For single field fixed_size key, we simply insert to HashMap without append to unsafe
 * Row */
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
  PROCESS(arrow::StringType)
      if (project_outputs.size() == 1 &&
          project_outputs[0]->type_id() != arrow::Type::STRING) {
        switch (project_outputs[0]->type_id()) {
#define PROCESS(InType)                                                   \
  case TypeTraits<InType>::type_id: {                                     \
    using ArrayType = precompile::TypeTraits<InType>::ArrayType;          \
    auto typed_key_arr = std::make_shared<ArrayType>(project_outputs[0]); \
    return hash_relation_->AppendKeyColumn(key_array, typed_key_arr);     \
  } break;
          PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
          default: {
            return arrow::Status::NotImplemented(
                "HashRelation Evaluate doesn't support single key type ",
                project_outputs[0]->type_id());
          } break;
        }
#undef PROCESS_SUPPORTED_TYPES
      } else {
        /* Append key array to UnsafeArray for later UnsafeRow projection */
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        for (auto arr : project_outputs) {
          std::shared_ptr<UnsafeArray> payload;
          RETURN_NOT_OK(MakeUnsafeArray(arr->type(), i++, arr, &payload));
          payloads.push_back(payload);
        }
        return hash_relation_->AppendKeyColumn(key_array, payloads);
      }
    }
    return arrow::Status::OK();
  }

  std::string GetSignature() { return ""; }
  arrow::Status MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                   std::shared_ptr<ResultIterator<HashRelation>>* out) {
    *out = std::make_shared<HashRelationResultIterator>(hash_relation_);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  arrow::MemoryPool* pool_;
  std::vector<std::shared_ptr<arrow::Field>> input_field_list_;
  std::vector<std::shared_ptr<arrow::Field>> output_field_list_;
  std::vector<int> key_indices_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  std::shared_ptr<gandiva::Projector> key_prepare_projector_;
  std::shared_ptr<arrow::Schema> hash_input_schema_;
  std::shared_ptr<HashRelation> hash_relation_;
  int builder_type_ = 0;

  class HashRelationResultIterator : public ResultIterator<HashRelation> {
   public:
    HashRelationResultIterator(std::shared_ptr<HashRelation> hash_relation)
        : hash_relation_(hash_relation) {}

    arrow::Status Next(std::shared_ptr<HashRelation>* out) override {
      *out = hash_relation_;
      return arrow::Status::OK();
    }

    arrow::Status ProcessAndCacheOne(
        const std::vector<std::shared_ptr<arrow::Array>>& in,
        const std::shared_ptr<arrow::Array>& selection = nullptr) override {
      for (int i = 0; i < in.size(); i++) {
        RETURN_NOT_OK(hash_relation_->AppendPayloadColumn(i, in[i]));
      }
      return arrow::Status::OK();
    }

   private:
    std::shared_ptr<HashRelation> hash_relation_;
  };
};  // namespace extra

arrow::Status HashRelationKernel::Make(
    arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
    std::shared_ptr<gandiva::Node> root_node,
    const std::vector<std::shared_ptr<arrow::Field>>& output_field_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<HashRelationKernel>(ctx, input_field_list, root_node,
                                              output_field_list);
  return arrow::Status::OK();
}

HashRelationKernel::HashRelationKernel(
    arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
    std::shared_ptr<gandiva::Node> root_node,
    const std::vector<std::shared_ptr<arrow::Field>>& output_field_list) {
  impl_.reset(new Impl(ctx, input_field_list, root_node, output_field_list));
  kernel_name_ = "HashRelationKernel";
}

arrow::Status HashRelationKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status HashRelationKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<HashRelation>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

std::string HashRelationKernel::GetSignature() { return impl_->GetSignature(); }

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin