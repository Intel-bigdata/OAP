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
#include "codegen/common/hash_relation.h"
#include "utils/macros.h"
//#include "codegen/arrow_compute/ext/codegen_node_visitor.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  WholeStageCodeGen  ////////////////
class WholeStageCodeGenKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
       std::shared_ptr<gandiva::Node> root_node,
       const std::vector<std::shared_ptr<arrow::Field>>& output_field_list)
      : ctx_(ctx) {
    int hash_relation_idx = 0;
    THROW_NOT_OK(ParseNodeTree(root_node, &hash_relation_idx, &kernel_list_));
    THROW_NOT_OK(LoadJITFunction(input_field_list, output_field_list, kernel_list_,
                                 &wscg_kernel_));
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return wscg_kernel_->MakeResultIterator(schema, out);
  }

  std::string GetSignature() { return signature_; }

 private:
  arrow::compute::FunctionContext* ctx_;
  arrow::MemoryPool* pool_;
  std::vector<std::shared_ptr<KernalBase>> kernel_list_;
  std::shared_ptr<CodeGenBase> wscg_kernel_;
  std::string signature_;

  arrow::Status GetArguments(std::shared_ptr<gandiva::Node> node, int i,
                             gandiva::NodeVector* node_list) {
    auto function_node = std::dynamic_pointer_cast<gandiva::FunctionNode>(node);
    auto arg_node =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(function_node->children()[i]);
    *node_list = arg_node->children();
    return arrow::Status::OK();
  }

  arrow::Status CreateKernelByName(std::shared_ptr<gandiva::Node> node,
                                   int* hash_relation_idx,
                                   std::shared_ptr<KernalBase>* out) {
    auto function_node = std::dynamic_pointer_cast<gandiva::FunctionNode>(node);
    auto func_name = function_node->descriptor()->name();
    if (func_name.compare(0, 22, "conditionedProbeArrays") == 0) {
      int join_type;
      gandiva::NodeVector left_schema_list;
      RETURN_NOT_OK(GetArguments(function_node, 0, &left_schema_list));
      gandiva::NodeVector right_schema_list;
      RETURN_NOT_OK(GetArguments(function_node, 1, &right_schema_list));
      gandiva::NodeVector left_key_list;
      RETURN_NOT_OK(GetArguments(function_node, 2, &left_key_list));
      gandiva::NodeVector right_key_list;
      RETURN_NOT_OK(GetArguments(function_node, 3, &right_key_list));
      gandiva::NodeVector result_list;
      RETURN_NOT_OK(GetArguments(function_node, 4, &result_list));
      gandiva::NodeVector configuration_list;
      RETURN_NOT_OK(GetArguments(function_node, 5, &configuration_list));
      gandiva::NodePtr condition;
      if (function_node->children().size() > 6) {
        condition = function_node->children()[6];
      }

      if (func_name.compare("conditionedProbeArraysInner") == 0) {
        join_type = 0;
      } else if (func_name.compare("conditionedProbeArraysOuter") == 0) {
        join_type = 1;
      } else if (func_name.compare("conditionedProbeArraysAnti") == 0) {
        join_type = 2;
      } else if (func_name.compare("conditionedProbeArraysSemi") == 0) {
        join_type = 3;
      } else if (func_name.compare("conditionedProbeArraysExistence") == 0) {
        join_type = 4;
      }
      int cur_hash_relation_idx = *hash_relation_idx;
      *hash_relation_idx += 1;
      RETURN_NOT_OK(ConditionedProbeKernel::Make(
          ctx_, left_key_list, right_key_list, left_schema_list, right_schema_list,
          condition, join_type, result_list, configuration_list, cur_hash_relation_idx,
          out));

    } else if (func_name.compare("project") == 0) {
      auto project_expression_list =
          std::dynamic_pointer_cast<gandiva::FunctionNode>(function_node->children()[1])
              ->children();
      auto field_node_list =
          std::dynamic_pointer_cast<gandiva::FunctionNode>(function_node->children()[0])
              ->children();
      RETURN_NOT_OK(
          ProjectKernel::Make(ctx_, field_node_list, project_expression_list, out));
    } else if (func_name.compare("filter") == 0) {
      auto field_node_list =
          std::dynamic_pointer_cast<gandiva::FunctionNode>(function_node->children()[0])
              ->children();
      RETURN_NOT_OK(
          FilterKernel::Make(ctx_, field_node_list, function_node->children()[1], out));
    }
    return arrow::Status::OK();
  }

  /* *
   * Expecting insert node is a function node whose function name is "child", and real
   * function is its first child, if who has two children, second one is the next child.
   * */
  arrow::Status ParseNodeTree(std::shared_ptr<gandiva::Node> root_node,
                              int* hash_relation_index,
                              std::vector<std::shared_ptr<KernalBase>>* kernel_list_) {
    auto function_node = std::dynamic_pointer_cast<gandiva::FunctionNode>(root_node);
    if (function_node->descriptor()->name() != "child") {
      return arrow::Status::NotImplemented(
          "WholeStageCodeGenResultIterator expect child keyword.");
    }
    auto children = function_node->children();
    if (children.size() > 1) {
      ParseNodeTree(children[1], hash_relation_index, kernel_list_);
    }
    std::shared_ptr<KernalBase> kernel;
    RETURN_NOT_OK(CreateKernelByName(children[0], hash_relation_index, &kernel));
    (*kernel_list_).push_back(kernel);
    return arrow::Status::OK();
  }

  arrow::Status LoadJITFunction(
      const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
      const std::vector<std::shared_ptr<arrow::Field>>& output_field_list,
      const std::vector<std::shared_ptr<KernalBase>>& kernel_list,
      std::shared_ptr<CodeGenBase>* out) {
    int argument_id = 0;
    int level = 0;
    std::vector<std::shared_ptr<CodeGenContext>> codegen_ctx_list;
    std::vector<std::string> input_list;
    for (int i = 0; i < input_field_list.size(); i++) {
      auto name = "typed_in_col_" + std::to_string(i);
      input_list.push_back(name);
    }
    for (auto kernel : kernel_list) {
      std::shared_ptr<CodeGenContext> child_codegen_ctx;
      RETURN_NOT_OK(
          kernel->DoCodeGen(level++, input_list, &child_codegen_ctx, &argument_id));
      codegen_ctx_list.push_back(child_codegen_ctx);
      input_list.clear();
      for (auto pair : child_codegen_ctx->output_list) {
        input_list.push_back(pair.first);
      }
    }
    std::string codes;
    RETURN_NOT_OK(
        DoCodeGen(input_field_list, output_field_list, codegen_ctx_list, &codes));
    // generate dll signature
    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(codes);
    signature_ = signature_ss.str();
    auto file_lock = FileSpinLock();
    auto status = LoadLibrary(signature_, ctx_, out);

    if (!status.ok()) {
      // process
      try {
        // compile codes
        RETURN_NOT_OK(CompileCodes(codes, signature_));
        RETURN_NOT_OK(LoadLibrary(signature_, ctx_, out));
      } catch (const std::runtime_error& error) {
        FileSpinUnLock(file_lock);
        throw error;
      }
    }
    FileSpinUnLock(file_lock);
    return arrow::Status::OK();
  }

  arrow::Status DoCodeGen(
      const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
      const std::vector<std::shared_ptr<arrow::Field>>& output_field_list,
      const std::vector<std::shared_ptr<CodeGenContext>>& codegen_ctx_list,
      std::string* codes) {
    std::stringstream codes_ss;
    std::string out_list;
    std::stringstream define_ss;
    codes_ss << BaseCodes() << std::endl;
    codes_ss << R"(#include "precompile/builder.h")" << std::endl;
    std::vector<std::string> headers;
    for (auto codegen_ctx : codegen_ctx_list) {
      for (auto header : codegen_ctx->header_codes) {
        if (std::find(headers.begin(), headers.end(), header) == headers.end()) {
          headers.push_back(header);
        }
      }
    }
    for (auto header : headers) {
      codes_ss << header << std::endl;
    }

    codes_ss << R"(
using namespace sparkcolumnarplugin::precompile;
class TypedWholeStageCodeGenImpl : public CodeGenBase {
 public:
  TypedWholeStageCodeGenImpl(arrow::compute::FunctionContext *ctx) : ctx_(ctx) {}
  ~TypedWholeStageCodeGenImpl() {}
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>> *out) override {
    *out = std::make_shared<WholeStageCodeGenResultIterator>(ctx_, schema);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  class WholeStageCodeGenResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    WholeStageCodeGenResultIterator(arrow::compute::FunctionContext* ctx,
                                    const std::shared_ptr<arrow::Schema>& result_schema)
        : ctx_(ctx), result_schema_(result_schema) {)";
    codes_ss << GetBuilderInitializeCodes(output_field_list) << std::endl;
    codes_ss << "}" << std::endl;
    codes_ss << R"(
    arrow::Status SetDependencies(
        const std::vector<std::shared_ptr<ResultIteratorBase>>& dependent_iter_list) {
      std::vector<std::shared_ptr<ResultIterator<HashRelation>>> typed_dependent_iter_list;
      for (auto iter : dependent_iter_list) {
        typed_dependent_iter_list.push_back(
            std::dynamic_pointer_cast<ResultIterator<HashRelation>>(iter));
      }
      hash_relation_list_.resize(dependent_iter_list.size());
      )";
    for (auto codegen_ctx : codegen_ctx_list) {
      codes_ss << codegen_ctx->hash_relation_prepare_codes << std::endl;
    }
    codes_ss << R"(
      return arrow::Status::OK();
    }

    arrow::Status Process(const std::vector<std::shared_ptr<arrow::Array>>& in,
                          std::shared_ptr<arrow::RecordBatch>* out,
                          const std::shared_ptr<arrow::Array>& selection = nullptr)
        override {)"
             << std::endl;

    // convert input data to typed array
    for (int i = 0; i < input_field_list.size(); i++) {
      auto typed_array_name = "typed_in_" + std::to_string(i);
      codes_ss << "auto " << typed_array_name << " = std::make_shared<"
               << GetTypeString(input_field_list[i]->type(), "Array") << ">(in[" << i
               << "]);";
    }

    codes_ss << R"(
          uint64_t out_length = 0;
          auto length = typed_in_0->length();
          for (int i = 0; i < length; i++) {
    )" << std::endl;
    // input preparation
    for (int i = 0; i < input_field_list.size(); i++) {
      auto typed_array_name = "typed_in_" + std::to_string(i);
      auto name = "typed_in_col_" + std::to_string(i);
      auto validity = name + "_validity";
      if (input_field_list[i]->type()->id() == arrow::Type::STRING) {
        define_ss << "bool " << validity << ";" << std::endl;
        define_ss << GetCTypeString(input_field_list[i]->type()) << " " << name << ";"
                  << std::endl;
        codes_ss << validity << " = " << typed_array_name << "->IsNull(i) ? false : true;"
                 << std::endl;
        codes_ss << "if (" << validity << ") {" << std::endl;
        codes_ss << name << " = " << typed_array_name << "->GetString(i);" << std::endl;
        codes_ss << "}" << std::endl;

      } else {
        define_ss << "bool " << validity << ";" << std::endl;
        define_ss << GetCTypeString(input_field_list[i]->type()) << " " << name << ";"
                  << std::endl;
        codes_ss << validity << " = " << typed_array_name << "->IsNull(i) ? false : true;"
                 << std::endl;
        codes_ss << "if (" << validity << ") {" << std::endl;

        codes_ss << name << " = " << typed_array_name << "->GetView(i);" << std::endl;
        codes_ss << "}" << std::endl;
      }
    }
    // paste children's codegen
    for (auto codegen_ctx : codegen_ctx_list) {
      codes_ss << codegen_ctx->prepare_codes << std::endl;
      codes_ss << codegen_ctx->process_codes << std::endl;
    }

    codes_ss << GetProcessMaterializeCodes(codegen_ctx_list.back()) << std::endl;
    codes_ss << "out_length += 1;" << std::endl;
    for (auto codegen_ctx : codegen_ctx_list) {
      codes_ss << codegen_ctx->finish_codes << std::endl;
    }
    codes_ss << "} // end of for loop" << std::endl;
    codes_ss << GetProcessFinishCodes(output_field_list) << std::endl;
    codes_ss << R"(
      *out = arrow::RecordBatch::Make(result_schema_, out_length, {)" +
                    GetProcessOutListCodes(output_field_list) + R"(});
      return arrow::Status::OK();
    }

   private:
    arrow::compute::FunctionContext* ctx_;
    std::shared_ptr<arrow::Schema> result_schema_;
    std::vector<std::shared_ptr<HashRelation>> hash_relation_list_;)"
             << std::endl;
    codes_ss << define_ss.str();
    for (auto codegen_ctx : codegen_ctx_list) {
      codes_ss << codegen_ctx->definition_codes << std::endl;
    }
    codes_ss << GetBuilderDefinitionCodes(output_field_list) << std::endl;
    for (auto codegen_ctx : codegen_ctx_list) {
      for (auto func_codes : codegen_ctx->function_list) {
        codes_ss << func_codes << std::endl;
      }
    }

    codes_ss << "};" << std::endl;
    codes_ss << "};" << std::endl;
    codes_ss << R"(
extern "C" void MakeCodeGen(arrow::compute::FunctionContext *ctx,
                            std::shared_ptr<CodeGenBase> *out) {
  *out = std::make_shared<TypedWholeStageCodeGenImpl>(ctx);
})";

    *codes = codes_ss.str();
    return arrow::Status::OK();
  }

  std::string GetProcessMaterializeCodes(std::shared_ptr<CodeGenContext> codegen_ctx) {
    std::stringstream codes_ss;
    int i = 0;
    for (auto pair : codegen_ctx->output_list) {
      auto name = pair.first;
      auto type = pair.second;
      auto validity = name + "_validity";
      codes_ss << "if (" << validity << ") {" << std::endl;
      if (type->id() == arrow::Type::STRING) {
        codes_ss << "  RETURN_NOT_OK(builder_" << i << "_->AppendString(" << name << "));"
                 << std::endl;
      } else {
        codes_ss << "  RETURN_NOT_OK(builder_" << i << "_->Append(" << name << "));"
                 << std::endl;
      }
      codes_ss << "} else {" << std::endl;
      codes_ss << "  RETURN_NOT_OK(builder_" << i << "_->AppendNull());" << std::endl;
      codes_ss << "}" << std::endl;
      i++;
    }
    return codes_ss.str();
  }

  std::string GetProcessFinishCodes(gandiva::FieldVector output_field_list) {
    std::stringstream codes_ss;
    for (int i = 0; i < output_field_list.size(); i++) {
      auto data_type = output_field_list[i]->type();
      codes_ss << "std::shared_ptr<arrow::Array> out_" << i << ";" << std::endl;
      codes_ss << "RETURN_NOT_OK(builder_" << i << "_->Finish(&out_" << i << "));"
               << std::endl;
      codes_ss << "builder_" << i << "_->Reset();" << std::endl;
    }
    return codes_ss.str();
  }

  std::string GetProcessOutListCodes(gandiva::FieldVector output_field_list) {
    std::vector<std::string> output_list;
    for (int i = 0; i < output_field_list.size(); i++) {
      output_list.push_back("out_" + std::to_string(i));
    }
    std::stringstream codes_ss;
    codes_ss << GetParameterList(output_list, false);
    return codes_ss.str();
  }

  std::string GetBuilderInitializeCodes(gandiva::FieldVector output_field_list) {
    std::stringstream codes_ss;
    for (int i = 0; i < output_field_list.size(); i++) {
      auto data_type = output_field_list[i]->type();
      if (data_type->id() == arrow::Type::DECIMAL) {
        codes_ss << "builder_" << i << "_ = std::make_shared<"
                 << GetTypeString(data_type, "Builder")
                 << ">(arrow::" << GetArrowTypeDefString(data_type)
                 << ", ctx_->memory_pool());" << std::endl;
      } else {
        codes_ss << "builder_" << i << "_ = std::make_shared<"
                 << GetTypeString(data_type, "Builder") << ">(ctx_->memory_pool());"
                 << std::endl;
      }
    }
    return codes_ss.str();
  }

  std::string GetBuilderDefinitionCodes(gandiva::FieldVector output_field_list) {
    std::stringstream codes_ss;
    for (int i = 0; i < output_field_list.size(); i++) {
      auto data_type = output_field_list[i]->type();
      codes_ss << "std::shared_ptr<" << GetTypeString(data_type, "Builder")
               << "> builder_" << i << "_;" << std::endl;
    }
    return codes_ss.str();
  }
};

arrow::Status WholeStageCodeGenKernel::Make(
    arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
    std::shared_ptr<gandiva::Node> root_node,
    const std::vector<std::shared_ptr<arrow::Field>>& output_field_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<WholeStageCodeGenKernel>(ctx, input_field_list, root_node,
                                                   output_field_list);
  return arrow::Status::OK();
}

WholeStageCodeGenKernel::WholeStageCodeGenKernel(
    arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<arrow::Field>>& input_field_list,
    std::shared_ptr<gandiva::Node> root_node,
    const std::vector<std::shared_ptr<arrow::Field>>& output_field_list) {
  impl_.reset(new Impl(ctx, input_field_list, root_node, output_field_list));
  kernel_name_ = "WholeStageCodeGenKernel";
}

arrow::Status WholeStageCodeGenKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

std::string WholeStageCodeGenKernel::GetSignature() { return impl_->GetSignature(); }

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin