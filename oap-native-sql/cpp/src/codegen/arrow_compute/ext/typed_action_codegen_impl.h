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

#include <arrow/status.h>

#include <sstream>

#include "codegen/arrow_compute/ext/action_codegen.h"
#include "codegen/arrow_compute/ext/codegen_register.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
class TypedActionCodeGenImpl {
 public:
  TypedActionCodeGenImpl() {}
  int* GetFuncCountRef() { return &func_count_; }
  std::stringstream* GetCodeStreamRef() { return &codes_ss_; }
  std::vector<int>* GetInputIndexListRef() { return &input_index_list_; }
  std::vector<std::shared_ptr<arrow::Field>>* GetInputFieldsListRef() {
    return &input_fields_list_;
  }
  arrow::Status SetActionName(std::string action_name) {
    action_name_ = action_name;
    return arrow::Status::OK();
  }
  arrow::Status SetInputList(std::vector<std::string> var_list) {
    input_list_ = var_list;
    return arrow::Status::OK();
  }

  arrow::Status SetChildList(std::vector<std::string> var_list) {
    child_list_ = var_list;
    return arrow::Status::OK();
  }

  arrow::Status MakeGandivaProjection(
      std::shared_ptr<gandiva::Node> func_node,
      std::vector<std::shared_ptr<arrow::Field>> original_fields_list) {
    std::stringstream signature_ss;
    std::shared_ptr<CodeGenRegister> node_tmp;
    RETURN_NOT_OK(MakeCodeGenRegister(func_node, &node_tmp));
    signature_ss << std::hex << std::hash<std::string>{}(node_tmp->GetFingerprint());
    auto name = "projection_" + signature_ss.str();
    auto res_field = arrow::field(name, func_node->return_type());
    named_projector_ = gandiva::TreeExprBuilder::MakeExpression(func_node, res_field);
    std::cout << "MakeGandivaProjection: " << named_projector_->ToString()
              << ", result_field is " << named_projector_->result()->ToString()
              << std::endl;
    return arrow::Status::OK();
  }

  std::vector<int> GetInputIndexList() { return input_index_list_; }
  std::vector<std::shared_ptr<arrow::Field>> GetInputFieldsList() {
    return input_fields_list_;
  }
  std::string GetActionName() { return action_name_; }
  arrow::Status ProduceCodes(std::shared_ptr<ActionCodeGen>* action_codegen) {
    if (action_name_.compare("action_sum_count") == 0) {
      std::string name;
      if (!input_index_list_.empty()) {
        name = std::to_string(input_index_list_[0]);
      }
      *action_codegen = std::make_shared<SumCountActionCodeGen>(
          name, child_list_, input_list_, input_fields_list_, codes_ss_.str(),
          named_projector_);

    } else if (action_name_.find("action_groupby") != std::string::npos) {
      std::string name;
      if (!input_index_list_.empty()) {
        name = std::to_string(input_index_list_[0]);
      }
      bool keep = true;
      if (action_name_.size() > 14) {
        keep = false;
      }
      *action_codegen = std::make_shared<GroupByActionCodeGen>(
          name, keep, child_list_, input_list_, input_fields_list_, codes_ss_.str(),
          named_projector_);

    } else if (action_name_.compare("action_sum") == 0) {
      std::string name;
      if (!input_index_list_.empty()) {
        name = std::to_string(input_index_list_[0]);
      }
      *action_codegen = std::make_shared<SumActionCodeGen>(
          name, child_list_, input_list_, input_fields_list_, codes_ss_.str(),
          named_projector_);

    } else if (action_name_.compare("action_count") == 0) {
      std::string name;
      if (!input_index_list_.empty()) {
        name = std::to_string(input_index_list_[0]);
      }
      *action_codegen = std::make_shared<CountActionCodeGen>(
          name, child_list_, input_list_, input_fields_list_, codes_ss_.str(),
          named_projector_);

    } else if (action_name_.compare(0, 20, "action_countLiteral_") == 0) {
      auto lit = std::stoi(action_name_.substr(20));
      *action_codegen = std::make_shared<CountLiteralActionCodeGen>(
          "count_literal_" + std::to_string(lit), lit, child_list_, input_list_,
          input_fields_list_, codes_ss_.str(), named_projector_);

    } else if (action_name_.compare("action_avg") == 0) {
      std::string name;
      if (!input_index_list_.empty()) {
        name = std::to_string(input_index_list_[0]);
      }
      *action_codegen = std::make_shared<AvgActionCodeGen>(
          name, child_list_, input_list_, input_fields_list_, codes_ss_.str(),
          named_projector_);

    } else if (action_name_.compare("action_avgByCount") == 0) {
      std::string name;
      if (input_index_list_.size() == 2) {
        name = std::to_string(input_index_list_[0]) + "_" +
               std::to_string(input_index_list_[1]);
      }
      *action_codegen = std::make_shared<AvgByCountActionCodeGen>(
          name, child_list_, input_list_, input_fields_list_, codes_ss_.str(),
          named_projector_);

    } else if (action_name_.compare("action_min") == 0) {
      std::string name;
      if (!input_index_list_.empty()) {
        name = std::to_string(input_index_list_[0]);
      }
      *action_codegen = std::make_shared<MinActionCodeGen>(
          name, child_list_, input_list_, input_fields_list_, codes_ss_.str(),
          named_projector_);

    } else if (action_name_.compare("action_max") == 0) {
      std::string name;
      if (!input_index_list_.empty()) {
        name = std::to_string(input_index_list_[0]);
      }
      *action_codegen = std::make_shared<MaxActionCodeGen>(
          name, child_list_, input_list_, input_fields_list_, codes_ss_.str(),
          named_projector_);
    } else {
      std::cout << "action_name " << action_name_ << " is unrecognized" << std::endl;
      return arrow::Status::Invalid("Invalid action_name ", action_name_);
    }
    return arrow::Status::OK();
  }

 private:
  int func_count_ = 0;
  std::stringstream codes_ss_;
  std::vector<int> input_index_list_;
  std::vector<std::shared_ptr<arrow::Field>> input_fields_list_;
  std::vector<std::string> input_list_;
  std::vector<std::string> child_list_;
  std::string action_name_;
  std::shared_ptr<gandiva::Expression> named_projector_;
  gandiva::NodePtr func_node_;
};
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin