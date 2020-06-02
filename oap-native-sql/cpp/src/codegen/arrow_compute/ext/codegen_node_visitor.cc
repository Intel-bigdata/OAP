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

#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include <gandiva/node.h>
#include <iostream>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
std::string CodeGenNodeVisitor::GetResult() { return codes_str_; }
std::string CodeGenNodeVisitor::GetPreCheck() { return check_str_; }
arrow::Status CodeGenNodeVisitor::Visit(const gandiva::FunctionNode& node) {
  std::vector<std::shared_ptr<CodeGenNodeVisitor>> child_visitor_list;
  auto cur_func_id = *func_count_;
  for (auto child : node.children()) {
    std::shared_ptr<CodeGenNodeVisitor> child_visitor;
    *func_count_ = *func_count_ + 1;
    RETURN_NOT_OK(MakeCodeGenNodeVisitor(child, field_list_v_, func_count_, codes_ss_,
                                         &child_visitor));
    child_visitor_list.push_back(child_visitor);
  }

  auto func_name = node.descriptor()->name();
  std::stringstream ss;
  if (func_name.compare("less_than") == 0) {
    auto check_str = child_visitor_list[0]->GetPreCheck();
    if (!check_str.empty()) {
      ss << check_str << " || ";
    }
    check_str = child_visitor_list[1]->GetPreCheck();
    if (!check_str.empty()) {
      ss << check_str << " || ";
    }
    ss << "(" << child_visitor_list[0]->GetResult() << " < "
       << child_visitor_list[1]->GetResult() << ")";
  }
  if (func_name.compare("greater_than") == 0) {
    auto check_str = child_visitor_list[0]->GetPreCheck();
    if (!check_str.empty()) {
      ss << check_str << " || ";
    }
    check_str = child_visitor_list[1]->GetPreCheck();
    if (!check_str.empty()) {
      ss << check_str << " || ";
    }
    ss << "(" << child_visitor_list[0]->GetResult() << " > "
       << child_visitor_list[1]->GetResult() << ")";
  }
  if (func_name.compare("less_than_or_equal_to") == 0) {
    auto check_str = child_visitor_list[0]->GetPreCheck();
    if (!check_str.empty()) {
      ss << check_str << " || ";
    }
    check_str = child_visitor_list[1]->GetPreCheck();
    if (!check_str.empty()) {
      ss << check_str << " || ";
    }
    ss << "(" << child_visitor_list[0]->GetResult()
       << " <= " << child_visitor_list[1]->GetResult() << ")";
  }
  if (func_name.compare("greater_than_or_equal_to") == 0) {
    auto check_str = child_visitor_list[0]->GetPreCheck();
    if (!check_str.empty()) {
      ss << check_str << " || ";
    }
    check_str = child_visitor_list[1]->GetPreCheck();
    if (!check_str.empty()) {
      ss << check_str << " || ";
    }
    ss << "(" << child_visitor_list[0]->GetResult()
       << " >= " << child_visitor_list[1]->GetResult() << ")";
  }
  if (func_name.compare("equal") == 0) {
    auto check_str = child_visitor_list[0]->GetPreCheck();
    if (!check_str.empty()) {
      ss << check_str << " || ";
    }
    check_str = child_visitor_list[1]->GetPreCheck();
    if (!check_str.empty()) {
      ss << check_str << " || ";
    }
    ss << "(" << child_visitor_list[0]->GetResult()
       << " == " << child_visitor_list[1]->GetResult() << ")";
  }
  if (func_name.compare("not") == 0) {
    ss << "!(" << child_visitor_list[0]->GetResult() << ")";
  }
  codes_str_ = ss.str();
  return arrow::Status::OK();
}
arrow::Status CodeGenNodeVisitor::Visit(const gandiva::FieldNode& node) {
  auto cur_func_id = *func_count_;
  auto this_field = node.field();
  int arg_id = 0;
  bool found = false;
  for (auto field_list : field_list_v_) {
    for (auto field : field_list) {
      if (field->name() == this_field->name()) {
        found = true;
        break;
      }
      arg_id++;
    }
    if (found) {
      break;
    }
  }
  std::string type_name;
  if (this_field->type()->id() == arrow::Type::STRING) {
    type_name = "std::string";
  } else {
    type_name = this_field->type()->name();
  }
  *codes_ss_ << type_name << " *input_field_" << cur_func_id << " = nullptr;"
             << std::endl;
  *codes_ss_ << "if (!is_null_func_list[" << arg_id << "]()) {" << std::endl;
  *codes_ss_ << "  input_field_" << cur_func_id << " = (" << type_name
             << "*)get_func_list[" << arg_id << "]();" << std::endl;
  *codes_ss_ << "}" << std::endl;

  std::stringstream ss;
  ss << "*input_field_" << cur_func_id;
  codes_str_ = ss.str();

  std::stringstream ss_check;
  ss_check << "(input_field_" << cur_func_id << " == nullptr)";
  check_str_ = ss_check.str();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::IfNode& node) {
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::LiteralNode& node) {
  auto cur_func_id = *func_count_;
  if (node.return_type()->id() == arrow::Type::STRING) {
    *codes_ss_ << "const std::string input_field_" << cur_func_id << R"( = ")"
               << gandiva::ToString(node.holder()) << R"(";)" << std::endl;

  } else {
    *codes_ss_ << "const " << node.return_type()->name() << " input_field_" << cur_func_id
               << " = " << gandiva::ToString(node.holder()) << ";" << std::endl;
  }

  std::stringstream ss;
  ss << "input_field_" << cur_func_id;
  codes_str_ = ss.str();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::BooleanNode& node) {
  std::vector<std::shared_ptr<CodeGenNodeVisitor>> child_visitor_list;
  auto cur_func_id = *func_count_;
  for (auto child : node.children()) {
    std::shared_ptr<CodeGenNodeVisitor> child_visitor;
    *func_count_ = *func_count_ + 1;
    RETURN_NOT_OK(MakeCodeGenNodeVisitor(child, field_list_v_, func_count_, codes_ss_,
                                         &child_visitor));
    child_visitor_list.push_back(child_visitor);
  }

  std::stringstream ss;
  if (node.expr_type() == gandiva::BooleanNode::AND) {
    ss << "(" << child_visitor_list[0]->GetResult() << ") && ("
       << child_visitor_list[1]->GetResult() << ")";
  }
  if (node.expr_type() == gandiva::BooleanNode::OR) {
    ss << "(" << child_visitor_list[0]->GetResult() << ") || ("
       << child_visitor_list[1]->GetResult() << ")";
  }
  codes_str_ = ss.str();
  return arrow::Status::OK();
}
arrow::Status CodeGenNodeVisitor::Visit(const gandiva::InExpressionNode<int>& node) {
  return arrow::Status::OK();
}
arrow::Status CodeGenNodeVisitor::Visit(const gandiva::InExpressionNode<long int>& node) {
  return arrow::Status::OK();
}
arrow::Status CodeGenNodeVisitor::Visit(
    const gandiva::InExpressionNode<std::string>& node) {
  return arrow::Status::OK();
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
