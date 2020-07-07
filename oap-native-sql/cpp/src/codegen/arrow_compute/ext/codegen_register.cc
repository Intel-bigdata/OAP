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

#include "codegen/arrow_compute/ext/codegen_register.h"

#include <gandiva/node.h>

#include <iostream>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
std::string CodeGenRegister::GetFingerprint() { return fp_; }
arrow::Status CodeGenRegister::Visit(const gandiva::FunctionNode& node) {
  std::stringstream ss;
  ss << node.descriptor()->return_type()->ToString() << " " << node.descriptor()->name()
     << " (";
  bool skip_comma = true;
  for (auto& child : node.children()) {
    std::shared_ptr<CodeGenRegister> _node;
    RETURN_NOT_OK(MakeCodeGenRegister(child, &_node));
    if (skip_comma) {
      ss << _node->GetFingerprint();
      skip_comma = false;
    } else {
      ss << ", " << _node->GetFingerprint();
    }
  }
  ss << ")";
  fp_ = ss.str();

  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::FieldNode& node) {
  fp_ = "(" + node.field()->type()->ToString() + ") ";
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::IfNode& node) {
  std::stringstream ss;
  std::shared_ptr<CodeGenRegister> _condition_node;
  RETURN_NOT_OK(MakeCodeGenRegister(node.condition(), &_condition_node));
  std::shared_ptr<CodeGenRegister> _then_node;
  RETURN_NOT_OK(MakeCodeGenRegister(node.then_node(), &_then_node));
  std::shared_ptr<CodeGenRegister> _else_node;
  RETURN_NOT_OK(MakeCodeGenRegister(node.else_node(), &_else_node));
  ss << "if (" << _condition_node->GetFingerprint() << ") { ";
  ss << _then_node->GetFingerprint() << " } else { ";
  ss << _else_node->GetFingerprint() << " }";
  fp_ = ss.str();
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::LiteralNode& node) {
  fp_ = node.ToString();
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::BooleanNode& node) {
  std::stringstream ss;
  bool first = true;
  for (auto& child : node.children()) {
    if (!first) {
      if (node.expr_type() == gandiva::BooleanNode::AND) {
        ss << " && ";
      } else {
        ss << " || ";
      }
    }
    std::shared_ptr<CodeGenRegister> _child_node;
    RETURN_NOT_OK(MakeCodeGenRegister(child, &_child_node));
    ss << _child_node->GetFingerprint();
    first = false;
  }
  fp_ = ss.str();

  return arrow::Status::OK();
}

template <typename Type>
std::string InExpressionVisitInternal(const gandiva::InExpressionNode<Type>& node) {
  std::stringstream ss;
  std::shared_ptr<CodeGenRegister> _child_node;
  MakeCodeGenRegister(node.eval_expr(), &_child_node);
  ss << _child_node->GetFingerprint() << " IN (";
  bool add_comma = false;
  for (auto& value : node.values()) {
    if (add_comma) {
      ss << ", ";
    }
    // add type in the front to differentiate
    ss << value;
    add_comma = true;
  }
  ss << ")";
  return ss.str();
}

arrow::Status CodeGenRegister::Visit(const gandiva::InExpressionNode<int>& node) {
  fp_ = InExpressionVisitInternal(node);
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::InExpressionNode<long int>& node) {
  fp_ = InExpressionVisitInternal(node);
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::InExpressionNode<std::string>& node) {
  fp_ = InExpressionVisitInternal(node);
  return arrow::Status::OK();
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
