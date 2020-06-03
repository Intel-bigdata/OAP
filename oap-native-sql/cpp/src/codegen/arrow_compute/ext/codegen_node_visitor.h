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

#include <sstream>
#include "codegen/common/visitor_base.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
class CodeGenNodeVisitor : public VisitorBase {
 public:
  CodeGenNodeVisitor(std::shared_ptr<gandiva::Node> func,
                     std::vector<std::vector<std::shared_ptr<arrow::Field>>> field_list_v,
                     int* func_count, std::stringstream* codes_ss)
      : func_(func),
        field_list_v_(field_list_v),
        func_count_(func_count),
        codes_ss_(codes_ss) {}

  arrow::Status Eval() {
    RETURN_NOT_OK(func_->Accept(*this));
    return arrow::Status::OK();
  }
  std::string GetResult();
  std::string GetPreCheck();
  arrow::Status Visit(const gandiva::FunctionNode& node) override;
  arrow::Status Visit(const gandiva::FieldNode& node) override;
  arrow::Status Visit(const gandiva::IfNode& node) override;
  arrow::Status Visit(const gandiva::LiteralNode& node) override;
  arrow::Status Visit(const gandiva::BooleanNode& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<int>& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<long int>& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<std::string>& node) override;

 private:
  std::shared_ptr<gandiva::Node> func_;
  std::vector<std::vector<std::shared_ptr<arrow::Field>>> field_list_v_;
  int* func_count_;
  // output
  std::stringstream* codes_ss_;
  std::string codes_str_;
  std::string check_str_;
};
static arrow::Status MakeCodeGenNodeVisitor(
    std::shared_ptr<gandiva::Node> func,
    std::vector<std::vector<std::shared_ptr<arrow::Field>>> field_list_v, int* func_count,
    std::stringstream* codes_ss, std::shared_ptr<CodeGenNodeVisitor>* out) {
  auto visitor =
      std::make_shared<CodeGenNodeVisitor>(func, field_list_v, func_count, codes_ss);
  RETURN_NOT_OK(visitor->Eval());
  *out = visitor;
  return arrow::Status::OK();
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
