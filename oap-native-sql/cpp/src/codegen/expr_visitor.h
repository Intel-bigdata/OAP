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

#include <iostream>
#include "codegen/code_generator.h"
#include "codegen/common/visitor_base.h"

#define ARROW_COMPUTE 0x0001
#define GANDIVA 0x0002
#define COMPUTE_EXT 0x0003

namespace sparkcolumnarplugin {
namespace codegen {
class ExprVisitor : public VisitorBase {
 public:
  ExprVisitor() {}

  arrow::Status create(std::vector<std::shared_ptr<gandiva::Expression>> exprs_vector,
                       int* out) {
    arrow::Status status = arrow::Status::OK();
    for (auto expr : exprs_vector) {
      status = expr->root()->Accept(*this);
      if (!status.ok()) {
        return status;
      }
    }
    *out = codegen_type;
    return status;
  }

 private:
  // std::vector<std::string> ac{
  //    "sum", "max", "min", "count", "getPrepareFunc", "splitArrayList", "encodeArray"};
  std::vector<std::string> gdv{"add", "substract", "multiply", "divide"};
  std::vector<std::string> ce{};
  int codegen_type;
  arrow::Status Visit(const gandiva::FunctionNode& node);
};
}  // namespace codegen
}  // namespace sparkcolumnarplugin
