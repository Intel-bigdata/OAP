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

#include "codegen/expr_visitor.h"

#include <arrow/status.h>
namespace sparkcolumnarplugin {
namespace codegen {
arrow::Status ExprVisitor::Visit(const gandiva::FunctionNode& node) {
  auto desc = node.descriptor();
  if (std::find(gdv.begin(), gdv.end(), desc->name()) != gdv.end()) {
    // gandiva can handle this
    codegen_type = GANDIVA;
  } else if (std::find(ce.begin(), ce.end(), desc->name()) != ce.end()) {
    // we need to implement our own function to handle this
    codegen_type = COMPUTE_EXT;
  } else {
    // arrow compute can handle this
    codegen_type = ARROW_COMPUTE;
  }
  return arrow::Status::OK();
}
}  // namespace codegen
}  // namespace sparkcolumnarplugin
