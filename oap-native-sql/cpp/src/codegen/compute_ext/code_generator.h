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

#include <arrow/type.h>
#include "codegen/code_generator.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace computeext {

class ComputeExtCodeGenerator : public CodeGenerator {
 public:
  ComputeExtCodeGenerator(
      std::shared_ptr<arrow::Schema> schema_ptr,
      std::vector<std::shared_ptr<gandiva::Expression>> exprs_vector,
      std::vector<std::shared_ptr<arrow::Field>> ret_types, bool return_when_finish,
      std::vector<std::shared_ptr<::gandiva::Expression>> finish_exprs_vector) {}
  ~ComputeExtCodeGenerator() {}
  arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) {
    return arrow::Status::OK();
  }

  arrow::Status getResSchema(std::shared_ptr<arrow::Schema>* out) {
    return arrow::Status::OK();
  }
  arrow::Status SetMember(const std::shared_ptr<arrow::RecordBatch>& in) {
    arrow::Status status = arrow::Status::OK();
    return status;
  }

  arrow::Status evaluate(const std::shared_ptr<arrow::RecordBatch>& in,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    arrow::Status status = arrow::Status::OK();
    return status;
  }

  arrow::Status finish(std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    return arrow::Status::OK();
  }
};

}  // namespace computeext
}  // namespace codegen
}  // namespace sparkcolumnarplugin
