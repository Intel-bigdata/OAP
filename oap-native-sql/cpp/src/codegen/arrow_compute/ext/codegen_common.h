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
#include <arrow/compute/context.h>
#include <arrow/type.h>

#include <string>

#include "codegen/arrow_compute/ext/code_generator_base.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

std::string BaseCodes();

int FileSpinLock(std::string path);

void FileSpinUnLock(int fd);

std::string GetTypeString(std::shared_ptr<arrow::DataType> type);

arrow::Status CompileCodes(std::string codes, std::string signature);

arrow::Status LoadLibrary(std::string signature, arrow::compute::FunctionContext* ctx,
                          std::shared_ptr<CodeGenBase>* out);
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
