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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/checked_cast.h>
#include <iostream>
#include <memory>
#include <sstream>
#include "codegen/arrow_compute/ext/array_item_index.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
class ItemIterator {
 public:
  ItemIterator(std::shared_ptr<arrow::DataType> type, bool is_array_list);
  ~ItemIterator() {}
  arrow::Status Submit(ArrayList in_arr_list, std::shared_ptr<arrow::Array> selection,
                       std::function<arrow::Status()>* next,
                       std::function<bool()>* is_null, std::function<void*()>* get);
  arrow::Status Submit(std::shared_ptr<arrow::Array> in_arr,
                       std::shared_ptr<arrow::Array> selection,
                       std::function<arrow::Status()>* next,
                       std::function<bool()>* is_null, std::function<void*()>* get);
  arrow::Status Submit(ArrayList in_arr_list,
                       std::function<bool(ArrayItemIndex)>* is_null,
                       std::function<void*(ArrayItemIndex)>* get);
  arrow::Status Submit(std::shared_ptr<arrow::Array> in_arr,
                       std::function<bool(int)>* is_null, std::function<void*(int)>* get);
  class Impl;

 private:
  std::shared_ptr<Impl> impl_;
};
static arrow::Status MakeArrayListItemIterator(std::shared_ptr<arrow::DataType> type,
                                               std::shared_ptr<ItemIterator>* out) {
  *out = std::make_shared<ItemIterator>(type, true);
  return arrow::Status::OK();
}
static arrow::Status MakeArrayItemIterator(std::shared_ptr<arrow::DataType> type,
                                           std::shared_ptr<ItemIterator>* out) {
  *out = std::make_shared<ItemIterator>(type, false);
  return arrow::Status::OK();
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
