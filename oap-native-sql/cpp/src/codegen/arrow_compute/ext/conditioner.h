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

class ConditionerBase {
 public:
  virtual arrow::Status Submit(
      std::vector<std::function<bool(ArrayItemIndex)>> left_is_null_func_list,
      std::vector<std::function<void*(ArrayItemIndex)>> left_get_func_list,
      std::vector<std::function<bool(int)>> right_is_null_func_list,
      std::vector<std::function<void*(int)>> right_get_func_list,
      std::function<bool(ArrayItemIndex, int)>* out) {
    return arrow::Status::NotImplemented(
        "ConditionerBase Submit is an abstract interface.");
  }
};
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
