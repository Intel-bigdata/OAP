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
