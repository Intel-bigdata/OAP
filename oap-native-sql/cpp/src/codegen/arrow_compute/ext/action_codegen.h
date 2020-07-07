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

#include <arrow/status.h>
#include <gandiva/configuration.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>

#include <iostream>
#include <sstream>

#include "codegen/arrow_compute/ext/codegen_common.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_signed_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_unsigned_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_floating_point<I>> {
  using Type = arrow::DoubleType;
};
class ActionCodeGen {
 public:
  bool IsGroupBy() { return is_key_; }
  bool IsPreProjected() {
    if (projector_expr_) {
      return true;
    } else {
      return false;
    }
  }
  std::shared_ptr<gandiva::Expression> GetProjectorExpr() { return projector_expr_; }
  std::vector<std::shared_ptr<arrow::Field>> GetInputFieldList() {
    return input_field_list_;
  }
  std::vector<std::string> GetInputDataNameList() { return input_data_list_; }
  std::vector<std::string> GetVariablesList() { return func_sig_list_; }
  std::vector<std::string> GetVariablesDefineList() {
    return func_sig_define_codes_list_;
  }
  std::vector<std::pair<std::string, std::string>> GetTypedInputList() {
    return typed_input_and_prepare_list_;
  }
  std::vector<std::string> GetOnExistsPrepareCodesList() {
    return on_exists_prepare_codes_list_;
  }
  std::vector<std::string> GetOnNewPrepareCodesList() {
    return on_new_prepare_codes_list_;
  }
  std::vector<std::string> GetOnExistsCodesList() { return on_exists_codes_list_; }
  std::vector<std::string> GetOnNewCodesList() { return on_new_codes_list_; }
  std::vector<std::string> GetOnFinishCodesList() { return on_finish_codes_list_; }
  std::vector<std::string> GetFinishVariablesList() { return finish_variable_list_; }
  std::vector<std::string> GetFinishVariablesDefineList() {
    return finish_var_define_codes_list_;
  }
  std::vector<std::string> GetFinishVariablesParameterList() {
    return finish_var_parameter_codes_list_;
  }
  std::vector<std::string> GetFinishVariablesPrepareList() {
    return finish_var_prepare_codes_list_;
  }
  std::vector<std::string> GetFinishVariablesToBuilderList() {
    return finish_var_to_builder_codes_list_;
  }
  std::vector<std::string> GetFinishVariablesToArrayList() {
    return finish_var_to_array_codes_list_;
  }
  std::vector<std::string> GetFinishVariablesArrayList() {
    return finish_var_array_codes_list_;
  }

 protected:
  bool is_key_ = false;
  std::shared_ptr<gandiva::Expression> projector_expr_;
  std::vector<std::shared_ptr<arrow::Field>> input_field_list_;
  std::vector<std::string> input_data_list_;
  std::vector<std::pair<std::string, std::string>> typed_input_and_prepare_list_;
  std::vector<std::string> func_sig_list_;
  std::vector<std::string> func_sig_define_codes_list_;
  std::vector<std::string> on_exists_prepare_codes_list_;
  std::vector<std::string> on_new_prepare_codes_list_;
  std::vector<std::string> on_exists_codes_list_;
  std::vector<std::string> on_new_codes_list_;
  std::vector<std::string> on_finish_codes_list_;
  std::vector<std::string> finish_variable_list_;
  std::vector<std::string> finish_var_define_codes_list_;
  std::vector<std::string> finish_var_prepare_codes_list_;
  std::vector<std::string> finish_var_parameter_codes_list_;
  std::vector<std::string> finish_var_to_builder_codes_list_;
  std::vector<std::string> finish_var_to_array_codes_list_;
  std::vector<std::string> finish_var_array_codes_list_;
  std::string Replace(std::string& str_input, const std::string& oldStr,
                      const std::string& newStr) {
    auto str = str_input;
    std::string::size_type pos = 0u;
    while ((pos = str.find(oldStr, pos)) != std::string::npos) {
      str.replace(pos, oldStr.length(), newStr);
      pos += newStr.length();
    }
    return str;
  }
  std::string GetTypedVectorDefineString(std::shared_ptr<arrow::DataType> type,
                                         std::string name) {
    return "std::vector<" + GetCTypeString(type) + "> " + name;
  }

  void GetTypedArrayCastString(std::shared_ptr<arrow::DataType> type, std::string name) {
    std::stringstream ss;
    auto cached_name = Replace(name, "[", "_");
    cached_name = "typed_" + Replace(cached_name, "]", "");
    ss << "auto " << cached_name
       << " = std::dynamic_pointer_cast<arrow::" << GetTypeString(type, "Array") << ">("
       << name << ");";
    typed_input_and_prepare_list_.push_back(std::make_pair(cached_name, ss.str()));
  }

  void GetTypedArrayCastByNameString(std::shared_ptr<arrow::DataType> type,
                                     std::string name) {
    std::stringstream ss;
    auto cached_name = "typed_" + name;
    ss << "auto " << cached_name
       << " = std::dynamic_pointer_cast<arrow::" << GetTypeString(type, "Array")
       << ">(projected_batch->GetColumnByName(\"" << name << "\"));";
    typed_input_and_prepare_list_.push_back(std::make_pair(cached_name, ss.str()));
  }

  std::string GetTypedVectorAndBuilderDefineString(std::shared_ptr<arrow::DataType> type,
                                                   std::string name) {
    std::stringstream ss;
    auto cache_name = name + "_vector_";
    auto builder_name = name + "_builder_";
    ss << "std::vector<" << GetCTypeString(type) << "> " << cache_name << ";"
       << std::endl;
    ss << "std::shared_ptr<arrow::" << GetTypeString(type, "Builder") << "> "
       << builder_name << ";" << std::endl;
    return ss.str();
  }

  std::string GetTypedVectorAndBuilderPrepareString(std::shared_ptr<arrow::DataType> type,
                                                    std::string name) {
    std::stringstream ss;
    auto cache_name_tmp = name + "_vector_tmp";
    auto cache_name = name + "_vector_";
    ss << cache_name << " = " << cache_name_tmp << ";" << std::endl;
    auto builder_name_tmp = name + "_builder";
    auto builder_name = name + "_builder_";
    ss << "std::unique_ptr<arrow::ArrayBuilder> " << builder_name_tmp << ";" << std::endl;
    ss << "arrow::MakeBuilder(ctx_->memory_pool(), arrow::" << GetArrowTypeDefString(type)
       << ", &" << builder_name_tmp << ");" << std::endl;
    ss << builder_name
       << ".reset(arrow::internal::checked_cast<arrow::" << GetTypeString(type, "Builder")
       << "*>(" << builder_name_tmp << ".release()));" << std::endl;
    return ss.str();
  }

  std::string GetTypedVectorToBuilderString(std::shared_ptr<arrow::DataType> type,
                                            std::string name) {
    std::stringstream ss;
    auto cache_name = name + "_vector_";
    auto builder_name = name + "_builder_";
    ss << "RETURN_NOT_OK(" << builder_name << "->Append(" << cache_name
       << "[offset_ + count]));" << std::endl;
    return ss.str();
  }

  std::string GetTypedResultToArrayString(std::shared_ptr<arrow::DataType> type,
                                          std::string name) {
    std::stringstream ss;
    auto builder_name = name + "_builder_";
    auto out_name = name + "_out";
    ss << "std::shared_ptr<arrow::Array> " << out_name << ";" << std::endl;
    ss << "RETURN_NOT_OK(" << builder_name << "->Finish(&" << out_name << "));"
       << std::endl;
    ss << builder_name << "->Reset();" << std::endl;
    return ss.str();
  }

  std::string GetTypedResultArrayString(std::shared_ptr<arrow::DataType> type,
                                        std::string name) {
    return name + "_out";
  }
};

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)

class GroupByActionCodeGen : public ActionCodeGen {
 public:
  GroupByActionCodeGen(std::string name, bool keep, std::vector<std::string> child_list,
                       std::vector<std::string> input_list,
                       std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                       std::string prepare_codes_str,
                       std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = true;
    auto sig_name = "action_groupby_" + name + "_";
    auto data_type = input_fields_list[0]->type();
    auto tmp_name = child_list[0];
    input_field_list_.push_back(input_fields_list[0]);
    input_data_list_.push_back(input_list[0]);
    if (keep == false) {
      return;
    }
    func_sig_list_.push_back(sig_name);

    GetTypedArrayCastString(data_type, input_list[0]);
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back(prepare_codes_str + "\n");
    on_exists_codes_list_.push_back("");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
  }
};  // namespace extra

class SumActionCodeGen : public ActionCodeGen {
 public:
  SumActionCodeGen(std::string name, std::vector<std::string> child_list,
                   std::vector<std::string> input_list,
                   std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                   std::string prepare_codes_str,
                   std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::string sig_name;
    std::shared_ptr<arrow::DataType> data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _name = projector->result()->name();
      auto _type = projector->result()->type();
      sig_name = "action_sum_" + _name + "_";
      data_type = _type;
      projector_expr_ = projector;
      GetTypedArrayCastByNameString(_type, _name);
    } else {
      sig_name = "action_sum_" + name + "_";
      data_type = input_fields_list[0]->type();
      GetTypedArrayCastString(data_type, input_list[0]);
    }
    func_sig_list_.push_back(sig_name);
    auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
    std::stringstream prepare_codes_ss;
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    // Since sum may overflow original data type, we need to calculate its accumulateType
    // here
    switch (data_type->id()) {
#define PROCESS(InType)                                          \
  case InType::type_id: {                                        \
    using AggrType = typename FindAccumulatorType<InType>::Type; \
    data_type = arrow::TypeTraits<AggrType>::type_singleton();   \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      default: {
        std::cout << "Not Found " << data_type->ToString() << ", type id is "
                  << data_type->id() << std::endl;
      } break;
    }

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(sig_name + "[i] += " + tmp_name + ";");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
  }
};

class CountActionCodeGen : public ActionCodeGen {
 public:
  CountActionCodeGen(std::string name, std::vector<std::string> child_list,
                     std::vector<std::string> input_list,
                     std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                     std::string prepare_codes_str,
                     std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    auto sig_name = "action_count_" + name + "_";
    auto data_type = arrow::int64();
    func_sig_list_.push_back(sig_name);
    GetTypedArrayCastString(data_type, input_list[0]);
    auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
    std::stringstream prepare_codes_ss;
    auto count_name_tmp = tmp_name + "_count";
    prepare_codes_ss << GetCTypeString(data_type) << " " << count_name_tmp << " = 0;"
                     << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    prepare_codes_ss << count_name_tmp << " = 1;" << std::endl;
    prepare_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                    "[i] += " + count_name_tmp + ";");
    on_new_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                 ".push_back(" + count_name_tmp + ");");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
  }
};

class CountLiteralActionCodeGen : public ActionCodeGen {
 public:
  CountLiteralActionCodeGen(std::string name, int arg,
                            std::vector<std::string> child_list,
                            std::vector<std::string> input_list,
                            std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                            std::string prepare_codes_str,
                            std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    auto sig_name = "action_countLiteral_" + name + "_";
    auto data_type = arrow::int64();
    auto tmp_name = sig_name + "_tmp";
    func_sig_list_.push_back(sig_name);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_codes_list_.push_back(sig_name + "[i] += " + std::to_string(arg) + ";");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + std::to_string(arg) + ");");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
  }
};

class SumCountActionCodeGen : public ActionCodeGen {
 public:
  SumCountActionCodeGen(std::string name, std::vector<std::string> child_list,
                        std::vector<std::string> input_list,
                        std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                        std::string prepare_codes_str,
                        std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::string sig_name;
    std::shared_ptr<arrow::DataType> data_type = arrow::float64();
    if (projector) {
      // if projection pre-defined, use projection input
      auto _name = projector->result()->name();
      sig_name = "action_sum_" + _name + "_";
      projector_expr_ = projector;
      GetTypedArrayCastByNameString(data_type, _name);
    } else {
      sig_name = "action_sum_" + name + "_";
      GetTypedArrayCastString(data_type, input_list[0]);
    }
    func_sig_list_.push_back(sig_name);
    auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
    std::stringstream prepare_codes_ss;
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(sig_name + "[i] += " + tmp_name + ";");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));

    sig_name = "action_count_" + name + "_";
    data_type = arrow::int64();
    func_sig_list_.push_back(sig_name);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
    prepare_codes_ss.str("");
    auto count_name_tmp = tmp_name + "_count";
    prepare_codes_ss << GetCTypeString(data_type) << " " << count_name_tmp << " = 0;"
                     << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    prepare_codes_ss << count_name_tmp << " = 1;" << std::endl;
    prepare_codes_ss << "}" << std::endl;
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                    "[i] += " + count_name_tmp + ";");
    on_new_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                 ".push_back(" + count_name_tmp + ");");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
  }
};

class AvgByCountActionCodeGen : public ActionCodeGen {
 public:
  AvgByCountActionCodeGen(std::string name, std::vector<std::string> child_list,
                          std::vector<std::string> input_list,
                          std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                          std::string prepare_codes_str,
                          std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    auto sig_name = "action_sum_" + name + "_";
    auto data_type = input_fields_list[0]->type();
    func_sig_list_.push_back(sig_name);
    GetTypedArrayCastString(data_type, input_list[0]);
    auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
    std::stringstream prepare_codes_ss;
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(sig_name + "[i] += " + tmp_name + ";");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");

    sig_name = "action_count_" + name + "_";
    data_type = input_fields_list[1]->type();
    func_sig_list_.push_back(sig_name);
    GetTypedArrayCastString(data_type, input_list[1]);
    tmp_name = typed_input_and_prepare_list_[1].first + "_tmp";
    prepare_codes_ss.str("");
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[1].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[1].first
                       << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[1].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(sig_name + "[i] += " + tmp_name + ";");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");

    sig_name = "action_avg_" + name + "_";
    auto sum_name = "action_sum_" + name + "_";
    auto count_name = "action_count_" + name + "_";
    data_type = arrow::float64();
    func_sig_list_.push_back(sig_name);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_codes_list_.push_back("");
    on_new_codes_list_.push_back("");
    on_finish_codes_list_.push_back(sig_name + ".push_back(" + sum_name + "[i] / " +
                                    count_name + "[i]);");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
  }
};

class MaxActionCodeGen : public ActionCodeGen {
 public:
  MaxActionCodeGen(std::string name, std::vector<std::string> child_list,
                   std::vector<std::string> input_list,
                   std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                   std::string prepare_codes_str,
                   std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::string sig_name;
    std::shared_ptr<arrow::DataType> data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _name = projector->result()->name();
      auto _type = projector->result()->type();
      sig_name = "action_max_" + _name + "_";
      data_type = _type;
      projector_expr_ = projector;
      GetTypedArrayCastByNameString(_type, _name);
    } else {
      sig_name = "action_max_" + name + "_";
      data_type = input_fields_list[0]->type();
      GetTypedArrayCastString(data_type, input_list[0]);
    }
    func_sig_list_.push_back(sig_name);
    auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
    std::stringstream prepare_codes_ss;
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(sig_name + "[i] = " + sig_name + "[i] > " + tmp_name +
                                    "?" + sig_name + "[i]:" + tmp_name + ";");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
  }
};

class MinActionCodeGen : public ActionCodeGen {
 public:
  MinActionCodeGen(std::string name, std::vector<std::string> child_list,
                   std::vector<std::string> input_list,
                   std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                   std::string prepare_codes_str,
                   std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::string sig_name;
    std::shared_ptr<arrow::DataType> data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _name = projector->result()->name();
      auto _type = projector->result()->type();
      sig_name = "action_min_" + _name + "_";
      data_type = _type;
      projector_expr_ = projector;
      GetTypedArrayCastByNameString(_type, _name);
    } else {
      sig_name = "action_min_" + name + "_";
      data_type = input_fields_list[0]->type();
      GetTypedArrayCastString(data_type, input_list[0]);
    }
    func_sig_list_.push_back(sig_name);
    auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
    std::stringstream prepare_codes_ss;
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(sig_name + "[i] = " + sig_name + "[i] < " + tmp_name +
                                    "?" + sig_name + "[i]:" + tmp_name + ";");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
  }
};

class AvgActionCodeGen : public ActionCodeGen {
 public:
  AvgActionCodeGen(std::string name, std::vector<std::string> child_list,
                   std::vector<std::string> input_list,
                   std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                   std::string prepare_codes_str,
                   std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::string sig_name;
    std::shared_ptr<arrow::DataType> data_type = arrow::float64();
    if (projector) {
      // if projection pre-defined, use projection input
      auto _name = projector->result()->name();
      sig_name = "action_sum_" + _name + "_";
      projector_expr_ = projector;
      GetTypedArrayCastByNameString(data_type, _name);
    } else {
      sig_name = "action_sum_" + name + "_";
      GetTypedArrayCastString(data_type, input_list[0]);
    }
    func_sig_list_.push_back(sig_name);
    auto tmp_name = sig_name + "_tmp";
    std::stringstream prepare_codes_ss;
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_codes_list_.push_back(sig_name + "[i] += " + tmp_name + ";");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");

    sig_name = "action_count_" + name + "_";
    data_type = arrow::int64();
    func_sig_list_.push_back(sig_name);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
    prepare_codes_ss.str("");
    auto count_name_tmp = tmp_name + "_count";
    prepare_codes_ss << GetCTypeString(data_type) << " " << count_name_tmp << " = 0;"
                     << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    prepare_codes_ss << count_name_tmp << " = 1;" << std::endl;
    prepare_codes_ss << "}" << std::endl;
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                    "[i] += " + count_name_tmp + ";");
    on_new_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                 ".push_back(" + count_name_tmp + ");");

    sig_name = "action_avg_" + name + "_";
    auto sum_name = "action_sum_" + name + "_";
    auto count_name = "action_count_" + name + "_";
    data_type = arrow::float64();
    func_sig_list_.push_back(sig_name);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_codes_list_.push_back("");
    on_new_codes_list_.push_back("");
    on_finish_codes_list_.push_back(sig_name + ".push_back(" + sum_name + "[i] / " +
                                    count_name + "[i]);");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
  }
};
#undef PROCESS_SUPPORTED_TYPES

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin