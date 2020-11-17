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

#include "codegen/arrow_compute/ext/expression_codegen_visitor.h"

#include <gandiva/decimal_scalar.h>
#include <gandiva/node.h>

#include <iostream>

#include "codegen/arrow_compute/ext/codegen_common.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
std::string ExpressionCodegenVisitor::GetInput() { return input_codes_str_; }
std::string ExpressionCodegenVisitor::GetResult() { return codes_str_; }
std::string ExpressionCodegenVisitor::GetPrepare() { return prepare_str_; }
std::string ExpressionCodegenVisitor::GetPreCheck() { return check_str_; }
std::string ExpressionCodegenVisitor::GetRealResult() { return real_codes_str_; }
std::string ExpressionCodegenVisitor::GetRealValidity() { return real_validity_str_; }
std::vector<std::string> ExpressionCodegenVisitor::GetHeaders() { return header_list_; }
ExpressionCodegenVisitor::FieldType ExpressionCodegenVisitor::GetFieldType() {
  return field_type_;
}

arrow::Status ExpressionCodegenVisitor::Visit(const gandiva::FunctionNode& node) {
  auto func_name = node.descriptor()->name();
  auto input_list = input_list_;

  std::vector<std::shared_ptr<ExpressionCodegenVisitor>> child_visitor_list;
  auto cur_func_id = *func_count_;
  for (auto child : node.children()) {
    std::shared_ptr<ExpressionCodegenVisitor> child_visitor;
    *func_count_ = *func_count_ + 1;

    RETURN_NOT_OK(MakeExpressionCodegenVisitor(child, input_list, field_list_v_,
                                               hash_relation_id_, func_count_,
                                               prepared_list_, &child_visitor));
    child_visitor_list.push_back(child_visitor);
    if (field_type_ == unknown || field_type_ == literal) {
      field_type_ = child_visitor->GetFieldType();
    } else if (field_type_ != child_visitor->GetFieldType() && field_type_ != literal &&
               child_visitor->GetFieldType() != literal) {
      field_type_ = mixed;
    }
    for (auto header : child_visitor->GetHeaders()) {
      if (std::find(header_list_.begin(), header_list_.end(), header) ==
          header_list_.end()) {
        header_list_.push_back(header);
      }
    }
  }
  if (node.return_type()->id() == arrow::Type::DECIMAL) {
    auto _decimal_type =
        std::dynamic_pointer_cast<arrow::Decimal128Type>(node.return_type());
    decimal_scale_ = std::to_string(_decimal_type->scale());
  }

  std::stringstream ss;

  if (func_name.compare("less_than") == 0) {
    real_codes_str_ = "(" + child_visitor_list[0]->GetResult() + " < " +
                      child_visitor_list[1]->GetResult() + ")";
    real_validity_str_ = CombineValidity(
        {child_visitor_list[0]->GetPreCheck(), child_visitor_list[1]->GetPreCheck()});
    ss << real_validity_str_ << " && " << real_codes_str_;
    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    codes_str_ = ss.str();
  } else if (func_name.compare("greater_than") == 0) {
    real_codes_str_ = "(" + child_visitor_list[0]->GetResult() + " > " +
                      child_visitor_list[1]->GetResult() + ")";
    real_validity_str_ = CombineValidity(
        {child_visitor_list[0]->GetPreCheck(), child_visitor_list[1]->GetPreCheck()});
    ss << real_validity_str_ << " && " << real_codes_str_;
    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    codes_str_ = ss.str();
  } else if (func_name.compare("less_than_or_equal_to") == 0) {
    real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                      " <= " + child_visitor_list[1]->GetResult() + ")";
    real_validity_str_ = CombineValidity(
        {child_visitor_list[0]->GetPreCheck(), child_visitor_list[1]->GetPreCheck()});
    ss << real_validity_str_ << " && " << real_codes_str_;
    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    codes_str_ = ss.str();
  } else if (func_name.compare("greater_than_or_equal_to") == 0) {
    real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                      " >= " + child_visitor_list[1]->GetResult() + ")";
    real_validity_str_ = CombineValidity(
        {child_visitor_list[0]->GetPreCheck(), child_visitor_list[1]->GetPreCheck()});
    ss << real_validity_str_ << " && " << real_codes_str_;
    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    codes_str_ = ss.str();
  } else if (func_name.compare("equal") == 0) {
    real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                      " == " + child_visitor_list[1]->GetResult() + ")";
    real_validity_str_ = CombineValidity(
        {child_visitor_list[0]->GetPreCheck(), child_visitor_list[1]->GetPreCheck()});
    ss << real_validity_str_ << " && " << real_codes_str_;
    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    codes_str_ = ss.str();
  } else if (func_name.compare("not") == 0) {
    std::string check_validity;
    if (child_visitor_list[0]->GetPreCheck() != "") {
      check_validity = child_visitor_list[0]->GetPreCheck() + " && ";
    }
    ss << check_validity << child_visitor_list[0]->GetRealValidity() << " && !"
       << child_visitor_list[0]->GetRealResult();
    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    codes_str_ = ss.str();
  } else if (func_name.compare("isnotnull") == 0) {
    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    codes_str_ = "isnotnull_" + std::to_string(cur_func_id);
    check_str_ = GetValidityName(codes_str_);
    std::stringstream prepare_ss;
    prepare_ss << "bool " << codes_str_ << " = " << child_visitor_list[0]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "bool " << check_str_ << " = true;" << std::endl;
    prepare_str_ += prepare_ss.str();
  } else if (func_name.compare("isnull") == 0) {
    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    codes_str_ = "isnotnull_" + std::to_string(cur_func_id);
    check_str_ = GetValidityName(codes_str_);
    std::stringstream prepare_ss;
    prepare_ss << "bool " << codes_str_ << " = !" << child_visitor_list[0]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "bool " << check_str_ << " = true;" << std::endl;
    prepare_str_ += prepare_ss.str();
  } else if (func_name.compare("substr") == 0) {
    ss << child_visitor_list[0]->GetResult() << ".substr("
       << "((" << child_visitor_list[1]->GetResult() << " - 1) < 0 ? 0 : ("
       << child_visitor_list[1]->GetResult() << " - 1)), "
       << child_visitor_list[2]->GetResult() << ")";
    check_str_ = child_visitor_list[0]->GetPreCheck();
    for (int i = 0; i < 3; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    // codes_str_ = ss.str();
    codes_str_ = "substr_" + std::to_string(cur_func_id);
    check_str_ = GetValidityName(codes_str_);
    std::stringstream prepare_ss;
    prepare_ss << "auto " << codes_str_ << " = " << ss.str() << ";" << std::endl;
    prepare_ss << "bool " << check_str_ << " = true;" << std::endl;
    prepare_str_ += prepare_ss.str();
  } else if (func_name.compare("upper") == 0) {
    std::stringstream prepare_ss;
    auto child_name = child_visitor_list[0]->GetResult();
    codes_str_ = "upper_" + std::to_string(cur_func_id);
    check_str_ = codes_str_ + "_validity";
    prepare_ss << "std::string " << codes_str_ << ";" << std::endl;
    prepare_ss << codes_str_ << ".resize(" << child_name << ".size());" << std::endl;
    prepare_ss << "bool " << check_str_ << " = " << child_visitor_list[0]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "if (" << check_str_ << ") {" << std::endl;
    prepare_ss << "std::transform(" << child_name << ".begin(), " << child_name
               << ".end(), " << codes_str_ << ".begin(), ::toupper);" << std::endl;
    prepare_ss << "}" << std::endl;
    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
  } else if (func_name.compare("lower") == 0) {
    std::stringstream prepare_ss;
    auto child_name = child_visitor_list[0]->GetResult();
    codes_str_ = "lower_" + std::to_string(cur_func_id);
    check_str_ = codes_str_ + "_validity";
    prepare_ss << "std::string " << codes_str_ << ";" << std::endl;
    prepare_ss << codes_str_ << ".resize(" << child_name << ".size());" << std::endl;
    prepare_ss << "bool " << check_str_ << " = " << child_visitor_list[0]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "if (" << check_str_ << ") {" << std::endl;
    prepare_ss << "std::transform(" << child_name << ".begin(), " << child_name
               << ".end(), " << codes_str_ << ".begin(), ::tolower);" << std::endl;
    prepare_ss << "}" << std::endl;
    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
  } else if (func_name.find("cast") != std::string::npos &&
             func_name.compare("castDATE") != 0 &&
             func_name.compare("castDECIMAL") != 0) {
    codes_str_ = func_name + "_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream fix_ss;
    if (node.return_type()->id() == arrow::Type::DOUBLE ||
        node.return_type()->id() == arrow::Type::FLOAT) {
      fix_ss << " * 1.0 ";
    }
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = " << child_visitor_list[0]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = static_cast<" << GetCTypeString(node.return_type())
               << ">(" << child_visitor_list[0]->GetResult() << fix_ss.str() << ");"
               << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
  } else if (func_name.find("hash") != std::string::npos) {
    if (child_visitor_list.size() == 1) {
      ss << "sparkcolumnarplugin::thirdparty::murmurhash32::" << func_name << "("
         << child_visitor_list[0]->GetResult() << ", "
         << child_visitor_list[0]->GetPreCheck() << ")" << std::endl;
      for (int i = 0; i < 1; i++) {
        prepare_str_ += child_visitor_list[i]->GetPrepare();
      }
    } else {
      ss << "sparkcolumnarplugin::thirdparty::murmurhash32::" << func_name << "("
         << child_visitor_list[0]->GetResult() << ", "
         << child_visitor_list[0]->GetPreCheck() << ", "
         << child_visitor_list[1]->GetResult() << ")" << std::endl;
      for (int i = 0; i < 2; i++) {
        prepare_str_ += child_visitor_list[i]->GetPrepare();
      }
    }
    check_str_ = "true";
    codes_str_ = ss.str();
    header_list_.push_back(R"(#include "third_party/murmurhash/murmurhash32.h")");
  } else if (func_name.compare("castDATE") == 0) {
    codes_str_ = func_name + "_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = " << child_visitor_list[0]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << func_name << "("
               << child_visitor_list[0]->GetResult() << ");" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
    header_list_.push_back(R"(#include "precompile/gandiva.h")");
  } else if (func_name.compare("castDECIMAL") == 0) {
    codes_str_ = func_name + "_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream fix_ss;
    auto decimal_type =
        std::dynamic_pointer_cast<arrow::Decimal128Type>(node.return_type());
    if (!child_visitor_list[0]->decimal_scale_.empty()) {
      fix_ss << ", " << child_visitor_list[0]->decimal_scale_ << ", "
             << decimal_type->scale();
    } else {
      fix_ss << ", " << decimal_type->precision() << ", " << decimal_type->scale();
    }
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = " << child_visitor_list[0]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << func_name << "("
               << child_visitor_list[0]->GetResult() << fix_ss.str() << ");" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
    header_list_.push_back(R"(#include "precompile/gandiva.h")");
  } else if (func_name.compare("rescaleDECIMAL") == 0) {
    codes_str_ = func_name + "_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream fix_ss;
    auto decimal_type =
        std::dynamic_pointer_cast<arrow::Decimal128Type>(node.return_type());
    if (!child_visitor_list[0]->decimal_scale_.empty()) {
      fix_ss << ", 0, " << decimal_type->scale();
    } else {
      fix_ss << ", " << decimal_type->precision() << ", " << decimal_type->scale();
    }
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = " << child_visitor_list[0]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = castDECIMAL(" << child_visitor_list[0]->GetResult()
               << fix_ss.str() << ");" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
    header_list_.push_back(R"(#include "precompile/gandiva.h")");
  } else if (func_name.compare("extractYear") == 0) {
    codes_str_ = func_name + "_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = " << child_visitor_list[0]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << func_name << "("
               << child_visitor_list[0]->GetResult() << ");" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
    header_list_.push_back(R"(#include "precompile/gandiva.h")");
  } else if (func_name.compare("round") == 0) {
    codes_str_ = func_name + "_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream fix_ss;
    if (child_visitor_list.size() > 1) {
      fix_ss << ", " << child_visitor_list[1]->GetResult();
    }
    for (int i = 0; i < 1; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = " << child_visitor_list[0]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = round_2(" << child_visitor_list[0]->GetResult()
               << fix_ss.str() << ");" << std::endl;
    prepare_ss << "}" << std::endl;

    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
    header_list_.push_back(R"(#include "precompile/gandiva.h")");
  } else if (func_name.compare("add") == 0) {
    codes_str_ = "add_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = ("
               << CombineValidity({child_visitor_list[0]->GetPreCheck(),
                                   child_visitor_list[1]->GetPreCheck()})
               << ");" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " + "
               << child_visitor_list[1]->GetResult() << ";" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
  } else if (func_name.compare("subtract") == 0) {
    codes_str_ = "subtract_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = ("
               << CombineValidity({child_visitor_list[0]->GetPreCheck(),
                                   child_visitor_list[1]->GetPreCheck()})
               << ");" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " - "
               << child_visitor_list[1]->GetResult() << ";" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
  } else if (func_name.compare("multiply") == 0) {
    codes_str_ = "multiply_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = ("
               << CombineValidity({child_visitor_list[0]->GetPreCheck(),
                                   child_visitor_list[1]->GetPreCheck()})
               << ");" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " * "
               << child_visitor_list[1]->GetResult() << ";" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
  } else if (func_name.compare("divide") == 0) {
    codes_str_ = "divide_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = ("
               << CombineValidity({child_visitor_list[0]->GetPreCheck(),
                                   child_visitor_list[1]->GetPreCheck()})
               << ");" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " / "
               << child_visitor_list[1]->GetResult() << ";" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
  } else if (func_name.compare("shift_left") == 0) {
    codes_str_ = "shift_left_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = ("
               << CombineValidity({child_visitor_list[0]->GetPreCheck(),
                                   child_visitor_list[1]->GetPreCheck()})
               << ");" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " << "
               << child_visitor_list[1]->GetResult() << ";" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
  } else if (func_name.compare("shift_right") == 0) {
    codes_str_ = "shift_right_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = ("
               << CombineValidity({child_visitor_list[0]->GetPreCheck(),
                                   child_visitor_list[1]->GetPreCheck()})
               << ");" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " >> "
               << child_visitor_list[1]->GetResult() << ";" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
  } else if (func_name.compare("bitwise_and") == 0) {
    codes_str_ = "bitwise_and_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = ("
               << CombineValidity({child_visitor_list[0]->GetPreCheck(),
                                   child_visitor_list[1]->GetPreCheck()})
               << ");" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " & "
               << child_visitor_list[1]->GetResult() << ";" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
  } else if (func_name.compare("bitwise_or") == 0) {
    codes_str_ = "bitwise_or_" + std::to_string(cur_func_id);
    auto validity = codes_str_ + "_validity";
    std::stringstream prepare_ss;
    prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
               << std::endl;
    prepare_ss << "bool " << validity << " = ("
               << CombineValidity({child_visitor_list[0]->GetPreCheck(),
                                   child_visitor_list[1]->GetPreCheck()})
               << ");" << std::endl;
    prepare_ss << "if (" << validity << ") {" << std::endl;
    prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " | "
               << child_visitor_list[1]->GetResult() << ";" << std::endl;
    prepare_ss << "}" << std::endl;

    for (int i = 0; i < 2; i++) {
      prepare_str_ += child_visitor_list[i]->GetPrepare();
    }
    prepare_str_ += prepare_ss.str();
    check_str_ = validity;
  } else {
    return arrow::Status::NotImplemented(func_name, " is currently not supported.");
  }
  return arrow::Status::OK();
}

arrow::Status ExpressionCodegenVisitor::Visit(const gandiva::FieldNode& node) {
  auto cur_func_id = *func_count_;
  auto this_field = node.field();
  std::stringstream prepare_ss;
  auto index_pair = GetFieldIndex(this_field, field_list_v_);
  auto index = index_pair.first;
  auto arg_id = index_pair.second;

  if (field_list_v_.size() == 1) {
    codes_str_ = input_list_[arg_id];
    codes_validity_str_ = GetValidityName(codes_str_);
  } else {
    if (index == 0) {
      codes_str_ = "hash_relation_" + std::to_string(hash_relation_id_) + "_" +
                   std::to_string(arg_id) + "_value";
      codes_validity_str_ = GetValidityName(codes_str_);
      input_codes_str_ = "hash_relation_" + std::to_string(hash_relation_id_) + "_" +
                         std::to_string(arg_id);
      prepare_ss << "  bool " << codes_validity_str_ << " = true;" << std::endl;
      prepare_ss << "  " << GetCTypeString(this_field->type()) << " " << codes_str_ << ";"
                 << std::endl;
      prepare_ss << "  if (" << input_codes_str_ << "->IsNull(x.array_id, x.id)) {"
                 << std::endl;
      prepare_ss << "    " << codes_validity_str_ << " = false;" << std::endl;
      prepare_ss << "  } else {" << std::endl;
      prepare_ss << "    " << codes_str_ << " = " << input_codes_str_
                 << "->GetValue(x.array_id, x.id);" << std::endl;
      prepare_ss << "  }" << std::endl;
      field_type_ = left;

    } else {
      codes_str_ = input_list_[arg_id];
      codes_validity_str_ = GetValidityName(codes_str_);
      field_type_ = right;
    }
  }

  check_str_ = codes_validity_str_;
  if (prepared_list_ != nullptr) {
    if (std::find((*prepared_list_).begin(), (*prepared_list_).end(), codes_str_) ==
        (*prepared_list_).end()) {
      (*prepared_list_).push_back(codes_str_);
      prepare_str_ = prepare_ss.str();
    }
  }
  return arrow::Status::OK();
}

arrow::Status ExpressionCodegenVisitor::Visit(const gandiva::IfNode& node) {
  std::stringstream prepare_ss;
  auto cur_func_id = *func_count_;

  std::vector<gandiva::NodePtr> children = {node.condition(), node.then_node(),
                                            node.else_node()};
  std::vector<std::shared_ptr<ExpressionCodegenVisitor>> child_visitor_list;
  for (auto child : children) {
    std::shared_ptr<ExpressionCodegenVisitor> child_visitor;
    *func_count_ = *func_count_ + 1;
    RETURN_NOT_OK(MakeExpressionCodegenVisitor(child, input_list_, field_list_v_,
                                               hash_relation_id_, func_count_,
                                               prepared_list_, &child_visitor));
    child_visitor_list.push_back(child_visitor);
    if (field_type_ == unknown || field_type_ == literal) {
      field_type_ = child_visitor->GetFieldType();
    } else if (field_type_ != child_visitor->GetFieldType() && field_type_ != literal &&
               child_visitor->GetFieldType() != literal) {
      field_type_ = mixed;
    }
    for (auto header : child_visitor->GetHeaders()) {
      if (std::find(header_list_.begin(), header_list_.end(), header) ==
          header_list_.end()) {
        header_list_.push_back(header);
      }
    }
  }
  for (int i = 0; i < 3; i++) {
    prepare_str_ += child_visitor_list[i]->GetPrepare();
  }
  auto condition_name = "condition_" + std::to_string(cur_func_id);
  auto condition_validity = condition_name + "_validity";
  prepare_ss << GetCTypeString(node.return_type()) << " " << condition_name << ";"
             << std::endl;
  prepare_ss << "bool " << condition_validity << ";" << std::endl;
  prepare_ss << "if (" << child_visitor_list[0]->GetResult() << ") {" << std::endl;
  prepare_ss << condition_name << " = " << child_visitor_list[1]->GetResult() << ";"
             << std::endl;
  prepare_ss << condition_validity << " = " << child_visitor_list[1]->GetPreCheck() << ";"
             << std::endl;
  prepare_ss << "} else {" << std::endl;
  prepare_ss << condition_name << " = " << child_visitor_list[2]->GetResult() << ";"
             << std::endl;
  prepare_ss << condition_validity << " = " << child_visitor_list[2]->GetPreCheck() << ";"
             << std::endl;
  prepare_ss << "}" << std::endl;
  codes_str_ = condition_name;
  prepare_str_ += prepare_ss.str();
  check_str_ = condition_validity;
  return arrow::Status::OK();
}

arrow::Status ExpressionCodegenVisitor::Visit(const gandiva::LiteralNode& node) {
  auto cur_func_id = *func_count_;
  std::stringstream codes_ss;
  if (node.return_type()->id() == arrow::Type::STRING) {
    codes_ss << "\"" << gandiva::ToString(node.holder()) << "\"" << std::endl;
  } else if (node.return_type()->id() == arrow::Type::DECIMAL) {
    auto scalar = arrow::util::get<gandiva::DecimalScalar128>(node.holder());
    auto decimal = arrow::Decimal128(scalar.value());
    codes_ss << "arrow::Decimal128(\"" << decimal.ToString(scalar.scale()) << "\")"
             << std::endl;
    decimal_scale_ = std::to_string(scalar.scale());
  } else {
    codes_ss << gandiva::ToString(node.holder()) << std::endl;
  }

  codes_str_ = codes_ss.str();
  check_str_ = node.is_null() ? "false" : "true";
  field_type_ = literal;
  return arrow::Status::OK();
}

arrow::Status ExpressionCodegenVisitor::Visit(const gandiva::BooleanNode& node) {
  std::vector<std::shared_ptr<ExpressionCodegenVisitor>> child_visitor_list;
  auto cur_func_id = *func_count_;
  for (auto child : node.children()) {
    std::shared_ptr<ExpressionCodegenVisitor> child_visitor;
    *func_count_ = *func_count_ + 1;
    RETURN_NOT_OK(MakeExpressionCodegenVisitor(child, input_list_, field_list_v_,
                                               hash_relation_id_, func_count_,
                                               prepared_list_, &child_visitor));

    prepare_str_ += child_visitor->GetPrepare();
    child_visitor_list.push_back(child_visitor);
    if (field_type_ == unknown || field_type_ == literal) {
      field_type_ = child_visitor->GetFieldType();
    } else if (field_type_ != child_visitor->GetFieldType() && field_type_ != literal &&
               child_visitor->GetFieldType() != literal) {
      field_type_ = mixed;
    }
    for (auto header : child_visitor->GetHeaders()) {
      if (std::find(header_list_.begin(), header_list_.end(), header) ==
          header_list_.end()) {
        header_list_.push_back(header);
      }
    }
  }

  std::stringstream ss;
  if (node.expr_type() == gandiva::BooleanNode::AND) {
    ss << "(" << child_visitor_list[0]->GetResult() << ") && ("
       << child_visitor_list[1]->GetResult() << ")";
  }
  if (node.expr_type() == gandiva::BooleanNode::OR) {
    ss << "(" << child_visitor_list[0]->GetResult() << ") || ("
       << child_visitor_list[1]->GetResult() << ")";
  }
  codes_str_ = ss.str();
  return arrow::Status::OK();
}

arrow::Status ExpressionCodegenVisitor::Visit(
    const gandiva::InExpressionNode<int>& node) {
  auto cur_func_id = *func_count_;
  std::shared_ptr<ExpressionCodegenVisitor> child_visitor;
  *func_count_ = *func_count_ + 1;

  RETURN_NOT_OK(MakeExpressionCodegenVisitor(node.eval_expr(), input_list_, field_list_v_,
                                             hash_relation_id_, func_count_,
                                             prepared_list_, &child_visitor));
  std::stringstream prepare_ss;
  prepare_ss << "std::vector<int> in_list_" << cur_func_id << " = {";
  bool add_comma = false;
  for (auto& value : node.values()) {
    if (add_comma) {
      prepare_ss << ", ";
    }
    // add type in the front to differentiate
    prepare_ss << value;
    add_comma = true;
  }
  prepare_ss << "};" << std::endl;

  std::stringstream ss;
  ss << child_visitor->GetPreCheck() << " && "
     << "std::find(in_list_" << cur_func_id << ".begin(), in_list_" << cur_func_id
     << ".end(), " << child_visitor->GetResult() << ") != "
     << "in_list_" << cur_func_id << ".end()";
  codes_str_ = ss.str();
  prepare_str_ = prepare_ss.str();
  field_type_ = child_visitor->GetFieldType();
  prepare_str_ += child_visitor->GetPrepare();
  for (auto header : child_visitor->GetHeaders()) {
    if (std::find(header_list_.begin(), header_list_.end(), header) ==
        header_list_.end()) {
      header_list_.push_back(header);
    }
  }
  return arrow::Status::OK();
}

arrow::Status ExpressionCodegenVisitor::Visit(
    const gandiva::InExpressionNode<long int>& node) {
  auto cur_func_id = *func_count_;
  std::shared_ptr<ExpressionCodegenVisitor> child_visitor;
  *func_count_ = *func_count_ + 1;

  RETURN_NOT_OK(MakeExpressionCodegenVisitor(node.eval_expr(), input_list_, field_list_v_,
                                             hash_relation_id_, func_count_,
                                             prepared_list_, &child_visitor));
  std::stringstream prepare_ss;
  prepare_ss << "std::vector<long int> in_list_" << cur_func_id << " = {";
  bool add_comma = false;
  for (auto& value : node.values()) {
    if (add_comma) {
      prepare_ss << ", ";
    }
    // add type in the front to differentiate
    prepare_ss << value;
    add_comma = true;
  }
  prepare_ss << "};" << std::endl;

  std::stringstream ss;
  ss << child_visitor->GetPreCheck() << " && "
     << "std::find(in_list_" << cur_func_id << ".begin(), in_list_" << cur_func_id
     << ".end(), " << child_visitor->GetResult() << ") != "
     << "in_list_" << cur_func_id << ".end()";
  codes_str_ = ss.str();
  prepare_str_ = prepare_ss.str();
  field_type_ = child_visitor->GetFieldType();
  prepare_str_ += child_visitor->GetPrepare();
  for (auto header : child_visitor->GetHeaders()) {
    if (std::find(header_list_.begin(), header_list_.end(), header) ==
        header_list_.end()) {
      header_list_.push_back(header);
    }
  }
  return arrow::Status::OK();
}

arrow::Status ExpressionCodegenVisitor::Visit(
    const gandiva::InExpressionNode<std::string>& node) {
  auto cur_func_id = *func_count_;
  std::shared_ptr<ExpressionCodegenVisitor> child_visitor;
  *func_count_ = *func_count_ + 1;

  RETURN_NOT_OK(MakeExpressionCodegenVisitor(node.eval_expr(), input_list_, field_list_v_,
                                             hash_relation_id_, func_count_,
                                             prepared_list_, &child_visitor));
  std::stringstream prepare_ss;
  prepare_ss << "std::vector<std::string> in_list_" << cur_func_id << " = {";
  bool add_comma = false;
  for (auto& value : node.values()) {
    if (add_comma) {
      prepare_ss << ", ";
    }
    // add type in the front to differentiate
    prepare_ss << R"(")" << value << R"(")";
    add_comma = true;
  }
  prepare_ss << "};" << std::endl;

  std::stringstream ss;
  ss << child_visitor->GetPreCheck() << " && "
     << "std::find(in_list_" << cur_func_id << ".begin(), in_list_" << cur_func_id
     << ".end(), " << child_visitor->GetResult() << ") != "
     << "in_list_" << cur_func_id << ".end()";
  codes_str_ = ss.str();
  prepare_str_ = prepare_ss.str();
  field_type_ = child_visitor->GetFieldType();
  prepare_str_ += child_visitor->GetPrepare();
  for (auto header : child_visitor->GetHeaders()) {
    if (std::find(header_list_.begin(), header_list_.end(), header) ==
        header_list_.end()) {
      header_list_.push_back(header);
    }
  }
  return arrow::Status::OK();
}

std::string ExpressionCodegenVisitor::CombineValidity(
    std::vector<std::string> validity_list) {
  bool first = true;
  std::stringstream out;
  for (auto validity : validity_list) {
    if (first) {
      if (validity.compare("true") != 0) {
        out << validity;
        first = false;
      }
    } else {
      if (validity.compare("true") != 0) {
        out << " && " << validity;
      }
    }
  }
  return out.str();
}

std::string ExpressionCodegenVisitor::GetValidityName(std::string name) {
  auto pos = name.find("!");
  if (pos == std::string::npos) {
    return (name + "_validity");
  } else {
    return (name.substr(pos + 1) + "_validity");
  }
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
