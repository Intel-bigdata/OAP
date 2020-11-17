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

#include "codegen/arrow_compute/expr_visitor.h"

#include <arrow/array.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>

#include <memory>

#include "codegen/arrow_compute/expr_visitor_impl.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {

arrow::Status MakeExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                              std::shared_ptr<gandiva::Expression> expr,
                              std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                              ExprVisitorMap* expr_visitor_cache,
                              std::shared_ptr<ExprVisitor>* out) {
  auto visitor = std::make_shared<BuilderVisitor>(schema_ptr, expr->root(), ret_fields,
                                                  expr_visitor_cache);
  RETURN_NOT_OK(visitor->Eval());
  RETURN_NOT_OK(visitor->GetResult(out));
  return arrow::Status::OK();
}

arrow::Status MakeExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                              std::shared_ptr<gandiva::Expression> expr,
                              std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                              std::shared_ptr<gandiva::Expression> finish_expr,
                              ExprVisitorMap* expr_visitor_cache,
                              std::shared_ptr<ExprVisitor>* out) {
  auto visitor = std::make_shared<BuilderVisitor>(
      schema_ptr, expr->root(), ret_fields, finish_expr->root(), expr_visitor_cache);
  RETURN_NOT_OK(visitor->Eval());
  RETURN_NOT_OK(visitor->GetResult(out));
  return arrow::Status::OK();
}

arrow::Status BuilderVisitor::Visit(const gandiva::FieldNode& node) {
  node_id_ = node.field()->name();
  node_type_ = BuilderVisitorNodeType::FieldNode;
  return arrow::Status::OK();
}

arrow::Status BuilderVisitor::Visit(const gandiva::FunctionNode& node) {
  auto desc = node.descriptor();
  node_id_ = desc->name();
  node_type_ = BuilderVisitorNodeType::FunctionNode;
  std::shared_ptr<ExprVisitor> dependency;
  std::vector<std::string> param_names;
  auto func_name = desc->name();
  // if This functionNode is a "codegen",
  // we don't need to create expr_visitor for its children.
  if (func_name.compare(0, 17, "wholestagecodegen") == 0) {
    RETURN_NOT_OK(
        ExprVisitor::Make(std::dynamic_pointer_cast<gandiva::FunctionNode>(func_),
                          schema_, ret_fields_, &expr_visitor_));
  } else if (func_name.compare("standalone") == 0) {
    RETURN_NOT_OK(
        ExprVisitor::Make(std::dynamic_pointer_cast<gandiva::FunctionNode>(func_),
                          schema_, ret_fields_, &expr_visitor_));
  } else if (func_name.compare(0, 8, "codegen_") == 0) {
    RETURN_NOT_OK(
        ExprVisitor::Make(std::dynamic_pointer_cast<gandiva::FunctionNode>(func_),
                          schema_, ret_fields_, &expr_visitor_));
  } else if (func_name == "window") {
    RETURN_NOT_OK(ExprVisitor::MakeWindow(schema_, ret_fields_, node, &expr_visitor_));
  } else {
    for (auto child_node : node.children()) {
      auto child_visitor = std::make_shared<BuilderVisitor>(
          schema_, child_node, ret_fields_, expr_visitor_cache_);
      RETURN_NOT_OK(child_visitor->Eval());
      switch (child_visitor->GetNodeType()) {
        case BuilderVisitorNodeType::FunctionNode: {
          if (dependency) {
            return arrow::Status::Invalid(
                "BuilderVisitor build ExprVisitor failed, got two depency while only "
                "support one.");
          }
          RETURN_NOT_OK(child_visitor->GetResult(&dependency));
          node_id_.append(child_visitor->GetResult());
        } break;
        case BuilderVisitorNodeType::FieldNode: {
          std::string col_name = child_visitor->GetResult();
          node_id_.append(col_name);
          param_names.push_back(col_name);
        } break;
        default:
          return arrow::Status::Invalid("BuilderVisitorNodeType is invalid");
      }
    }

    // Add a new type of Function "Action", which will not create a new expr_visitor,
    // instead, it will register itself to its dependency
    if (func_name.compare(0, 7, "action_") == 0) {
      if (dependency) {
        RETURN_NOT_OK(dependency->AppendAction(func_name, param_names));
        expr_visitor_ = dependency;
#ifdef DEBUG
        std::cout << "Build ExprVisitor for " << node_id_ << ", return ExprVisitor is "
                  << expr_visitor_ << std::endl;
#endif
        return arrow::Status::OK();
      } else {
        return arrow::Status::Invalid(
            "BuilderVisitor is processing an action without dependency, this is "
            "invalid.");
      }
    }

    // Get or insert exprVisitor
    auto search = expr_visitor_cache_->find(node_id_);
    if (search == expr_visitor_cache_->end()) {
      if (dependency) {
        RETURN_NOT_OK(ExprVisitor::Make(schema_, node.descriptor()->name(), param_names,
                                        dependency, finish_func_, &expr_visitor_));
      } else {
        RETURN_NOT_OK(ExprVisitor::Make(schema_, node.descriptor()->name(), param_names,
                                        nullptr, finish_func_, &expr_visitor_));
      }
      expr_visitor_cache_->insert(
          std::pair<std::string, std::shared_ptr<ExprVisitor>>(node_id_, expr_visitor_));
#ifdef DEBUG
      std::cout << "Build ExprVisitor for " << node_id_ << ", return ExprVisitor is "
                << expr_visitor_ << std::endl;
#endif
      return arrow::Status::OK();
    }
    expr_visitor_ = search->second;
#ifdef DEBUG
    std::cout << "Build ExprVisitor for " << node_id_ << ", return ExprVisitor is "
              << expr_visitor_ << std::endl;
#endif
  }

  return arrow::Status::OK();
}

std::string BuilderVisitor::GetResult() { return node_id_; }

arrow::Status BuilderVisitor::GetResult(std::shared_ptr<ExprVisitor>* out) {
  if (!expr_visitor_) {
    return arrow::Status::Invalid(
        "BuilderVisitor GetResult Failed, expr_visitor does not be made.");
  }
  *out = expr_visitor_;
  return arrow::Status::OK();
}

//////////////////////// ExprVisitor ////////////////////////
arrow::Status ExprVisitor::Make(std::shared_ptr<arrow::Schema> schema_ptr,
                                std::string func_name,
                                std::vector<std::string> param_field_names,
                                std::shared_ptr<ExprVisitor> dependency,
                                std::shared_ptr<gandiva::Node> finish_func,
                                std::shared_ptr<ExprVisitor>* out) {
  auto expr = std::make_shared<ExprVisitor>(schema_ptr, func_name, param_field_names,
                                            dependency, finish_func);
  RETURN_NOT_OK(expr->MakeExprVisitorImpl(func_name, expr.get()));
  *out = expr;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Make(const std::shared_ptr<gandiva::FunctionNode>& node,
                                std::shared_ptr<arrow::Schema> schema_ptr,
                                std::vector<std::shared_ptr<arrow::Field>> ret_fields,
                                std::shared_ptr<ExprVisitor>* out) {
  auto func_name = node->descriptor()->name();
  *out = std::make_shared<ExprVisitor>(func_name);
  if (func_name.compare(0, 17, "wholestagecodegen") == 0) {
    auto function_node =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(node->children()[0]);
    RETURN_NOT_OK((*out)->MakeExprVisitorImpl(
        func_name, function_node, schema_ptr->fields(), ret_fields, (*out).get()));
  } else if (func_name.compare("standalone") == 0) {
    auto function_node =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(node->children()[0]);
    RETURN_NOT_OK((*out)->MakeExprVisitorImpl(
        func_name, function_node, schema_ptr->fields(), ret_fields, (*out).get()));
  } else if (func_name.compare("codegen_withOneInput") == 0) {
    auto children = node->children();
    if (children.size() != 2) {
      return arrow::Status::Invalid("codegen_withOneInput expects three arguments");
    }
    // first child is a function
    auto codegen_func_node =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[0]);
    // second child is left_kernel_schema
    std::vector<std::shared_ptr<arrow::Field>> field_list;
    auto func_node = std::dynamic_pointer_cast<gandiva::FunctionNode>(children[1]);
    for (auto field : func_node->children()) {
      auto field_node = std::dynamic_pointer_cast<gandiva::FieldNode>(field);
      field_list.push_back(field_node->field());
    }
    RETURN_NOT_OK((*out)->MakeExprVisitorImpl(codegen_func_node->descriptor()->name(),
                                              codegen_func_node, field_list, ret_fields,
                                              (*out).get()));
  } else if (func_name.compare("codegen_withTwoInputs") == 0) {
    auto children = node->children();
    if (children.size() != 3) {
      return arrow::Status::Invalid("codegen_withTwoInputs expects three arguments");
    }
    // first child is a function
    auto codegen_func_node =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(children[0]);
    // second child is left_kernel_schema
    std::vector<std::shared_ptr<arrow::Field>> left_field_list;
    auto left_func_node = std::dynamic_pointer_cast<gandiva::FunctionNode>(children[1]);
    for (auto field : left_func_node->children()) {
      auto field_node = std::dynamic_pointer_cast<gandiva::FieldNode>(field);
      left_field_list.push_back(field_node->field());
    }
    // third child is right_kernel_schema
    std::vector<std::shared_ptr<arrow::Field>> right_field_list;
    auto right_func_node = std::dynamic_pointer_cast<gandiva::FunctionNode>(children[2]);
    for (auto field : right_func_node->children()) {
      auto field_node = std::dynamic_pointer_cast<gandiva::FieldNode>(field);
      right_field_list.push_back(field_node->field());
    }
    RETURN_NOT_OK((*out)->MakeExprVisitorImpl(
        codegen_func_node->descriptor()->name(), codegen_func_node, left_field_list,
        right_field_list, ret_fields, (*out).get()));
  }
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::MakeWindow(
    std::shared_ptr<arrow::Schema> schema_ptr,
    std::vector<std::shared_ptr<arrow::Field>> ret_fields,
    const gandiva::FunctionNode& node, std::shared_ptr<ExprVisitor>* out) {
  auto func_name = node.descriptor()->name();
  if (func_name != "window") {
    return arrow::Status::Invalid("window's Gandiva function name mismatch");
  }
  *out = std::make_shared<ExprVisitor>(schema_ptr, func_name);
  std::vector<std::shared_ptr<gandiva::FunctionNode>> window_functions;
  std::shared_ptr<gandiva::FunctionNode> partition_spec;
  std::shared_ptr<gandiva::FunctionNode> order_spec;
  std::shared_ptr<gandiva::FunctionNode> frame_spec;

  for (const auto& child : node.children()) {
    auto child_function = std::dynamic_pointer_cast<gandiva::FunctionNode>(child);
    auto child_func_name = child_function->descriptor()->name();
    if (child_func_name == "sum" || child_func_name == "avg" ||
        child_func_name == "rank_asc" || child_func_name == "rank_desc") {
      window_functions.push_back(child_function);
    } else if (child_func_name == "partitionSpec") {
      partition_spec = child_function;
    } else if (child_func_name == "orderSpec") {
      order_spec = child_function;
    } else if (child_func_name == "frameSpec") {
      frame_spec = child_function;
    } else {
      return arrow::Status::Invalid("unsupported child function name in window: " +
                                    child_func_name);
    }
  }

  if (window_functions.empty()) {
    return arrow::Status::Invalid("no available function found in window");
  }
  RETURN_NOT_OK((*out)->MakeExprVisitorImpl(func_name, window_functions, partition_spec,
                                            order_spec, frame_spec, ret_fields,
                                            (*out).get()));
  return arrow::Status::OK();
}

ExprVisitor::ExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr, std::string func_name,
                         std::vector<std::string> param_field_names,
                         std::shared_ptr<ExprVisitor> dependency,
                         std::shared_ptr<gandiva::Node> finish_func)
    : schema_(schema_ptr), func_name_(func_name), param_field_names_(param_field_names) {
  if (dependency) {
    dependency_ = dependency;
  }
  if (finish_func) {
    finish_func_ = finish_func;
  }
}

ExprVisitor::ExprVisitor(std::string func_name) : func_name_(func_name) {}

ExprVisitor::ExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr, std::string func_name)
    : schema_(schema_ptr), func_name_(func_name) {}

arrow::Status ExprVisitor::MakeExprVisitorImpl(
    const std::string& func_name, std::shared_ptr<gandiva::FunctionNode> func_node,
    std::vector<std::shared_ptr<arrow::Field>> field_list,
    std::vector<std::shared_ptr<arrow::Field>> ret_fields, ExprVisitor* p) {
  if (func_name.compare("hashAggregateArrays") == 0) {
    RETURN_NOT_OK(HashAggregateArraysVisitorImpl::Make(field_list, func_node->children(),
                                                       ret_fields, p, &impl_));
    goto finish;
  } else if (func_name.compare(0, 17, "wholestagecodegen") == 0) {
    RETURN_NOT_OK(
        WholeStageCodeGenVisitorImpl::Make(field_list, func_node, ret_fields, p, &impl_));
    goto finish;
  } else if (func_name.compare("standalone") == 0) {
    auto child_func_name = func_node->descriptor()->name();
    if (child_func_name.compare(0, 22, "conditionedProbeArrays") == 0) {
      RETURN_NOT_OK(ConditionedProbeArraysVisitorImpl::Make(field_list, func_node,
                                                            ret_fields, p, &impl_));
    } else if (child_func_name.compare("HashRelation") == 0) {
      RETURN_NOT_OK(
          HashRelationVisitorImpl::Make(field_list, func_node, ret_fields, p, &impl_));
    } else if (child_func_name.compare("sortArraysToIndices") == 0) {
      RETURN_NOT_OK(SortArraysToIndicesVisitorImpl::Make(field_list, func_node,
                                                         ret_fields, p, &impl_));
    } else if (child_func_name.compare("ConcatArrayList") == 0) {
      RETURN_NOT_OK(
          ConcatArrayListVisitorImpl::Make(field_list, func_node, ret_fields, p, &impl_));
    }
    goto finish;
  }
finish:
  return arrow::Status::OK();

unrecognizedFail:
  return arrow::Status::NotImplemented("Function name ", func_name,
                                       " is not implemented yet.");
}
arrow::Status ExprVisitor::MakeExprVisitorImpl(
    const std::string& func_name, std::shared_ptr<gandiva::FunctionNode> func_node,
    std::vector<std::shared_ptr<arrow::Field>> left_field_list,
    std::vector<std::shared_ptr<arrow::Field>> right_field_list,
    std::vector<std::shared_ptr<arrow::Field>> ret_fields, ExprVisitor* p) {
  if (func_name.compare("conditionedProbeArraysInner") == 0 ||
      func_name.compare("conditionedProbeArraysOuter") == 0 ||
      func_name.compare("conditionedProbeArraysAnti") == 0 ||
      func_name.compare("conditionedProbeArraysSemi") == 0 ||
      func_name.compare("conditionedProbeArraysExistence") == 0) {
    // first child is left_key_schema
    std::vector<std::shared_ptr<arrow::Field>> left_key_list;
    auto left_func_node =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(func_node->children()[0]);
    for (auto field : left_func_node->children()) {
      auto field_node = std::dynamic_pointer_cast<gandiva::FieldNode>(field);
      left_key_list.push_back(field_node->field());
    }
    // second child is right_key_schema
    std::vector<std::shared_ptr<arrow::Field>> right_key_list;
    auto right_func_node =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(func_node->children()[1]);
    for (auto field : right_func_node->children()) {
      auto field_node = std::dynamic_pointer_cast<gandiva::FieldNode>(field);
      right_key_list.push_back(field_node->field());
    }
    // if there is third child, it should be condition
    std::shared_ptr<gandiva::Node> condition_node;
    if (func_node->children().size() > 2) {
      condition_node = func_node->children()[2];
    }
    int join_type = 0;
    if (func_name.compare("conditionedProbeArraysInner") == 0) {
      join_type = 0;
    } else if (func_name.compare("conditionedProbeArraysOuter") == 0) {
      join_type = 1;
    } else if (func_name.compare("conditionedProbeArraysAnti") == 0) {
      join_type = 2;
    } else if (func_name.compare("conditionedProbeArraysSemi") == 0) {
      join_type = 3;
    } else if (func_name.compare("conditionedProbeArraysExistence") == 0) {
      join_type = 4;
    }
    RETURN_NOT_OK(CodegenProbeArraysVisitorImpl::Make(
        left_key_list, right_key_list, condition_node, join_type, left_field_list,
        right_field_list, ret_fields, p, &impl_));
    goto finish;
  }
  if (func_name.compare("conditionedJoinArraysInner") == 0 ||
      func_name.compare("conditionedJoinArraysOuter") == 0 ||
      func_name.compare("conditionedJoinArraysAnti") == 0 ||
      func_name.compare("conditionedJoinArraysSemi") == 0) {
    // first child is left_key_schema
    std::vector<std::shared_ptr<arrow::Field>> left_key_list;
    auto left_func_node =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(func_node->children()[0]);
    for (auto field : left_func_node->children()) {
      auto field_node = std::dynamic_pointer_cast<gandiva::FieldNode>(field);
      left_key_list.push_back(field_node->field());
    }
    // second child is right_key_schema
    std::vector<std::shared_ptr<arrow::Field>> right_key_list;
    auto right_func_node =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(func_node->children()[1]);
    for (auto field : right_func_node->children()) {
      auto field_node = std::dynamic_pointer_cast<gandiva::FieldNode>(field);
      right_key_list.push_back(field_node->field());
    }
    // if there is third child, it should be condition
    std::shared_ptr<gandiva::Node> condition_node;
    if (func_node->children().size() > 2) {
      condition_node = func_node->children()[2];
    }
    int join_type = 0;
    if (func_name.compare("conditionedJoinArraysInner") == 0) {
      join_type = 0;
    } else if (func_name.compare("conditionedJoinArraysOuter") == 0) {
      join_type = 1;
    } else if (func_name.compare("conditionedJoinArraysAnti") == 0) {
      join_type = 2;
    } else if (func_name.compare("conditionedJoinArraysSemi") == 0) {
      join_type = 3;
    }
    RETURN_NOT_OK(ConditionedJoinArraysVisitorImpl::Make(
        left_key_list, right_key_list, condition_node, join_type, left_field_list,
        right_field_list, ret_fields, p, &impl_));
    goto finish;
  }

finish:
  return arrow::Status::OK();

unrecognizedFail:
  return arrow::Status::NotImplemented("Function name ", func_name,
                                       " is not implemented yet.");
}

arrow::Status ExprVisitor::MakeExprVisitorImpl(const std::string& func_name,
                                               ExprVisitor* p) {
  if (func_name.compare("splitArrayListWithAction") == 0) {
    RETURN_NOT_OK(SplitArrayListWithActionVisitorImpl::Make(p, &impl_));
    goto finish;
  }
  if (func_name.compare("sum") == 0 || func_name.compare("count") == 0 ||
      func_name.compare("unique") == 0 || func_name.compare("append") == 0 ||
      func_name.compare("sum_count") == 0 || func_name.compare("avgByCount") == 0 ||
      func_name.compare("min") == 0 || func_name.compare("max") == 0 ||
      func_name.compare("stddev_samp_partial") == 0 ||
      func_name.compare("stddev_samp_final") == 0 ||
      func_name.compare("sum_count_merge") == 0) {
    RETURN_NOT_OK(AggregateVisitorImpl::Make(p, func_name, &impl_));
    goto finish;
  }
  if (func_name.compare("encodeArray") == 0) {
    RETURN_NOT_OK(EncodeVisitorImpl::Make(p, &impl_));
    goto finish;
  }
  goto unrecognizedFail;
finish:
  return arrow::Status::OK();

unrecognizedFail:
  return arrow::Status::NotImplemented("Function name ", func_name,
                                       " is not implemented yet.");
}

arrow::Status ExprVisitor::MakeExprVisitorImpl(
    const std::string& func_name,
    std::vector<std::shared_ptr<gandiva::FunctionNode>> window_functions,
    std::shared_ptr<gandiva::FunctionNode> partition_spec,
    std::shared_ptr<gandiva::FunctionNode> order_spec,
    std::shared_ptr<gandiva::FunctionNode> frame_spec,
    std::vector<std::shared_ptr<arrow::Field>> ret_fields, ExprVisitor* p) {
  std::vector<std::string> window_function_names;
  std::vector<std::vector<gandiva::FieldPtr>> function_param_fields;
  for (auto window_function : window_functions) {
    std::string window_function_name = window_function->descriptor()->name();
    std::vector<gandiva::FieldPtr> function_param_fields_of_each;
    for (std::shared_ptr<gandiva::Node> child : window_function->children()) {
      std::shared_ptr<gandiva::FieldNode> field =
          std::dynamic_pointer_cast<gandiva::FieldNode>(child);
      function_param_fields_of_each.push_back(field->field());
    }
    window_function_names.push_back(window_function_name);
    function_param_fields.push_back(function_param_fields_of_each);
  }
  std::vector<gandiva::FieldPtr> partition_fields;
  for (std::shared_ptr<gandiva::Node> child : partition_spec->children()) {
    std::shared_ptr<gandiva::FieldNode> field =
        std::dynamic_pointer_cast<gandiva::FieldNode>(child);
    partition_fields.push_back(field->field());
  }
  std::vector<std::shared_ptr<arrow::DataType>> return_types;
  for (auto return_field : ret_fields) {
    std::shared_ptr<arrow::DataType> type = return_field->type();
    return_types.push_back(type);
  }
  // todo order_spec frame_spec
  RETURN_NOT_OK(WindowVisitorImpl::Make(p, window_function_names, return_types,
                                        function_param_fields, partition_fields, &impl_));
  return arrow::Status();
}

arrow::Status ExprVisitor::AppendAction(const std::string& func_name,
                                        std::vector<std::string> param_name) {
  action_name_list_.push_back(func_name);
  for (auto name : param_name) {
    action_param_list_.push_back(name);
  }
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::SetMember(const std::shared_ptr<arrow::RecordBatch>& ms) {
#ifdef DEBUG_LEVEL_2
  std::cout << typeid(*this).name() << __func__ << "memberset: " << ms << std::endl;
#endif
  member_record_batch_ = ms;
  impl_->SetMember();
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::SetDependency(
    const std::shared_ptr<ResultIterator<arrow::RecordBatch>>& dependency_iter,
    int index) {
  RETURN_NOT_OK(impl_->SetDependency(dependency_iter, index));
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Eval(const std::shared_ptr<arrow::Array>& selection_in,
                                const std::shared_ptr<arrow::RecordBatch>& in) {
  in_selection_array_ = selection_in;
  in_record_batch_ = in;
  RETURN_NOT_OK(Eval());
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Eval(const std::shared_ptr<arrow::RecordBatch>& in) {
  in_record_batch_ = in;
  RETURN_NOT_OK(Eval());
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Eval() {
  if (return_type_ != ArrowComputeResultType::None) {
#ifdef DEBUG_LEVEL_2
    std::cout << "ExprVisitor::Eval " << func_name_ << ", ptr " << this
              << ", already evaluated, skip" << std::endl;
#endif
    return arrow::Status::OK();
  }
#ifdef DEBUG_LEVEL_2
  std::cout << "ExprVisitor::Eval " << func_name_ << ", ptr " << this
            << ", start to check dependency" << std::endl;
#endif
  if (dependency_) {
    // if this visitor has dependency, we need to get dependency result firstly.
    if (in_selection_array_) {
      RETURN_NOT_OK(dependency_->Eval(in_selection_array_, in_record_batch_));
    } else {
      RETURN_NOT_OK(dependency_->Eval(in_record_batch_));
    }
    RETURN_NOT_OK(GetResultFromDependency());
  }
#ifdef DEBUG_LEVEL_2
  std::cout << "ExprVisitor::Eval " << func_name_ << ", ptr " << this
            << ", start to execute" << std::endl;
#endif
  // now we has dependeny result as this visitor's input.
  RETURN_NOT_OK(impl_->Eval());
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResultFromDependency() {
  if (dependency_ && dependency_result_type_ == ArrowComputeResultType::None) {
    // if this visitor has dependency, we need to get dependency result firstly.
    dependency_result_type_ = dependency_->GetResultType();
    switch (dependency_result_type_) {
      case ArrowComputeResultType::BatchList: {
        RETURN_NOT_OK(dependency_->GetResult(&in_batch_array_, &in_batch_size_array_,
                                             &in_fields_, &group_indices_));
      } break;
      case ArrowComputeResultType::Batch: {
        RETURN_NOT_OK(dependency_->GetResult(&in_batch_, &in_fields_, &group_indices_));
      } break;
      case ArrowComputeResultType::Array: {
        RETURN_NOT_OK(dependency_->GetResult(&in_array_, &in_fields_, &group_indices_));
      } break;
      case ArrowComputeResultType::None: {
      } break;
      default:
        return arrow::Status::Invalid("ArrowComputeResultType is invalid.");
    }
  }
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::ResetDependency() {
  if (dependency_) {
    RETURN_NOT_OK(dependency_->Reset());
  }
  switch (dependency_result_type_) {
    case ArrowComputeResultType::Array: {
      // in_array_.reset();
    } break;
    case ArrowComputeResultType::Batch: {
      in_batch_.clear();
    } break;
    case ArrowComputeResultType::BatchList: {
      in_batch_array_.clear();
      in_batch_size_array_.clear();
    } break;
    default:
      break;
  }
  in_fields_.clear();
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Reset() {
  RETURN_NOT_OK(ResetDependency());
  switch (return_type_) {
    case ArrowComputeResultType::Array: {
      // result_array_.reset();
    } break;
    case ArrowComputeResultType::Batch: {
      result_batch_.clear();
    } break;
    case ArrowComputeResultType::BatchList: {
      result_batch_list_.clear();
      result_batch_size_list_.clear();
    } break;
    default:
      break;
  }
  result_fields_.clear();
#ifdef DEBUG
  std::cout << "ExprVisitor::Reset " << func_name_ << " ,ptr is " << this << std::endl;
#endif
  return_type_ = ArrowComputeResultType::None;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Init() {
  if (initialized_) {
    return arrow::Status::OK();
  }
  if (dependency_) {
    RETURN_NOT_OK(dependency_->Init());
  }
#ifdef DEBUG
  std::cout << "ExprVisitor::Init " << func_name_ << " ,ptr is " << this << std::endl;
#endif
  RETURN_NOT_OK(impl_->Init());
  initialized_ = true;
  if (finish_func_) {
    std::string finish_func_name =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(finish_func_)
            ->descriptor()
            ->name();
    RETURN_NOT_OK(ExprVisitor::Make(schema_, finish_func_name, param_field_names_,
                                    shared_from_this(), nullptr, &finish_visitor_));
    RETURN_NOT_OK(finish_visitor_->Init());
  }
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Finish(std::shared_ptr<ExprVisitor>* finish_visitor) {
  if (return_type_ != ArrowComputeResultType::None) {
    return arrow::Status::OK();
  }
  if (dependency_) {
    std::shared_ptr<ExprVisitor> dummy;
    RETURN_NOT_OK(dependency_->Finish(&dummy));
    RETURN_NOT_OK(GetResultFromDependency());
  }
  RETURN_NOT_OK(impl_->Finish());
  if (finish_visitor_) {
    RETURN_NOT_OK(finish_visitor_->Eval());
    std::shared_ptr<ExprVisitor> dummy;
    RETURN_NOT_OK(finish_visitor_->Finish(&dummy));
    *finish_visitor = finish_visitor_;
  }
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                                              std::shared_ptr<ResultIteratorBase>* out) {
  if (dependency_) {
    std::shared_ptr<ExprVisitor> dummy;
    RETURN_NOT_OK(dependency_->Finish(&dummy));
    RETURN_NOT_OK(GetResultFromDependency());
  }
  if (!finish_visitor_) {
    RETURN_NOT_OK(impl_->MakeResultIterator(schema, out));
  } else {
    return arrow::Status::NotImplemented(
        "FinishVsitor MakeResultIterator is not tested, so mark as not implemented "
        "here, "
        "codes are commented.");
  }
  return arrow::Status::OK();
}

ArrowComputeResultType ExprVisitor::GetResultType() { return return_type_; }

arrow::Status ExprVisitor::GetResult(
    std::shared_ptr<arrow::Array>* out,
    std::vector<std::shared_ptr<arrow::Field>>* out_fields) {
  if (!result_array_) {
    return arrow::Status::Invalid(
        "ArrowComputeExprVisitor::GetResult result_array was not generated ", func_name_);
  }
  *out = result_array_;
  *out_fields = result_fields_;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    std::vector<ArrayList>* out, std::vector<int>* out_sizes,
    std::vector<std::shared_ptr<arrow::Field>>* out_fields) {
  if (result_batch_list_.empty()) {
    return arrow::Status::Invalid(
        "ArrowComputeExprVisitor::GetResult result_batch_list was not generated ",
        func_name_);
  }
  *out = result_batch_list_;
  *out_sizes = result_batch_size_list_;
  *out_fields = result_fields_;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    ArrayList* out, std::vector<std::shared_ptr<arrow::Field>>* out_fields) {
  if (result_batch_.empty()) {
    return arrow::Status::Invalid(
        "ArrowComputeExprVisitor::GetResult result_batch was not generated ", func_name_);
  }
  for (auto arr : result_batch_) {
    out->push_back(arr);
  }
  for (auto field : result_fields_) {
    out_fields->push_back(field);
  }
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    std::shared_ptr<arrow::Array>* out,
    std::vector<std::shared_ptr<arrow::Field>>* out_fields,
    std::vector<int>* group_indices) {
  RETURN_NOT_OK(GetResult(out, out_fields));
  *group_indices = group_indices_;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    std::vector<ArrayList>* out, std::vector<int>* out_sizes,
    std::vector<std::shared_ptr<arrow::Field>>* out_fields,
    std::vector<int>* group_indices) {
  RETURN_NOT_OK(GetResult(out, out_sizes, out_fields));
  *group_indices = group_indices_;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    ArrayList* out, std::vector<std::shared_ptr<arrow::Field>>* out_fields,
    std::vector<int>* group_indices) {
  RETURN_NOT_OK(GetResult(out, out_fields));
  *group_indices = group_indices_;
  return arrow::Status::OK();
}

}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
