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

#include <arrow/array.h>
#include <gtest/gtest.h>

#include <memory>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

TEST(TestArrowCompute, GroupByHashAggregateTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f_unique = field("unique", uint32());
  auto f_sum = field("sum", uint64());
  auto f_count = field("count", int64());
  auto f_avg = field("avg", float64());
  auto f_min = field("min", uint32());
  auto f_max = field("max", uint32());
  auto f_res = field("res", uint32());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto n_groupby = TreeExprBuilder::MakeFunction("action_groupby", {arg0}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {arg1}, uint32());
  auto n_count = TreeExprBuilder::MakeFunction("action_count", {arg1}, uint32());
  auto n_avg = TreeExprBuilder::MakeFunction("action_avg", {arg1}, uint32());
  auto n_sum_count = TreeExprBuilder::MakeFunction("action_sum_count", {arg1}, uint32());
  auto n_min = TreeExprBuilder::MakeFunction("action_min", {arg1}, uint32());
  auto n_max = TreeExprBuilder::MakeFunction("action_max", {arg1}, uint32());
  auto n_schema = TreeExprBuilder::MakeFunction(
      "codegen_schema", {TreeExprBuilder::MakeField(f0), TreeExprBuilder::MakeField(f1)},
      uint32());
  auto n_aggr = TreeExprBuilder::MakeFunction(
      "hashAggregateArrays",
      {n_groupby, n_sum, n_count, n_avg, n_sum_count, n_min, n_max}, uint32());
  auto n_codegen_aggr =
      TreeExprBuilder::MakeFunction("codegen_withOneInput", {n_aggr, n_schema}, uint32());

  auto aggr_expr = TreeExprBuilder::MakeExpression(n_codegen_aggr, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {aggr_expr};

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_sum,   f_count, f_avg,
                                                   f_sum,    f_count, f_min,   f_max};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      "[1, 2, 3, 4, 5, null, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, null, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::shared_ptr<arrow::RecordBatch> result_batch;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> aggr_result_iterator;
  ASSERT_NOT_OK(expr->finish(&aggr_result_iterator));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 5, 6, 7, 8 ,9, 10]",        "[8, 10, 9, 20, 45, 42, 28, 32, 54, 70]",
      "[8, 5, 3, 5, 9, 7, 4, 4, 6, 7]",         "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]",
      "[8, 10, 9, 20, 45, 42, 28, 32, 54, 70]", "[8, 5, 3, 5, 9, 7, 4, 4, 6, 7]",
      "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]",        "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"};
  auto res_sch =
      arrow::schema({f_unique, f_sum, f_count, f_avg, f_sum, f_count, f_min, f_max});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  if (aggr_result_iterator->HasNext()) {
    ASSERT_NOT_OK(aggr_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, GroupByHashAggregateWithStringTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f_unique = field("unique", utf8());
  auto f_sum = field("sum", float64());
  auto f_count = field("count", int64());
  auto f_avg = field("avg", float64());
  auto f_res = field("dummy_res", uint32());

  auto arg_unique = TreeExprBuilder::MakeField(f_unique);
  auto arg_sum = TreeExprBuilder::MakeField(f_sum);
  auto arg_count = TreeExprBuilder::MakeField(f_count);
  auto n_groupby =
      TreeExprBuilder::MakeFunction("action_groupby", {arg_unique}, uint32());
  auto n_avg =
      TreeExprBuilder::MakeFunction("action_avgByCount", {arg_sum, arg_count}, uint32());
  auto n_schema = TreeExprBuilder::MakeFunction(
      "codegen_schema", {arg_unique, arg_sum, arg_count}, uint32());
  auto n_aggr =
      TreeExprBuilder::MakeFunction("hashAggregateArrays", {n_groupby, n_avg}, uint32());
  auto n_codegen_aggr =
      TreeExprBuilder::MakeFunction("codegen_withOneInput", {n_aggr, n_schema}, uint32());

  auto aggr_expr = TreeExprBuilder::MakeExpression(n_codegen_aggr, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {aggr_expr};

  auto sch = arrow::schema({f_unique, f_sum, f_count});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_avg};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ",
"HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])",
      "[1, 4, 9, 16, 25, 25, 16, 1, 4, 4, 1, 1, 1, 16, 16, 9, 25, 25, 25, 25]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "CD", "DL", "DL", "CD", "CD", "CD",
"LA", "LA", "NY", "AU", "AU", "AU", "AU"])",
      "[36, 49, 64, 81, 100, 100, 81, 36, 49, 49, 36, 36, 36, 81, 81, 64, 100, 100, 100, "
      "100]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      R"(["BJ", "SH", "SZ", "NY", "WH", "WH", "AU", "BJ", "SH", "DL", "CD", "CD", "BJ",
"LA", "HZ", "LA", "WH", "NY", "WH", "WH"])",
      "[1, 4, 9, 64, 25, 25, 100, 1, 4, 49, 36, 36, 1, 81, 16, 81, 25, 64, 25, 25]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::shared_ptr<arrow::RecordBatch> result_batch;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> aggr_result_iterator;
  ASSERT_NOT_OK(expr->finish(&aggr_result_iterator));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY" ,"LA", "AU"])",
      "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"};
  auto res_sch = arrow::schema({f_unique, f_avg});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  if (aggr_result_iterator->HasNext()) {
    ASSERT_NOT_OK(aggr_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, GroupByHashAggregateWithProjectionTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f_0 = field("f0", utf8());
  auto f_1 = field("f1", float64());
  auto f_unique = field("unique", utf8());
  auto f_max = field("max", float64());
  auto f_res = field("dummy_res", uint32());

  auto arg_0 = TreeExprBuilder::MakeField(f_0);
  auto arg_1 = TreeExprBuilder::MakeField(f_1);
  auto arg_unique = TreeExprBuilder::MakeField(f_unique);
  auto arg_max = TreeExprBuilder::MakeField(f_max);
  auto n_groupby = TreeExprBuilder::MakeFunction("action_groupby", {arg_0}, uint32());
  auto n_projection = TreeExprBuilder::MakeFunction(
      "multiply", {arg_1, TreeExprBuilder::MakeLiteral(0.3)}, float64());
  auto n_max = TreeExprBuilder::MakeFunction("action_max", {n_projection}, uint32());
  auto n_schema =
      TreeExprBuilder::MakeFunction("codegen_schema", {arg_0, arg_1}, uint32());
  auto n_aggr =
      TreeExprBuilder::MakeFunction("hashAggregateArrays", {n_groupby, n_max}, uint32());
  auto n_codegen_aggr =
      TreeExprBuilder::MakeFunction("codegen_withOneInput", {n_aggr, n_schema}, uint32());

  auto aggr_expr = TreeExprBuilder::MakeExpression(n_codegen_aggr, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {aggr_expr};

  auto sch = arrow::schema({f_0, f_1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_max};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ",
"HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])",
      "[1, 4, 9, 16, 25, 25, 16, 1, 3, 5, 1, 1, 1, 16, 16, 9, 25, 25, 25, 25]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "CD", "DL", "DL", "CD", "CD", "CD",
"LA", "LA", "NY", "AU", "AU", "AU", "AU"])",
      "[36, 49, 64, 81, 100, 100, 81, 36, 49, 49, 36, 36, 36, 81, 81, 64, 100, 100, 100, "
      "100]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      R"(["BJ", "SH", "SZ", "NY", "WH", "WH", "AU", "BJ", "SH", "DL", "CD", "CD", "BJ",
"LA", "HZ", "LA", "WH", "NY", "WH", "WH"])",
      "[1, 4, 9, 64, 25, 25, 100, 1, 4, 49, 36, 36, 1, 81, 16, 81, 25, 64, 25, 25]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::shared_ptr<arrow::RecordBatch> result_batch;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> aggr_result_iterator;
  ASSERT_NOT_OK(expr->finish(&aggr_result_iterator));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY" ,"LA", "AU"])",
      "[0.3, 1.5, 2.7, 4.8, 7.5, 10.8, 14.7, 19.2, 24.3, 30]"};
  auto res_sch = arrow::schema(ret_types);
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  if (aggr_result_iterator->HasNext()) {
    ASSERT_NOT_OK(aggr_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, GroupByHashAggregateWithCaseWhenTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f_0 = field("f0", utf8());
  auto f_1 = field("f1", float64());
  auto f_unique = field("unique", utf8());
  auto f_sum = field("sum", float64());
  auto f_res = field("dummy_res", uint32());

  auto arg_0 = TreeExprBuilder::MakeField(f_0);
  auto arg_1 = TreeExprBuilder::MakeField(f_1);
  auto arg_unique = TreeExprBuilder::MakeField(f_unique);
  auto arg_sum = TreeExprBuilder::MakeField(f_sum);
  auto n_groupby = TreeExprBuilder::MakeFunction("action_groupby", {arg_0}, uint32());

  auto n_when = TreeExprBuilder::MakeFunction(
      "equal", {arg_0, TreeExprBuilder::MakeStringLiteral("BJ")}, arrow::boolean());
  auto n_then = TreeExprBuilder::MakeFunction(
      "multiply", {arg_1, TreeExprBuilder::MakeLiteral((double)0.3)}, float64());
  auto n_else = TreeExprBuilder::MakeFunction(
      "multiply", {arg_1, TreeExprBuilder::MakeLiteral((double)1.3)}, float64());
  auto n_projection = TreeExprBuilder::MakeIf(n_when, n_then, n_else, float64());

  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {n_projection}, uint32());
  auto n_schema =
      TreeExprBuilder::MakeFunction("codegen_schema", {arg_0, arg_1}, uint32());
  auto n_aggr =
      TreeExprBuilder::MakeFunction("hashAggregateArrays", {n_groupby, n_sum}, uint32());
  auto n_codegen_aggr =
      TreeExprBuilder::MakeFunction("codegen_withOneInput", {n_aggr, n_schema}, uint32());

  auto aggr_expr = TreeExprBuilder::MakeExpression(n_codegen_aggr, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {aggr_expr};

  auto sch = arrow::schema({f_0, f_1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_sum};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ",
"HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])",
      "[1, 4, 9, 16, 25, 25, 16, 1, 3, 5, 1, 1, 1, 16, 16, 9, 25, 25, 25, 25]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "CD", "DL", "DL", "CD", "CD", "CD",
"LA", "LA", "NY", "AU", "AU", "AU", "AU"])",
      "[36, 49, 64, 81, 100, 100, 81, 36, 49, 49, 36, 36, 36, 81, 81, 64, 100, 100, 100, "
      "100]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      R"(["BJ", "SH", "SZ", "NY", "WH", "WH", "AU", "BJ", "SH", "DL", "CD", "CD", "BJ",
"LA", "HZ", "LA", "WH", "NY", "WH", "WH"])",
      "[1, 4, 9, 64, 25, 25, 100, 1, 4, 49, 36, 36, 1, 81, 16, 81, 25, 64, 25, 25]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::shared_ptr<arrow::RecordBatch> result_batch;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> aggr_result_iterator;
  ASSERT_NOT_OK(expr->finish(&aggr_result_iterator));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY" ,"LA", "AU"])",
      "[2.4, 26, 35.1, 104, 357.5,  327.6, 254.8, 332.8, 631.8, 910]"};
  auto res_sch = arrow::schema(ret_types);
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  if (aggr_result_iterator->HasNext()) {
    ASSERT_NOT_OK(aggr_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, GroupByHashAggregateWithNoKeyTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f_unique = field("unique", utf8());
  auto f_sum = field("sum", float64());
  auto f_count = field("count", int64());
  auto f_avg = field("avg", float64());
  auto f_res = field("dummy_res", uint32());

  auto arg_unique = TreeExprBuilder::MakeField(f_unique);
  auto arg_sum = TreeExprBuilder::MakeField(f_sum);
  auto arg_count = TreeExprBuilder::MakeField(f_count);
  auto n_groupby =
      TreeExprBuilder::MakeFunction("action_groupby_no_keep", {arg_unique}, uint32());
  auto n_avg =
      TreeExprBuilder::MakeFunction("action_avgByCount", {arg_sum, arg_count}, uint32());
  auto n_schema = TreeExprBuilder::MakeFunction(
      "codegen_schema", {arg_unique, arg_sum, arg_count}, uint32());
  auto n_aggr =
      TreeExprBuilder::MakeFunction("hashAggregateArrays", {n_groupby, n_avg}, uint32());
  auto n_codegen_aggr =
      TreeExprBuilder::MakeFunction("codegen_withOneInput", {n_aggr, n_schema}, uint32());

  auto aggr_expr = TreeExprBuilder::MakeExpression(n_codegen_aggr, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {aggr_expr};

  auto sch = arrow::schema({f_unique, f_sum, f_count});
  std::vector<std::shared_ptr<Field>> ret_types = {f_avg};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ",
"HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])",
      "[1, 4, 9, 16, 25, 25, 16, 1, 4, 4, 1, 1, 1, 16, 16, 9, 25, 25, 25, 25]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "CD", "DL", "DL", "CD", "CD", "CD",
"LA", "LA", "NY", "AU", "AU", "AU", "AU"])",
      "[36, 49, 64, 81, 100, 100, 81, 36, 49, 49, 36, 36, 36, 81, 81, 64, 100, 100, 100, "
      "100]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      R"(["BJ", "SH", "SZ", "NY", "WH", "WH", "AU", "BJ", "SH", "DL", "CD", "CD", "BJ",
"LA", "HZ", "LA", "WH", "NY", "WH", "WH"])",
      "[1, 4, 9, 64, 25, 25, 100, 1, 4, 49, 36, 36, 1, 81, 16, 81, 25, 64, 25, 25]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::shared_ptr<arrow::RecordBatch> result_batch;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> aggr_result_iterator;
  ASSERT_NOT_OK(expr->finish(&aggr_result_iterator));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"};
  auto res_sch = arrow::schema(ret_types);
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  if (aggr_result_iterator->HasNext()) {
    ASSERT_NOT_OK(aggr_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, GroupByHashAggregateWithTwoStringTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f_unique_0 = field("unique_0", utf8());
  auto f_unique_1 = field("unique_1", utf8());
  auto f_sum = field("sum", float64());
  auto f_count_all = field("count_all", int64());
  auto f_res = field("dummy_res", uint32());

  auto arg_unique_0 = TreeExprBuilder::MakeField(f_unique_0);
  auto arg_unique_1 = TreeExprBuilder::MakeField(f_unique_1);
  auto arg_sum = TreeExprBuilder::MakeField(f_sum);
  auto n_groupby_0 =
      TreeExprBuilder::MakeFunction("action_groupby", {arg_unique_0}, uint32());
  auto n_groupby_1 =
      TreeExprBuilder::MakeFunction("action_groupby", {arg_unique_1}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {arg_sum}, uint32());
  auto n_count_all = TreeExprBuilder::MakeFunction("action_countLiteral_1", {}, uint32());
  auto n_schema = TreeExprBuilder::MakeFunction(
      "codegen_schema", {arg_unique_0, arg_unique_1, arg_sum}, uint32());
  auto n_aggr = TreeExprBuilder::MakeFunction(
      "hashAggregateArrays", {n_groupby_0, n_groupby_1, n_sum, n_count_all}, uint32());
  auto n_codegen_aggr =
      TreeExprBuilder::MakeFunction("codegen_withOneInput", {n_aggr, n_schema}, uint32());

  auto aggr_expr = TreeExprBuilder::MakeExpression(n_codegen_aggr, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {aggr_expr};

  auto sch = arrow::schema({f_unique_0, f_unique_1, f_sum});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique_0, f_unique_1, f_sum,
                                                   f_count_all};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["a", "b", "c", "d", "e", "e", "d", "a", "b", "b", "a", "a", "a", "d", "d", "c",
"e", "e", "e", "e"])",
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH",
"BJ", "BJ", "BJ", "HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])",
      R"([1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5])"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["f", "g", "h", "i", "j", "j", "i", "g", "h", "i", "g", "g", "g", "j", "i", "f",
"f", "i", "j", "j"])",
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "DL", "NY", "LA",
"DL", "DL", "DL", "AU", "LA", "CD", "CD", "LA", "AU", "AU"])",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::shared_ptr<arrow::RecordBatch> result_batch;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> aggr_result_iterator;
  ASSERT_NOT_OK(expr->finish(&aggr_result_iterator));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])",
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY", "LA", "AU"])",
      "[5, 6, 6, 16, 30, 24, 31, 15, 44, 49]", "[5, 3, 2, 4, 6, 3, 5, 2, 5, 5]"};
  auto res_sch = arrow::schema(ret_types);
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  if (aggr_result_iterator->HasNext()) {
    ASSERT_NOT_OK(aggr_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
