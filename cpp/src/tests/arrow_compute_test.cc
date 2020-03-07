#include <arrow/array.h>
#include <gtest/gtest.h>
#include <memory>
#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

TEST(TestArrowCompute, ProbeTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f_sum = field("append", uint64());
  auto f_res = field("res", uint64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto n_sum = TreeExprBuilder::MakeFunction("probeArray", {arg_0}, uint64());

  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sum_expr};
  auto sch = arrow::schema({f0});
  std::vector<std::shared_ptr<Field>> ret_types = {f_sum};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::shared_ptr<arrow::RecordBatch> member_batch;

  std::vector<std::string> input_data_string = {"[8, 10, 9]"};
  MakeInputBatch(input_data_string, sch, &input_batch);

  input_data_string = {"[8, 2, 3]"};
  MakeInputBatch(input_data_string, sch, &member_batch);
  ASSERT_NOT_OK(expr->SetMember(member_batch));

  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[0, null, null]"};
  auto res_sch = arrow::schema({f_sum});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, TakeTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f_sum = field("append", uint64());
  auto f_res = field("res", uint64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto n_sum = TreeExprBuilder::MakeFunction("takeArray", {arg_0}, uint64());

  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sum_expr};
  auto sch = arrow::schema({f0});
  std::vector<std::shared_ptr<Field>> ret_types = {f_sum};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::shared_ptr<arrow::RecordBatch> member_batch;

  std::vector<std::string> input_data_string = {"[8, 10, 9]"};
  MakeInputBatch(input_data_string, sch, &input_batch);

  input_data_string = {"[0, 2, 2]"};
  MakeInputBatch(input_data_string, sch, &member_batch);
  ASSERT_NOT_OK(expr->SetMember(member_batch));

  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[8, 9, 9]"};
  auto res_sch = arrow::schema({f_sum});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, NTakeTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f_sum = field("append", uint64());
  auto f_res = field("res", uint64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto n_sum = TreeExprBuilder::MakeFunction("ntakeArray", {arg_0}, uint64());

  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sum_expr};
  auto sch = arrow::schema({f0});
  std::vector<std::shared_ptr<Field>> ret_types = {f_sum};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::shared_ptr<arrow::RecordBatch> member_batch;

  std::vector<std::string> input_data_string = {"[8, 10, 9]"};
  MakeInputBatch(input_data_string, sch, &input_batch);

  input_data_string = {"[2, null, 0]"};
  MakeInputBatch(input_data_string, sch, &member_batch);
  ASSERT_NOT_OK(expr->SetMember(member_batch));

  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[8, 9]"};
  auto res_sch = arrow::schema({f_sum});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, JoinTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f_sum = field("append", uint64());
  auto f_res = field("res", uint64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto n_probe = TreeExprBuilder::MakeFunction("probeArray", {arg_0}, uint64());

  auto probe_expr = TreeExprBuilder::MakeExpression(n_probe, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {probe_expr};
  auto sch = arrow::schema({f0});
  std::vector<std::shared_ptr<Field>> ret_types = {f_sum};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::shared_ptr<arrow::RecordBatch> member_batch;

  std::vector<std::string> input_data_string = {"[8, 10, 9]"};
  MakeInputBatch(input_data_string, sch, &input_batch);

  input_data_string = {"[8, 66, 23, 10, 52, 9]"};
  MakeInputBatch(input_data_string, sch, &member_batch);
  ASSERT_NOT_OK(expr->SetMember(member_batch));

  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[0, 3, 5]"};
  auto res_sch = arrow::schema({f_sum});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));

  auto n_take = TreeExprBuilder::MakeFunction("ntakeArray", {arg_0}, uint64());

  auto take_expr = TreeExprBuilder::MakeExpression(n_take, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> take_expr_vector = {take_expr};
  ///////////////////// Calculation //////////////////
  ASSERT_NOT_OK(CreateCodeGenerator(sch, take_expr_vector, ret_types, &expr, true));
  expr->SetMember(result_batch[0]);

  std::vector<std::shared_ptr<arrow::RecordBatch>> take_result_batch;
  ASSERT_NOT_OK(expr->evaluate(input_batch, &take_result_batch));

  ASSERT_NOT_OK(expr->finish(&take_result_batch));

  expected_result_string = {"[8, 10, 9]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(take_result_batch[0]).get()));
}

TEST(TestArrowCompute, AggregatewithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f2 = field("f2", uint64());
  auto f_sum = field("sum", uint64());
  auto f_count = field("count", int32());
  auto f_float = field("float", float64());
  auto f_res = field("res", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto arg_2 = TreeExprBuilder::MakeField(f2);
  auto n_sum = TreeExprBuilder::MakeFunction("sum", {arg_0}, uint64());
  auto n_count = TreeExprBuilder::MakeFunction("count", {arg_0}, uint64());
  auto n_sum_count = TreeExprBuilder::MakeFunction("sum_count", {arg_0}, uint64());
  auto n_avg = TreeExprBuilder::MakeFunction("avgByCount", {arg_2, arg_1}, uint64());
  auto n_min = TreeExprBuilder::MakeFunction("min", {arg_0}, uint64());
  auto n_max = TreeExprBuilder::MakeFunction("max", {arg_0}, uint64());

  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_res);
  auto sum_count_expr = TreeExprBuilder::MakeExpression(n_sum_count, f_res);
  auto avg_expr = TreeExprBuilder::MakeExpression(n_avg, f_res);
  auto min_expr = TreeExprBuilder::MakeExpression(n_min, f_res);
  auto max_expr = TreeExprBuilder::MakeExpression(n_max, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {
      sum_expr, count_expr, sum_count_expr, avg_expr, min_expr, max_expr};
  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {f_sum,   f_count, f_sum, f_count,
                                                   f_float, f_res,   f_res};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  std::vector<std::string> input_data_string = {"[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]",
                                                "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
                                                "[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));
  std::vector<std::string> input_data_2_string = {
      "[8, 10, 9, 20, null, 42, 28, 32, 54, 70]", "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
      "[8, 10, 9, 20, null, 42, 28, 32, 54, 70]"};
  MakeInputBatch(input_data_2_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[601]",   "[19]", "[601]", "[19]",
                                                     "[30.05]", "[8]",  "[70]"};
  auto res_sch = arrow::schema({f_sum, f_count, f_sum, f_count, f_float, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByAggregateWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f_unique = field("unique", uint32());
  auto f_sum = field("sum", uint32());
  auto f_count = field("count", uint32());
  auto f_avg = field("avg", uint32());
  auto f_res = field("res", uint32());

  auto arg_pre = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_pre}, uint32());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto n_split = TreeExprBuilder::MakeFunction("splitArrayListWithAction",
                                               {n_pre, arg0, arg1}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_unique =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {n_split, arg1}, uint32());
  auto n_count = TreeExprBuilder::MakeFunction("action_count", {n_split, arg1}, uint32());
  auto n_avg = TreeExprBuilder::MakeFunction("action_avg", {n_split, arg1}, uint32());
  auto n_sum_count =
      TreeExprBuilder::MakeFunction("action_sum_count", {n_split, arg1}, uint32());
  auto n_min = TreeExprBuilder::MakeFunction("action_min", {n_split, arg1}, uint32());
  auto n_max = TreeExprBuilder::MakeFunction("action_max", {n_split, arg1}, uint32());

  auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_res);
  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_res);
  auto avg_expr = TreeExprBuilder::MakeExpression(n_avg, f_res);
  auto sum_count_expr = TreeExprBuilder::MakeExpression(n_sum_count, f_res);
  auto avg_min = TreeExprBuilder::MakeExpression(n_min, f_res);
  auto avg_max = TreeExprBuilder::MakeExpression(n_max, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {
      unique_expr, sum_expr, count_expr, avg_expr, sum_count_expr, avg_min, avg_max};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_sum,   f_count, f_avg,
                                                   f_sum,    f_count, f_res,   f_res};

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
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 5, 6, 7, 8 ,9, 10]",        "[8, 10, 9, 20, 45, 42, 28, 32, 54, 70]",
      "[8, 5, 3, 5, 9, 7, 4, 4, 6, 7]",         "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]",
      "[8, 10, 9, 20, 45, 42, 28, 32, 54, 70]", "[8, 5, 3, 5, 9, 7, 4, 4, 6, 7]",
      "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]",        "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"};
  auto res_sch =
      arrow::schema({f_unique, f_sum, f_count, f_avg, f_sum, f_count, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByAvgWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", utf8());
  auto f_sum = field("sum", float64());
  auto f_count = field("count", int64());
  auto f_unique = field("unique", utf8());
  auto f_avg = field("avg", float64());
  auto f_res = field("res", uint32());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg0}, utf8());

  auto arg_sum = TreeExprBuilder::MakeField(f_sum);
  auto arg_count = TreeExprBuilder::MakeField(f_count);
  auto n_split = TreeExprBuilder::MakeFunction(
      "splitArrayListWithAction", {n_pre, arg0, arg_sum, arg_count}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_unique = TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, utf8());
  auto n_avg = TreeExprBuilder::MakeFunction("action_avgByCount",
                                             {n_split, arg_sum, arg_count}, uint32());

  auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_res);
  auto avg_expr = TreeExprBuilder::MakeExpression(n_avg, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {unique_expr,
                                                                     avg_expr};
  auto sch = arrow::schema({f0, f_sum, f_count});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_avg};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ", "HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])",
      "[1, 4, 9, 16, 25, 25, 16, 1, 4, 4, 1, 1, 1, 16, 16, 9, 25, 25, 25, 25]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "CD", "DL", "DL", "CD", "CD", "CD", "LA", "LA", "NY", "AU", "AU", "AU", "AU"])",
      "[36, 49, 64, 81, 100, 100, 81, 36, 49, 49, 36, 36, 36, 81, 81, 64, 100, 100, 100, "
      "100]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      R"(["BJ", "SH", "SZ", "NY", "WH", "WH", "AU", "BJ", "SH", "DL", "CD", "CD", "BJ", "LA", "HZ", "LA", "WH", "NY", "WH", "WH"])",
      "[1, 4, 9, 64, 25, 25, 100, 1, 4, 49, 36, 36, 1, 81, 16, 81, 25, 64, 25, 25]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY" ,"LA", "AU"])",
      "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"};
  auto res_sch = arrow::schema({f_unique, f_avg});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByCountAllWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", utf8());
  auto f_unique = field("unique", utf8());
  auto f_count = field("avg", uint64());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg0}, utf8());

  auto n_split =
      TreeExprBuilder::MakeFunction("splitArrayListWithAction", {n_pre, arg0}, uint32());

  auto n_unique = TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, utf8());
  auto n_count =
      TreeExprBuilder::MakeFunction("action_countLiteral_1", {n_split}, uint32());

  auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_unique);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_count);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {unique_expr,
                                                                     count_expr};
  auto sch = arrow::schema({f0, f_count});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_count};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ", "HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "CD", "DL", "DL", "CD", "CD", "CD", "LA", "LA", "NY", "AU", "AU", "AU", "AU"])"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      R"(["BJ", "SH", "SZ", "NY", "WH", "WH", "AU", "BJ", "SH", "DL", "CD", "CD", "BJ", "LA", "HZ", "LA", "WH", "NY", "WH", "WH"])"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY" ,"LA", "AU"])",
      "[8, 5, 3, 5, 11, 7, 4, 4, 6, 7]"};
  auto res_sch = arrow::schema({f_unique, f_count});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByTwoAggregateWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f2 = field("f2", uint32());
  auto f_unique_0 = field("unique", uint32());
  auto f_unique_1 = field("unique", uint32());
  auto f_sum = field("sum", uint64());
  auto f_res = field("res", uint64());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto arg2 = TreeExprBuilder::MakeField(f2);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg0, arg1}, uint32());

  auto n_split = TreeExprBuilder::MakeFunction("splitArrayListWithAction",
                                               {n_pre, arg0, arg1, arg2}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_unique_0 =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, uint32());
  auto n_unique_1 =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg1}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {n_split, arg2}, uint32());

  auto unique_expr_0 = TreeExprBuilder::MakeExpression(n_unique_0, f_res);
  auto unique_expr_1 = TreeExprBuilder::MakeExpression(n_unique_1, f_res);
  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {
      unique_expr_0, unique_expr_1, sum_expr};
  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique_0, f_unique_1, f_sum};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 5, 6, 7, 8 ,9, 10]", "[1, 2, 3, 4, 5, 6, 7, 8 ,9, 10]",
      "[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]"};
  auto res_sch = arrow::schema({f_unique_0, f_unique_1, f_sum});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByTwoUtf8AggregateWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", utf8());
  auto f1 = field("f1", utf8());
  auto f2 = field("f2", uint32());
  auto f_unique_0 = field("unique", utf8());
  auto f_unique_1 = field("unique", utf8());
  auto f_sum = field("sum", uint64());
  auto f_res = field("res", uint64());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto arg2 = TreeExprBuilder::MakeField(f2);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg0, arg1}, uint32());

  auto n_split = TreeExprBuilder::MakeFunction("splitArrayListWithAction",
                                               {n_pre, arg0, arg1, arg2}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_unique_0 =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, uint32());
  auto n_unique_1 =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg1}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {n_split, arg2}, uint32());

  auto unique_expr_0 = TreeExprBuilder::MakeExpression(n_unique_0, f_res);
  auto unique_expr_1 = TreeExprBuilder::MakeExpression(n_unique_1, f_res);
  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {
      unique_expr_0, unique_expr_1, sum_expr};
  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique_0, f_unique_1, f_sum};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["a", "b", "c", "d", "e", "e", "d", "a", "b", "b", "a", "a", "a", "d", "d", "c", "e", "e", "e", "e"])",
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ", "HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])",
      R"([1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5])"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["f", "g", "h", "i", "j", "j", "i", "g", "h", "i", "g", "g", "g", "j", "i", "f", "f", "i", "j", "j"])",
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "DL", "NY", "LA", "DL", "DL", "DL", "AU", "LA", "CD", "CD", "LA", "AU", "AU"])",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])",
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY", "LA", "AU"])",
      "[5, 6, 6, 16, 30, 24, 31, 15, 44, 49]"};
  auto res_sch = arrow::schema({f_unique_0, f_unique_1, f_sum});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);

  std::cout << "3" << std::endl;
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByAggregateWithMultipleBatchOutputWoKeyTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f_unique = field("unique", uint32());
  auto f_sum = field("sum", uint64());
  auto f_count = field("count", uint64());
  auto f_res = field("res", uint64());

  auto arg_pre = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_pre}, uint32());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto n_split =
      TreeExprBuilder::MakeFunction("splitArrayListWithAction", {n_pre, arg1}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {n_split, arg1}, uint32());
  auto n_count = TreeExprBuilder::MakeFunction("action_count", {n_split, arg1}, uint32());

  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sum_expr,
                                                                     count_expr};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_sum, f_count};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]", "[8, 5, 3, 5, 11, 7, 4, 4, 6, 7]"};
  auto res_sch = arrow::schema({f_sum, f_count});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, SortTestNullsFirstAsc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndicesNullsFirstAsc", {arg_0}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "shuffleArrayList", {n_sort_to_indices, arg_0, arg_1}, uint32());
  auto n_action_0 =
      TreeExprBuilder::MakeFunction("action_dono", {n_sort, arg_0}, uint32());
  auto n_action_1 =
      TreeExprBuilder::MakeFunction("action_dono", {n_sort, arg_1}, uint32());

  auto sort_expr_0 = TreeExprBuilder::MakeExpression(n_action_0, f0);
  auto sort_expr_1 = TreeExprBuilder::MakeExpression(n_action_1, f1);
  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sort_expr_0,
                                                                     sort_expr_1};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_2 = {"[1, 14, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[null, null, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20, "
      "21, "
      "22, 23, 30, "
      "32, 33, 35, 37, 41, 42, 43, 50, 52, 59, 64]",
      "[34, 67, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, null, 16, 18, 19, 20, 21, 22, "
      "23, 24, "
      "31, 33, 34, 36, 38, 42, 43, 44, 51, null, 60, 65]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, SortTestNullsLastAsc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto n_sort_to_indices =
      TreeExprBuilder::MakeFunction("sortArraysToIndicesNullsLastAsc", {arg_0}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "shuffleArrayList", {n_sort_to_indices, arg_0, arg_1}, uint32());
  auto n_action_0 =
      TreeExprBuilder::MakeFunction("action_dono", {n_sort, arg_0}, uint32());
  auto n_action_1 =
      TreeExprBuilder::MakeFunction("action_dono", {n_sort, arg_1}, uint32());

  auto sort_expr_0 = TreeExprBuilder::MakeExpression(n_action_0, f0);
  auto sort_expr_1 = TreeExprBuilder::MakeExpression(n_action_1, f1);
  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sort_expr_0,
                                                                     sort_expr_1};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_2 = {"[1, 14, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21, 22, 23, "
      "30, "
      "32, 33, 35, 37, 41, 42, 43, 50, 52, 59, 64, null, null]",
      "[2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, null, 16, 18, 19, 20, 21, 22, 23, 24,"
      "31, 33, 34, 36, 38, 42, 43, 44, 51, null, 60, 65, 34, 67]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, SortTestNullsFirstDesc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndicesNullsFirstDesc", {arg_0}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "shuffleArrayList", {n_sort_to_indices, arg_0, arg_1}, uint32());
  auto n_action_0 =
      TreeExprBuilder::MakeFunction("action_dono", {n_sort, arg_0}, uint32());
  auto n_action_1 =
      TreeExprBuilder::MakeFunction("action_dono", {n_sort, arg_1}, uint32());

  auto sort_expr_0 = TreeExprBuilder::MakeExpression(n_action_0, f0);
  auto sort_expr_1 = TreeExprBuilder::MakeExpression(n_action_1, f1);
  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sort_expr_0,
                                                                     sort_expr_1};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_2 = {"[1, 14, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[null ,null ,64 ,59 ,52 ,50 ,43 ,42 ,41 ,37 ,35 ,33 ,32 ,30 ,23 ,22 ,21 ,20 ,19 "
      ",18 ,17 ,15 ,14 ,13 ,12 , 11 ,10 ,9 ,8 ,7 ,6 ,4 ,3 ,2 ,1]",
      "[34 ,67 ,65 ,60 ,null ,51 ,44 ,43 ,42 ,38 ,36 ,34 ,33 ,31 ,24 ,23 ,22 ,21 , 20 "
      ",19 ,18 ,16 ,null ,14 ,13 ,12 ,11 ,10 ,9 ,8 ,7 ,5 ,4 ,3 ,2]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, SortTestNullsLastDesc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndicesNullsLastDesc", {arg_0}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "shuffleArrayList", {n_sort_to_indices, arg_0, arg_1}, uint32());
  auto n_action_0 =
      TreeExprBuilder::MakeFunction("action_dono", {n_sort, arg_0}, uint32());
  auto n_action_1 =
      TreeExprBuilder::MakeFunction("action_dono", {n_sort, arg_1}, uint32());

  auto sort_expr_0 = TreeExprBuilder::MakeExpression(n_action_0, f0);
  auto sort_expr_1 = TreeExprBuilder::MakeExpression(n_action_1, f1);
  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sort_expr_0,
                                                                     sort_expr_1};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_2 = {"[1, 14, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));

  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[64 ,59 ,52 ,50 ,43 ,42 ,41 ,37 ,35 ,33 ,32 ,30 ,23 ,22 ,21 ,20 ,19 "
      ",18 ,17 ,15 ,14 ,13 ,12 , 11 ,10 ,9 ,8 ,7 ,6 ,4 ,3 ,2 ,1, null, null]",
      "[65 ,60 ,null ,51 ,44 ,43 ,42 ,38 ,36 ,34 ,33 ,31 ,24 ,23 ,22 ,21 , 20 "
      ",19 ,18 ,16 ,null ,14 ,13 ,12 ,11 ,10 ,9 ,8 ,7 ,5 ,4 ,3 ,2, 34, 67]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
