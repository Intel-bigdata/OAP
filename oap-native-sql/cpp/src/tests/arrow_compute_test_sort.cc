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
#include <arrow/ipc/json_simple.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include <memory>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

TEST(TestArrowComputeSort, SortTestNullsFirstAsc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndicesNullsFirstAsc", {arg_0}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort_to_indices, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  ASSERT_NOT_OK(
      CreateCodeGenerator(sch, {sortArrays_expr}, {f_indices}, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, 14, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[null, null, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21, "
      "22, 23, 30, "
      "32, 33, 35, 37, 41, 42, 43, 50, 52, 59, 64]",
      "[34, 67, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, null, 16, 18, 19, 20, 21, 22, "
      "23, 24, "
      "31, 33, 34, 36, 38, 42, 43, 44, 51, null, 60, 65]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator));

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestNullsLastAsc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_sort_to_indices =
      TreeExprBuilder::MakeFunction("sortArraysToIndicesNullsLastAsc", {arg_0}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort_to_indices, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  ASSERT_NOT_OK(
      CreateCodeGenerator(sch, {sortArrays_expr}, {f_indices}, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, 14, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21, 22, 23, 30, "
      "32, 33, 35, 37, 41, 42, 43, 50, 52, 59, 64, null, null]",
      "[2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, null, 16, 18, 19, 20, 21, 22, 23, 24,"
      "31, 33, 34, 36, 38, 42, 43, 44, 51, null, 60, 65, 34, 67]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator));

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestNullsFirstDesc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndicesNullsFirstDesc", {arg_0}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort_to_indices, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  ASSERT_NOT_OK(
      CreateCodeGenerator(sch, {sortArrays_expr}, {f_indices}, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, 14, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[null ,null ,64 ,59 ,52 ,50 ,43 ,42 ,41 ,37 ,35 ,33 ,32 ,30 ,23 ,22 ,21 ,20 ,19 "
      ",18 ,17 ,15 ,14 ,13 ,12 , 11 ,10 ,9 ,8 ,7 ,6 ,4 ,3 ,2 ,1]",
      "[34 ,67 ,65 ,60 ,null ,51 ,44 ,43 ,42 ,38 ,36 ,34 ,33 ,31 ,24 ,23 ,22 ,21 , 20 "
      ",19 ,18 ,16 ,null ,14 ,13 ,12 ,11 ,10 ,9 ,8 ,7 ,5 ,4 ,3 ,2]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator));

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestNullsLastDesc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndicesNullsLastDesc", {arg_0}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort_to_indices, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  ASSERT_NOT_OK(
      CreateCodeGenerator(sch, {sortArrays_expr}, {f_indices}, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, 14, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[64 ,59 ,52 ,50 ,43 ,42 ,41 ,37 ,35 ,33 ,32 ,30 ,23 ,22 ,21 ,20 ,19 "
      ",18 ,17 ,15 ,14 ,13 ,12 , 11 ,10 ,9 ,8 ,7 ,6 ,4 ,3 ,2 ,1, null, null]",
      "[65 ,60 ,null ,51 ,44 ,43 ,42 ,38 ,36 ,34 ,33 ,31 ,24 ,23 ,22 ,21 , 20 "
      ",19 ,18 ,16 ,null ,14 ,13 ,12 ,11 ,10 ,9 ,8 ,7 ,5 ,4 ,3 ,2, 34, 67]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator));

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestNullsFirstAscMultipleKeys) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", utf8());
  auto f2 = field("f2", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndicesNullsFirstAsc", {arg_0, arg_1}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort_to_indices, f_res);

  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1, f2};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  ASSERT_NOT_OK(
      CreateCodeGenerator(sch, {sortArrays_expr}, {f_indices}, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;

  std::vector<std::string> input_data_string = {"[8, 12, 4, 50, 52, 32, 11]",
                                                R"(["a", "a", "a", "b", "b","b", "b"])",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, 14, 8, 42, 6, null, 2]",
                                                  R"(["a", "a", "a", "b", "b","b", "b"])",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 8, 7, 9, 8, 33]",
                                                  R"(["a", "a", "a", "b", "b","b", "b"])",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  R"(["a", "a", "a", "b", "b","b", "b"])",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  R"(["a", "a", "a", "b", "b","b", "b"])",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[null, null, 1, 2, 3, 4, 6, 7, 8, 8, 8, 8, 8, 9, 11, 12, 13, 14, 17, 18, 20, 21, "
      "22, 23, 30, 32, 33, 35, 37, 41, 42, 50, 52, 59, 64]",
      R"(["b","a","a","b","a","a","b","b","a","a","a","b","b","b","b","a","b","a","a","b","b","b","a","a","b","b","b","b","a","a","b","b","b","b","a"])",
      "[34, 67, 2, 3, 4, 5, 7, 8, 11, 44, 16, 9, 20, 10, 12, 13, 14, null, 18, 19, 21, "
      "22, 23, 24, 31, 33, 34, 36, 38, 42, 43, 51, null, 60, 65]"};

  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator));

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
