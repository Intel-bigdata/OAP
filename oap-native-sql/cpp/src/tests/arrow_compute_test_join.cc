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

TEST(TestArrowCompute, JoinTestUsingInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);

  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysInner", {n_left_key, n_right_key}, indices_type);
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto n_conditionedShuffleArrayList =
      TreeExprBuilder::MakeFunction("conditionedShuffleArrayList", {}, uint32());
  auto n_codegen_shuffle = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_conditionedShuffleArrayList, n_left, n_right},
      uint32());

  auto conditionShuffleExpr = TreeExprBuilder::MakeExpression(n_codegen_shuffle, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0, {probeArrays_expr}, {f_indices},
                                    &expr_probe, true));
  std::shared_ptr<CodeGenerator> expr_conditioned_shuffle;
  ASSERT_NOT_OK(
      CreateCodeGenerator(schema_table, {conditionShuffleExpr},
                          {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1},
                          &expr_conditioned_shuffle, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 1, 2, 3, 3, 5, 6, 6]", "[1, 11, 2, 3, 13, 5, 6, 16]",
      "[1, 11, 2, 3, 13, 5, 6, 16]", "[1, 1, 2, 3, 3, 5, 6, 6]",
      "[1, 1, 2, 3, 3, 5, 6, 6]"};
  auto res_sch = arrow::schema({f_res, f_res, f_res, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[8, 10, 10, 12]", "[8, 10, 110, 12]", "[8, 10, 110, 12]",
                            "[8, 10, 10, 12]", "[8, 10, 10, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
    ASSERT_NOT_OK(expr_conditioned_shuffle->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator));
  ASSERT_NOT_OK(expr_conditioned_shuffle->SetDependency(probe_result_iterator));
  ASSERT_NOT_OK(expr_conditioned_shuffle->finish(&shuffle_result_iterator));

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->ProcessAndCacheOne(input));
    ASSERT_NOT_OK(shuffle_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, JoinTestWithTwoKeysUsingInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", utf8());
  auto table0_f2 = field("table0_f2", int32());
  auto table1_f0 = field("table1_f0", utf8());
  auto table1_f1 = field("table1_f1", utf8());

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);

  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysInner", {n_left_key, n_right_key}, indices_type);
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto n_conditionedShuffleArrayList =
      TreeExprBuilder::MakeFunction("conditionedShuffleArrayList", {}, uint32());
  auto n_codegen_shuffle = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_conditionedShuffleArrayList, n_left, n_right},
      uint32());

  auto conditionShuffleExpr = TreeExprBuilder::MakeExpression(n_codegen_shuffle, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});

  auto f_res_utf = field("res", utf8());

  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0, {probeArrays_expr}, {f_indices},
                                    &expr_probe, true));
  std::shared_ptr<CodeGenerator> expr_conditioned_shuffle;
  ASSERT_NOT_OK(
      CreateCodeGenerator(schema_table, {conditionShuffleExpr},
                          {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1},
                          &expr_conditioned_shuffle, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      R"(["l", "c", "a", "b"])", R"(["L", "C", "A", "B"])", "[10, 3, 1, 2]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["f", "n", "e", "j"])", R"(["F", "N", "E", "J"])",
                       "[6, 12, 5, 8]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {R"(["a", "b", "c", "d", "e", "f"])",
                                                  R"(["A", "B", "C", "D", "E", "F"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {R"(["i", "j", "k", "l", "m", "n"])",
                         R"(["I", "J", "K", "L", "M", "N"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["a", "b", "c", "e", "f"])", R"(["A", "B", "C", "E", "F"])", "[1, 2, 3, 5, 6]",
      R"(["a", "b", "c", "e", "f"])", R"(["A", "B", "C", "E", "F"])"};
  MakeInputBatch(expected_result_string, schema_table, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {R"(["j", "l", "n"])", R"(["J", "L", "N"])", "[8, 10, 12]",
                            R"(["j", "l", "n"])", R"(["J", "L", "N"])"};
  MakeInputBatch(expected_result_string, schema_table, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
    ASSERT_NOT_OK(expr_conditioned_shuffle->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator));
  ASSERT_NOT_OK(expr_conditioned_shuffle->SetDependency(probe_result_iterator));
  ASSERT_NOT_OK(expr_conditioned_shuffle->finish(&shuffle_result_iterator));

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->ProcessAndCacheOne(input));
    ASSERT_NOT_OK(shuffle_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, JoinTestUsingOuterJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);

  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysOuter", {n_left_key, n_right_key}, indices_type);
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto n_conditionedShuffleArrayList =
      TreeExprBuilder::MakeFunction("conditionedShuffleArrayList", {}, uint32());
  auto n_codegen_shuffle = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_conditionedShuffleArrayList, n_left, n_right},
      uint32());

  auto conditionShuffleExpr = TreeExprBuilder::MakeExpression(n_codegen_shuffle, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0, {probeArrays_expr}, {f_indices},
                                    &expr_probe, true));
  std::shared_ptr<CodeGenerator> expr_conditioned_shuffle;
  ASSERT_NOT_OK(
      CreateCodeGenerator(schema_table, {conditionShuffleExpr},
                          {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1},
                          &expr_conditioned_shuffle, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 1, 2, 3, 3, null, 5, 6, 6]", "[1, 11, 2, 3, 13, null, 5, 6, 16]",
      "[1, 11, 2, 3, 13, null, 5, 6, 16]", "[1, 1, 2, 3, 3, 4, 5, 6, 6]",
      "[1, 1, 2, 3, 3, 4, 5, 6, 6]"};
  auto res_sch = arrow::schema({f_res, f_res, f_res, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[null, 8, null, 10, 10, null, 12]",
                            "[null, 8, null, 10, 110, null, 12]",
                            "[null, 8, null, 10, 110, null, 12]",
                            "[7, 8, 9, 10, 10, 11, 12]", "[7, 8, 9, 10, 10, 11, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
    ASSERT_NOT_OK(expr_conditioned_shuffle->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator));
  ASSERT_NOT_OK(expr_conditioned_shuffle->SetDependency(probe_result_iterator));
  ASSERT_NOT_OK(expr_conditioned_shuffle->finish(&shuffle_result_iterator));

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->ProcessAndCacheOne(input));
    ASSERT_NOT_OK(shuffle_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, JoinTestUsingAntiJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);

  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysAnti", {n_left_key, n_right_key}, indices_type);
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto n_conditionedShuffleArrayList =
      TreeExprBuilder::MakeFunction("conditionedShuffleArrayList", {}, uint32());
  auto n_codegen_shuffle = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_conditionedShuffleArrayList, n_left, n_right},
      uint32());

  auto conditionShuffleExpr = TreeExprBuilder::MakeExpression(n_codegen_shuffle, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0, {probeArrays_expr}, {f_indices},
                                    &expr_probe, true));
  std::shared_ptr<CodeGenerator> expr_conditioned_shuffle;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table, {conditionShuffleExpr},
                                    {table1_f0, table1_f1}, &expr_conditioned_shuffle,
                                    true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  auto res_sch = arrow::schema({f_res, f_res});
  std::vector<std::string> expected_result_string = {"[4]", "[4]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[7, 9, 11]", "[7, 9, 11]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
    ASSERT_NOT_OK(expr_conditioned_shuffle->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator));
  ASSERT_NOT_OK(expr_conditioned_shuffle->SetDependency(probe_result_iterator));
  ASSERT_NOT_OK(expr_conditioned_shuffle->finish(&shuffle_result_iterator));

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->ProcessAndCacheOne(input));
    ASSERT_NOT_OK(shuffle_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, JoinTestUsingInnerJoinWithCondition) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);
  auto greater_than_function = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      arrow::boolean());
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysInner", {n_left_key, n_right_key, greater_than_function},
      indices_type);
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto n_conditionedShuffleArrayList =
      TreeExprBuilder::MakeFunction("conditionedShuffleArrayList", {}, uint32());
  auto n_codegen_shuffle = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_conditionedShuffleArrayList, n_left, n_right},
      uint32());

  auto conditionShuffleExpr = TreeExprBuilder::MakeExpression(n_codegen_shuffle, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0, {probeArrays_expr}, {f_indices},
                                    &expr_probe, true));
  std::shared_ptr<CodeGenerator> expr_conditioned_shuffle;
  ASSERT_NOT_OK(
      CreateCodeGenerator(schema_table, {conditionShuffleExpr},
                          {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1},
                          &expr_conditioned_shuffle, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 3, 6]", "[11, 13, 16]", "[11, 13, 16]", "[1, 3, 6]", "[1, 3, 6]"};
  auto res_sch = arrow::schema({f_res, f_res, f_res, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[10]", "[110]", "[110]", "[10]", "[10]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
    ASSERT_NOT_OK(expr_conditioned_shuffle->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator));
  ASSERT_NOT_OK(expr_conditioned_shuffle->SetDependency(probe_result_iterator));
  ASSERT_NOT_OK(expr_conditioned_shuffle->finish(&shuffle_result_iterator));

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->ProcessAndCacheOne(input));
    ASSERT_NOT_OK(shuffle_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
