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

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>
#include <gandiva/node.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <chrono>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "codegen/common/result_iterator.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

class BenchmarkArrowComputeWSCG : public ::testing::Test {
 public:
  void SetUp() override {
    // read input from parquet file
#ifdef BENCHMARK_FILE_PATH
    std::string dir_path = BENCHMARK_FILE_PATH;
#else
    std::string dir_path = "";
#endif
    std::string left_path = dir_path + "tpch_lineitem_join.parquet";
    std::string right_path = dir_path + "tpch_order_join.parquet";
    std::cout << "This Benchmark used file " << left_path << " and " << right_path
              << ", please download from server "
                 "vsr200://home/zhouyuan/sparkColumnarPlugin/source_files"
              << std::endl;
    std::shared_ptr<arrow::fs::FileSystem> right_fs;
    std::shared_ptr<arrow::fs::FileSystem> left_fs;
    std::string right_file_name;
    std::string left_file_name;
    ASSERT_OK_AND_ASSIGN(right_fs,
                         arrow::fs::FileSystemFromUri(right_path, &right_file_name));
    ASSERT_OK_AND_ASSIGN(left_fs,
                         arrow::fs::FileSystemFromUri(left_path, &left_file_name));

    ARROW_ASSIGN_OR_THROW(right_file, right_fs->OpenInputFile(right_file_name));
    ARROW_ASSIGN_OR_THROW(left_file, left_fs->OpenInputFile(left_file_name));

    parquet::ArrowReaderProperties properties(true);
    properties.set_batch_size(4096);
    auto pool = arrow::default_memory_pool();

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(left_file), properties,
        &left_parquet_reader));
    ASSERT_NOT_OK(left_parquet_reader->GetRecordBatchReader({0}, {0, 1, 2},
                                                            &left_record_batch_reader));

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(right_file), properties,
        &right_parquet_reader));
    ASSERT_NOT_OK(right_parquet_reader->GetRecordBatchReader({0}, {0, 1},
                                                             &right_record_batch_reader));

    left_schema = left_record_batch_reader->schema();
    right_schema = right_record_batch_reader->schema();
    std::cout << left_schema->ToString() << std::endl;
    std::cout << right_schema->ToString() << std::endl;

    ////////////////// expr prepration ////////////////
    left_field_list = left_record_batch_reader->schema()->fields();
    right_field_list = right_record_batch_reader->schema()->fields();
  }

 protected:
  std::shared_ptr<arrow::io::RandomAccessFile> left_file;
  std::shared_ptr<arrow::io::RandomAccessFile> right_file;
  std::unique_ptr<::parquet::arrow::FileReader> left_parquet_reader;
  std::unique_ptr<::parquet::arrow::FileReader> right_parquet_reader;
  std::shared_ptr<RecordBatchReader> left_record_batch_reader;
  std::shared_ptr<RecordBatchReader> right_record_batch_reader;
  std::shared_ptr<arrow::Schema> left_schema;
  std::shared_ptr<arrow::Schema> right_schema;

  std::vector<std::shared_ptr<::arrow::Field>> left_field_list;
  std::vector<std::shared_ptr<::arrow::Field>> right_field_list;
  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector;
  std::vector<std::shared_ptr<::arrow::Field>> ret_field_list;

  int left_primary_key_index = 0;
  int right_primary_key_index = 0;
};

TEST_F(BenchmarkArrowComputeWSCG, JoinBenchmark) {
  // prepare expression
  std::vector<std::shared_ptr<::gandiva::Node>> left_field_node_list;
  for (auto field : left_field_list) {
    left_field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }

  // prepare expression
  std::vector<std::shared_ptr<::gandiva::Node>> right_field_node_list;
  for (auto field : right_field_list) {
    right_field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);

  auto n_left = TreeExprBuilder::MakeFunction("codegen_left_schema", left_field_node_list,
                                              uint32());
  auto n_right = TreeExprBuilder::MakeFunction("codegen_right_schema",
                                               right_field_node_list, uint32());
  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {left_field_node_list[left_primary_key_index]}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_schema", {right_field_node_list[right_primary_key_index]}, uint32());
  auto f_res = field("res", uint32());

  auto schema_table_0 = arrow::schema(left_field_list);
  auto schema_table_1 = arrow::schema(right_field_list);
  std::vector<std::shared_ptr<Field>> field_list(left_field_list.size() +
                                                 right_field_list.size());
  std::merge(left_field_list.begin(), left_field_list.end(), right_field_list.begin(),
             right_field_list.end(), field_list.begin());
  auto schema_table = arrow::schema(field_list);

  ::gandiva::NodeVector result_node_list;
  for (auto field : field_list) {
    result_node_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_result = TreeExprBuilder::MakeFunction("result", result_node_list, uint32());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());

  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysInner",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config}, uint32());
  auto n_child_probe = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  auto n_wscg =
      TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child_probe}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  ASSERT_NOT_OK(
      CreateCodeGenerator(schema_table_0, {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_1, {probeArrays_expr}, field_list,
                                    &expr_probe, true));

  ///////////////////// Calculation //////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;

  ////////////////////// evaluate //////////////////////
  std::shared_ptr<arrow::RecordBatch> left_record_batch;
  std::shared_ptr<arrow::RecordBatch> right_record_batch;
  uint64_t elapse_gen = 0;
  uint64_t elapse_left_read = 0;
  uint64_t elapse_right_read = 0;
  uint64_t elapse_eval = 0;
  uint64_t elapse_finish = 0;
  uint64_t elapse_probe_process = 0;
  uint64_t elapse_shuffle_process = 0;
  uint64_t num_batches = 0;
  uint64_t num_rows = 0;

  do {
    TIME_MICRO_OR_THROW(elapse_left_read,
                        left_record_batch_reader->ReadNext(&left_record_batch));
    if (left_record_batch) {
      TIME_MICRO_OR_THROW(elapse_eval,
                          expr_build->evaluate(left_record_batch, &dummy_result_batches));
      num_batches += 1;
    }
  } while (left_record_batch);
  std::cout << "Readed left table with " << num_batches << " batches." << std::endl;

  TIME_MICRO_OR_THROW(elapse_finish, expr_build->finish(&build_result_iterator));
  TIME_MICRO_OR_THROW(elapse_finish, expr_probe->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  probe_result_iterator->SetDependencies({build_result_iterator});
  num_batches = 0;
  uint64_t num_output_batches = 0;
  std::shared_ptr<arrow::RecordBatch> out;
  do {
    TIME_MICRO_OR_THROW(elapse_right_read,
                        right_record_batch_reader->ReadNext(&right_record_batch));
    if (right_record_batch) {
      std::vector<std::shared_ptr<arrow::Array>> right_column_vector;
      for (int i = 0; i < right_record_batch->num_columns(); i++) {
        right_column_vector.push_back(right_record_batch->column(i));
      }
      TIME_MICRO_OR_THROW(elapse_probe_process,
                          probe_result_iterator->Process(right_column_vector, &out));
      num_batches += 1;
      num_output_batches++;
      num_rows += out->num_rows();
    }
  } while (right_record_batch);
  std::cout << "Readed right table with " << num_batches << " batches." << std::endl;

  std::cout << "=========================================="
            << "\nBenchmarkArrowComputeWSCG processed " << num_batches << " batches"
            << "\noutput " << num_output_batches << " batches with " << num_rows
            << " rows"
            << "\nCodeGen took " << TIME_TO_STRING(elapse_gen)
            << "\nLeft Batch Read took " << TIME_TO_STRING(elapse_left_read)
            << "\nRight Batch Read took " << TIME_TO_STRING(elapse_right_read)
            << "\nLeft Table Hash Insert took " << TIME_TO_STRING(elapse_eval)
            << "\nMake Result Iterator took " << TIME_TO_STRING(elapse_finish)
            << "\nProbe and Shuffle took " << TIME_TO_STRING(elapse_probe_process) << "\n"
            << "===========================================" << std::endl;
}

TEST_F(BenchmarkArrowComputeWSCG, MultipleJoinBenchmark) {
  // prepare expression
  std::vector<std::shared_ptr<::gandiva::Node>> left_field_node_list;
  for (auto field : left_field_list) {
    left_field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }

  // prepare expression
  std::vector<std::shared_ptr<::gandiva::Node>> right_field_node_list;
  for (auto field : right_field_list) {
    right_field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);

  auto n_left = TreeExprBuilder::MakeFunction("codegen_left_schema", left_field_node_list,
                                              uint32());
  auto n_right = TreeExprBuilder::MakeFunction("codegen_right_schema",
                                               right_field_node_list, uint32());
  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {left_field_node_list[left_primary_key_index]}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_schema", {right_field_node_list[right_primary_key_index]}, uint32());
  auto f_res = field("res", uint32());

  auto schema_table_0 = arrow::schema(left_field_list);
  auto schema_table_1 = arrow::schema(right_field_list);
  std::vector<std::shared_ptr<Field>> field_list(left_field_list.size() +
                                                 right_field_list.size());
  std::merge(left_field_list.begin(), left_field_list.end(), right_field_list.begin(),
             right_field_list.end(), field_list.begin());
  auto schema_table = arrow::schema(field_list);

  ::gandiva::NodeVector result_node_list;
  for (auto field : field_list) {
    result_node_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_result = TreeExprBuilder::MakeFunction("result", result_node_list, uint32());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than", {left_field_node_list[1], right_field_node_list[1]},
      arrow::boolean());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysAnti",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config, n_condition},
      uint32());
  auto n_child_probe = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());

  auto n_probeArrays_1 = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysAnti",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config, n_condition},
      uint32());
  auto n_child_probe_1 =
      TreeExprBuilder::MakeFunction("child", {n_probeArrays_1, n_child_probe}, uint32());

  auto n_wscg =
      TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child_probe_1}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build_0;
  ASSERT_NOT_OK(
      CreateCodeGenerator(schema_table_0, {hashRelation_expr}, {}, &expr_build_0, true));
  std::shared_ptr<CodeGenerator> expr_build_1;
  ASSERT_NOT_OK(
      CreateCodeGenerator(schema_table_0, {hashRelation_expr}, {}, &expr_build_1, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_1, {probeArrays_expr}, field_list,
                                    &expr_probe, true));

  ///////////////////// Calculation //////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> build_result_iterator_0;
  std::shared_ptr<ResultIteratorBase> build_result_iterator_1;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;

  ////////////////////// evaluate //////////////////////
  std::shared_ptr<arrow::RecordBatch> left_record_batch;
  std::shared_ptr<arrow::RecordBatch> right_record_batch;
  uint64_t elapse_gen = 0;
  uint64_t elapse_left_read = 0;
  uint64_t elapse_right_read = 0;
  uint64_t elapse_eval = 0;
  uint64_t elapse_finish = 0;
  uint64_t elapse_probe_process = 0;
  uint64_t elapse_shuffle_process = 0;
  uint64_t num_batches = 0;
  uint64_t num_rows = 0;

  do {
    TIME_MICRO_OR_THROW(elapse_left_read,
                        left_record_batch_reader->ReadNext(&left_record_batch));
    if (left_record_batch) {
      TIME_MICRO_OR_THROW(
          elapse_eval, expr_build_0->evaluate(left_record_batch, &dummy_result_batches));
      TIME_MICRO_OR_THROW(
          elapse_eval, expr_build_1->evaluate(left_record_batch, &dummy_result_batches));
      num_batches += 1;
    }
  } while (left_record_batch);
  std::cout << "Readed left table with " << num_batches << " batches." << std::endl;

  TIME_MICRO_OR_THROW(elapse_finish, expr_build_0->finish(&build_result_iterator_0));
  TIME_MICRO_OR_THROW(elapse_finish, expr_build_1->finish(&build_result_iterator_1));
  TIME_MICRO_OR_THROW(elapse_finish, expr_probe->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  probe_result_iterator->SetDependencies(
      {build_result_iterator_0, build_result_iterator_1});
  num_batches = 0;
  uint64_t num_output_batches = 0;
  std::shared_ptr<arrow::RecordBatch> out;
  do {
    TIME_MICRO_OR_THROW(elapse_right_read,
                        right_record_batch_reader->ReadNext(&right_record_batch));
    if (right_record_batch) {
      std::vector<std::shared_ptr<arrow::Array>> right_column_vector;
      for (int i = 0; i < right_record_batch->num_columns(); i++) {
        right_column_vector.push_back(right_record_batch->column(i));
      }
      TIME_MICRO_OR_THROW(elapse_probe_process,
                          probe_result_iterator->Process(right_column_vector, &out));
      num_batches += 1;
      num_output_batches++;
      num_rows += out->num_rows();
    }
  } while (right_record_batch);
  std::cout << "Readed right table with " << num_batches << " batches." << std::endl;

  std::cout << "=========================================="
            << "\nBenchmarkArrowComputeWSCG processed " << num_batches << " batches"
            << "\noutput " << num_output_batches << " batches with " << num_rows
            << " rows"
            << "\nCodeGen took " << TIME_TO_STRING(elapse_gen)
            << "\nLeft Batch Read took " << TIME_TO_STRING(elapse_left_read)
            << "\nRight Batch Read took " << TIME_TO_STRING(elapse_right_read)
            << "\nLeft Table Hash Insert took " << TIME_TO_STRING(elapse_eval)
            << "\nMake Result Iterator took " << TIME_TO_STRING(elapse_finish)
            << "\nProbe and Shuffle took " << TIME_TO_STRING(elapse_probe_process) << "\n"
            << "===========================================" << std::endl;
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
