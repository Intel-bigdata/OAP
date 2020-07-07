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

class BenchmarkArrowCompute : public ::testing::Test {
 public:
  void SetUp() override {
    // read input from parquet file
#ifdef BENCHMARK_FILE_PATH
    std::string dir_path = BENCHMARK_FILE_PATH;
#else
    std::string dir_path = "";
#endif
    std::string path = dir_path + "tpcds_websales_sort_big.parquet";
    std::cout << "This Benchmark used file " << path
              << ", please download from server "
                 "vsr200://home/zhouyuan/sparkColumnarPlugin/source_files"
              << std::endl;
    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::string file_name;
    ASSERT_OK_AND_ASSIGN(fs, arrow::fs::FileSystemFromUri(path, &file_name));

    ARROW_ASSIGN_OR_THROW(file, fs->OpenInputFile(file_name));

    parquet::ArrowReaderProperties properties(true);
    properties.set_batch_size(4096);

    auto pool = arrow::default_memory_pool();
    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(file), properties, &parquet_reader));

    ASSERT_NOT_OK(
        parquet_reader->GetRecordBatchReader({0}, {0, 1, 2}, &record_batch_reader));

    schema = record_batch_reader->schema();

    ////////////////// expr prepration ////////////////
    field_list = schema->fields();
  }

  void Start() {
    std::shared_ptr<CodeGenerator> expr;
    std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
    std::shared_ptr<arrow::RecordBatch> record_batch;
    uint64_t elapse_gen = 0;
    uint64_t elapse_read = 0;
    uint64_t elapse_eval = 0;
    uint64_t num_batches = 0;

    TIME_MICRO_OR_THROW(elapse_gen, CreateCodeGenerator(schema, expr_vector,
                                                        ret_field_list, &expr, true));

    do {
      TIME_MICRO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      if (record_batch) {
        TIME_MICRO_OR_THROW(elapse_eval, expr->evaluate(record_batch, &result_batch));
        num_batches += 1;
      }
    } while (record_batch);
    std::cout << "Readed " << num_batches << " batches." << std::endl;

    TIME_MICRO_OR_THROW(elapse_eval, expr->finish(&result_batch));
    /*auto batch = result_batch[result_batch.size() - 1];
    arrow::PrettyPrint(*batch.get(), 2, &std::cout);*/

    std::cout << "BenchmarkArrowComputeBigScale processed " << num_batches
              << " batches,\n output " << result_batch.size() << " batches,\n took "
              << TIME_TO_STRING(elapse_gen) << " doing codegen,\n took "
              << TIME_TO_STRING(elapse_read) << " doing BatchRead,\n took "
              << TIME_TO_STRING(elapse_eval) << " doing Batch Evaluation." << std::endl;
  }

  void StartWithIterator() {
    std::shared_ptr<CodeGenerator> expr;
    std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
    std::shared_ptr<arrow::RecordBatch> record_batch;
    std::shared_ptr<arrow::RecordBatch> out;
    uint64_t elapse_gen = 0;
    uint64_t elapse_read = 0;
    uint64_t elapse_eval = 0;
    uint64_t num_batches = 0;
    uint64_t num_rows = 0;

    TIME_MICRO_OR_THROW(elapse_gen, CreateCodeGenerator(schema, expr_vector,
                                                        ret_field_list, &expr, true));

    do {
      TIME_MICRO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      if (record_batch) {
        TIME_MICRO_OR_THROW(elapse_eval, expr->evaluate(record_batch, &result_batch));
        num_batches += 1;
      }
    } while (record_batch);
    std::cout << "Readed " << num_batches << " batches." << std::endl;

    std::shared_ptr<ResultIterator<arrow::RecordBatch>> it;
    uint64_t num_output_batches = 0;
    TIME_MICRO_OR_THROW(elapse_eval, expr->finish(&it));
    while (it->HasNext()) {
      TIME_MICRO_OR_THROW(elapse_eval, it->Next(&out));
      num_output_batches++;
      num_rows += out->num_rows();
    }

    std::cout << "BenchmarkArrowCompute processed " << num_batches
              << " batches, then output " << num_output_batches << " batches with "
              << num_rows << " rows, to complete, it took " << TIME_TO_STRING(elapse_gen)
              << " doing codegen, took " << TIME_TO_STRING(elapse_read)
              << " doing BatchRead, took " << TIME_TO_STRING(elapse_eval)
              << " doing Batch Evaluation." << std::endl;
  }

 protected:
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
  std::shared_ptr<RecordBatchReader> record_batch_reader;
  std::shared_ptr<arrow::Schema> schema;

  std::vector<std::shared_ptr<::arrow::Field>> field_list;
  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector;
  std::vector<std::shared_ptr<::arrow::Field>> ret_field_list;
};

TEST_F(BenchmarkArrowCompute, AggregateBenchmark) {
  for (auto field : field_list) {
    auto n_sum = TreeExprBuilder::MakeFunction("sum", {TreeExprBuilder::MakeField(field)},
                                               field->type());
    auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, field);
    expr_vector.push_back(sum_expr);
    ret_field_list.push_back(field);
  }

  ///////////////////// Calculation //////////////////
  Start();
}

TEST_F(BenchmarkArrowCompute, GroupByAggregateBenchmark) {
  // prepare expression
  std::vector<std::shared_ptr<::gandiva::Node>> field_node_list;
  for (auto field : field_list) {
    field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }

  auto n_encode = TreeExprBuilder::MakeFunction(
      "encodeArray", {TreeExprBuilder::MakeField(field_list[0])}, field_list[0]->type());

  std::vector<std::shared_ptr<::gandiva::Node>> arg_for_aggr = {n_encode};
  arg_for_aggr.insert(arg_for_aggr.end(), field_node_list.begin(), field_node_list.end());

  auto n_aggr = TreeExprBuilder::MakeFunction("splitArrayListWithAction", arg_for_aggr,
                                              uint32() /*won't be used*/);

  for (auto field : field_list) {
    auto action = TreeExprBuilder::MakeFunction(
        "action_sum", {n_aggr, TreeExprBuilder::MakeField(field)}, field->type());
    auto aggr_expr = TreeExprBuilder::MakeExpression(action, field);
    expr_vector.push_back(aggr_expr);
    ret_field_list.push_back(field);
  }

  ///////////////////// Calculation //////////////////
  Start();
}

TEST_F(BenchmarkArrowCompute, GroupByWithTwoAggregateBenchmark) {
  // prepare expression
  std::vector<std::shared_ptr<::gandiva::Node>> field_node_list;
  for (auto field : field_list) {
    field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }

  auto n_encode =
      TreeExprBuilder::MakeFunction("encodeArray",
                                    {TreeExprBuilder::MakeField(field_list[0]),
                                     TreeExprBuilder::MakeField(field_list[1])},
                                    uint32());

  std::vector<std::shared_ptr<::gandiva::Node>> arg_for_aggr = {n_encode};
  arg_for_aggr.insert(arg_for_aggr.end(), field_node_list.begin(), field_node_list.end());

  auto n_aggr = TreeExprBuilder::MakeFunction("splitArrayListWithAction", arg_for_aggr,
                                              uint32() /*won't be used*/);

  for (auto field : field_list) {
    auto action = TreeExprBuilder::MakeFunction(
        "action_sum", {n_aggr, TreeExprBuilder::MakeField(field)}, field->type());
    auto aggr_expr = TreeExprBuilder::MakeExpression(action, field);
    expr_vector.push_back(aggr_expr);
    ret_field_list.push_back(field);
  }

  ///////////////////// Calculation //////////////////
  Start();
}

/*TEST_F(BenchmarkArrowCompute, SortBenchmark) {
  // prepare expression
  std::vector<std::shared_ptr<::gandiva::Node>> field_node_list;
  for (auto field : field_list) {
    field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }

  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndicesNullsFirstAsc", {TreeExprBuilder::MakeField(field_list[0])},
      field_list[0]->type());
  std::vector<std::shared_ptr<::gandiva::Node>> arg_for_shuffle = {n_sort_to_indices};
  arg_for_shuffle.insert(arg_for_shuffle.end(), field_node_list.begin(),
                         field_node_list.end());
  auto n_sort =
      TreeExprBuilder::MakeFunction("shuffleArrayList", arg_for_shuffle, uint32());
  for (auto field : field_list) {
    auto action = TreeExprBuilder::MakeFunction(
        "action_dono", {n_sort, TreeExprBuilder::MakeField(field)}, field->type());
    auto sort_expr = TreeExprBuilder::MakeExpression(action, field);
    expr_vector.push_back(sort_expr);
    ret_field_list.push_back(field);
  }

  ///////////////////// Calculation //////////////////
  StartWithIterator();
}*/

}  // namespace codegen
}  // namespace sparkcolumnarplugin
