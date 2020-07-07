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
#include <gandiva/gandiva_aliases.h>
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

class BenchmarkArrowComputeSort : public ::testing::Test {
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

    ////////////////// expr prepration ////////////////
    field_list = record_batch_reader->schema()->fields();
    ret_field_list = record_batch_reader->schema()->fields();
  }

  void StartWithIterator(std::shared_ptr<CodeGenerator> sort_expr) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
    std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;

    std::shared_ptr<arrow::RecordBatch> record_batch;

    do {
      TIME_MICRO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      if (record_batch) {
        TIME_MICRO_OR_THROW(elapse_eval,
                            sort_expr->evaluate(record_batch, &dummy_result_batches));
        num_batches += 1;
      }
    } while (record_batch);
    std::cout << "Readed " << num_batches << " batches." << std::endl;
    TIME_MICRO_OR_THROW(elapse_sort, sort_expr->finish(&sort_result_iterator));
    std::shared_ptr<arrow::RecordBatch> result_batch;

    uint64_t num_output_batches = 0;
    while (sort_result_iterator->HasNext()) {
      TIME_MICRO_OR_THROW(elapse_shuffle, sort_result_iterator->Next(&result_batch));
      num_output_batches++;
    }
    arrow::PrettyPrint(*result_batch.get(), 2, &std::cout);

    std::cout << "==================== Summary ====================\n"
              << "BenchmarkArrowComputeJoin processed " << num_batches
              << " batches\nthen output " << num_output_batches
              << " batches\nCodeGen took " << TIME_TO_STRING(elapse_gen)
              << "\nBatch read took " << TIME_TO_STRING(elapse_read)
              << "\nEvaluation took " << TIME_TO_STRING(elapse_eval) << "\nSort took "
              << TIME_TO_STRING(elapse_sort) << "\nShuffle took "
              << TIME_TO_STRING(elapse_shuffle)
              << ".\n================================================" << std::endl;
  }

 protected:
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
  std::shared_ptr<RecordBatchReader> record_batch_reader;

  std::vector<std::shared_ptr<::arrow::Field>> field_list;
  std::vector<std::shared_ptr<::arrow::Field>> ret_field_list;

  int primary_key_index = 0;
  std::shared_ptr<arrow::Field> f_res;

  uint64_t elapse_gen = 0;
  uint64_t elapse_read = 0;
  uint64_t elapse_eval = 0;
  uint64_t elapse_sort = 0;
  uint64_t elapse_shuffle = 0;
  uint64_t num_batches = 0;
};

TEST_F(BenchmarkArrowComputeSort, SortBenchmark) {
  elapse_gen = 0;
  elapse_read = 0;
  elapse_eval = 0;
  elapse_sort = 0;
  elapse_shuffle = 0;
  num_batches = 0;
  ////////////////////// prepare expr_vector ///////////////////////
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  f_res = field("res", arrow::uint64());

  std::vector<std::shared_ptr<::gandiva::Node>> gandiva_field_list;
  for (auto field : field_list) {
    gandiva_field_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_sort_to_indices =
      TreeExprBuilder::MakeFunction("sortArraysToIndicesNullsFirstAsc",
                                    {gandiva_field_list[primary_key_index]}, uint64());
  std::shared_ptr<arrow::Schema> schema;
  schema = arrow::schema(field_list);
  std::cout << schema->ToString() << std::endl;

  ::gandiva::ExpressionPtr sortArrays_expr;
  sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort_to_indices, f_res);

  std::shared_ptr<CodeGenerator> sort_expr;
  TIME_MICRO_OR_THROW(elapse_gen, CreateCodeGenerator(schema, {sortArrays_expr},
                                                      ret_field_list, &sort_expr, true));

  ///////////////////// Calculation //////////////////
  StartWithIterator(sort_expr);
}

TEST_F(BenchmarkArrowComputeSort, SortBenchmarkWOPayLoad) {
  elapse_gen = 0;
  elapse_read = 0;
  elapse_eval = 0;
  elapse_sort = 0;
  elapse_shuffle = 0;
  num_batches = 0;
  ////////////////////// prepare expr_vector ///////////////////////
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  f_res = field("res", arrow::uint64());

  std::vector<std::shared_ptr<::gandiva::Node>> gandiva_field_list;
  for (auto field : field_list) {
    gandiva_field_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_sort_to_indices =
      TreeExprBuilder::MakeFunction("sortArraysToIndicesNullsFirstAsc",
                                    {gandiva_field_list[primary_key_index]}, uint64());
  std::shared_ptr<arrow::Schema> schema;
  schema = arrow::schema({field_list[primary_key_index]});
  ::gandiva::ExpressionPtr sortArrays_expr;
  sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort_to_indices, f_res);

  std::shared_ptr<CodeGenerator> sort_expr;
  TIME_MICRO_OR_THROW(elapse_gen, CreateCodeGenerator(schema, {sortArrays_expr},
                                                      {ret_field_list[primary_key_index]},
                                                      &sort_expr, true));

  ///////////////////// Calculation //////////////////
  StartWithIterator(sort_expr);
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
