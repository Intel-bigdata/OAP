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

#include <arrow/io/api.h>
#include <arrow/ipc/message.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/util.h>
#include <arrow/record_batch.h>
#include <arrow/util/io_util.h>
#include <gtest/gtest.h>
#include <iostream>
#include "shuffle/splitter.h"
#include "shuffle/type.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace shuffle {

class ShuffleTest : public ::testing::Test {
 protected:
  void SetUp() {

    auto f_pid = field("f_pid", arrow::int32());
    auto f_na = field("f_na", arrow::null());
    auto f_int8 = field("f_int8", arrow::int8());
    auto f_int16 = field("f_int16", arrow::int16());
    auto f_uint64 = field("f_uint64", arrow::uint64());
    auto f_bool = field("f_bool", arrow::boolean());
    auto f_string = field("f_string", arrow::utf8());

    std::shared_ptr<arrow::internal::TemporaryDir> tmp_dir1;
    std::shared_ptr<arrow::internal::TemporaryDir> tmp_dir2;
    ARROW_ASSIGN_OR_THROW(tmp_dir1, std::move(arrow::internal::TemporaryDir::Make(tmp_dir_prefix)))
    ARROW_ASSIGN_OR_THROW(tmp_dir2, std::move(arrow::internal::TemporaryDir::Make(tmp_dir_prefix)))
    auto config_dirs = tmp_dir1->path().ToString() + "," + tmp_dir2->path().ToString();

    setenv("NATIVESQL_SPARK_LOCAL_DIRS", config_dirs.c_str(), 1);

    schema_ = arrow::schema({f_pid, f_na, f_int8, f_int16, f_uint64, f_bool, f_string});
    ARROW_ASSIGN_OR_THROW(writer_schema_, schema_->RemoveField(0))

    ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make(schema_));
  }

  void TearDown() { ASSERT_NOT_OK(splitter_->Stop()); }

  std::string tmp_dir_prefix = "columnar-shuffle-test";

  std::string c_pid_ = "[1, 2, 1, 10]";
  std::vector<std::string> input_data_ = {c_pid_,
                                          "[null, null, null, null]",
                                          "[1, 2, 3, null]",
                                          "[1, -1, null, null]",
                                          "[null, null, null, null]",
                                          "[null, 1, 0, null]",
                                          R"(["alice", "bob", null, null])"};

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::Schema> writer_schema_;
  std::shared_ptr<Splitter> splitter_;
};

TEST_F(ShuffleTest, TestSplitterSchema) { ASSERT_EQ(*schema_, *splitter_->schema()); }

TEST_F(ShuffleTest, TestSplitterTypeId) {
  ASSERT_EQ(splitter_->column_type_id(0), Type::SHUFFLE_NULL);
  ASSERT_EQ(splitter_->column_type_id(1), Type::SHUFFLE_1BYTE);
  ASSERT_EQ(splitter_->column_type_id(2), Type::SHUFFLE_2BYTE);
  ASSERT_EQ(splitter_->column_type_id(3), Type::SHUFFLE_8BYTE);
  ASSERT_EQ(splitter_->column_type_id(4), Type::SHUFFLE_BIT);
}

TEST_F(ShuffleTest, TestWriterAfterSplit) {
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data_, schema_, &input_batch);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));

  ASSERT_NE(splitter_->writer(1), nullptr);
  ASSERT_NE(splitter_->writer(2), nullptr);
  ASSERT_NE(splitter_->writer(10), nullptr);
  ASSERT_EQ(splitter_->writer(100), nullptr);

  ASSERT_EQ(splitter_->writer(1)->pid(), 1);
  ASSERT_EQ(splitter_->writer(2)->pid(), 2);
  ASSERT_EQ(splitter_->writer(10)->pid(), 10);

  ASSERT_EQ(splitter_->writer(1)->capacity(), kDefaultSplitterBufferSize);

  ASSERT_EQ(splitter_->writer(1)->write_offset(), 2);
  ASSERT_EQ(splitter_->writer(2)->write_offset(), 1);
  ASSERT_EQ(splitter_->writer(10)->write_offset(), 1);
}

TEST_F(ShuffleTest, TestLastType) {
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data_, schema_, &input_batch);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_EQ(splitter_->writer(1)->last_type(), Type::SHUFFLE_BINARY);
  ASSERT_EQ(splitter_->writer(2)->last_type(), Type::SHUFFLE_BINARY);
  ASSERT_EQ(splitter_->writer(10)->last_type(), Type::SHUFFLE_BINARY);
}

TEST_F(ShuffleTest, TestMultipleInput) {
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data_, schema_, &input_batch);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_EQ(splitter_->writer(1)->write_offset(), 2);
  ASSERT_EQ(splitter_->writer(2)->write_offset(), 1);
  ASSERT_EQ(splitter_->writer(10)->write_offset(), 1);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_NOT_OK(splitter_->Split(*input_batch));

  ASSERT_EQ(splitter_->writer(1)->write_offset(), 6);
  ASSERT_EQ(splitter_->writer(2)->write_offset(), 3);
  ASSERT_EQ(splitter_->writer(10)->write_offset(), 3);
}

TEST_F(ShuffleTest, TestCustomBufferSize) {
  int64_t buffer_size = 2;
  splitter_->set_buffer_size(buffer_size);

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data_, schema_, &input_batch);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_EQ(splitter_->writer(1)->write_offset(), 2);
  ASSERT_EQ(splitter_->writer(2)->write_offset(), 1);
  ASSERT_EQ(splitter_->writer(10)->write_offset(), 1);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_EQ(splitter_->writer(1)->write_offset(), 2);
  ASSERT_EQ(splitter_->writer(2)->write_offset(), 2);
  ASSERT_EQ(splitter_->writer(10)->write_offset(), 2);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_EQ(splitter_->writer(1)->write_offset(), 2);
  ASSERT_EQ(splitter_->writer(2)->write_offset(), 1);
  ASSERT_EQ(splitter_->writer(10)->write_offset(), 1);
}

TEST_F(ShuffleTest, TestCreateTempFile) {
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data_, schema_, &input_batch);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_EQ(splitter_->GetPartitionFileInfo().size(), 3);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_EQ(splitter_->GetPartitionFileInfo().size(), 3);

  MakeInputBatch({"[100]", "[null]", "[null]", "[null]", "[null]", "[null]", "[null]"},
                 schema_, &input_batch);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_EQ(splitter_->GetPartitionFileInfo().size(), 4);

  auto pfn0 = splitter_->GetPartitionFileInfo()[0].second;
  auto pfn1 = splitter_->GetPartitionFileInfo()[1].second;
  auto pfn2 = splitter_->GetPartitionFileInfo()[2].second;
  auto pfn3 = splitter_->GetPartitionFileInfo()[3].second;
  ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(pfn0)), true);
  ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(pfn1)), true);
  ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(pfn2)), true);
  ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(pfn3)), true);

  ASSERT_NE(pfn0.find(tmp_dir_prefix), std::string::npos);
  ASSERT_NE(pfn1.find(tmp_dir_prefix), std::string::npos);
  ASSERT_NE(pfn2.find(tmp_dir_prefix), std::string::npos);
  ASSERT_NE(pfn3.find(tmp_dir_prefix), std::string::npos);
}

TEST_F(ShuffleTest, TestWriterMakeArrowRecordBatch) {
  int64_t buffer_size = 2;
  splitter_->set_buffer_size(buffer_size);

  std::vector<std::string> output_data = {"[null, null]", "[1, 3]",
                                          "[1, null]",    "[null, null]",
                                          "[null, 0]",    R"(["alice", null])"};

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::shared_ptr<arrow::RecordBatch> output_batch;
  MakeInputBatch(input_data_, schema_, &input_batch);
  MakeInputBatch(output_data, writer_schema_, &output_batch);

  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_NOT_OK(splitter_->Split(*input_batch));
  ASSERT_NOT_OK(splitter_->Split(*input_batch));

  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::io::ReadableFile> file_in;
  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_in,
                        arrow::io::ReadableFile::Open(splitter_->writer(1)->file_path()))

  ARROW_ASSIGN_OR_THROW(file_reader, arrow::ipc::RecordBatchStreamReader::Open(file_in))
  ASSERT_EQ(*file_reader->schema(), *writer_schema_);

  int num_rb = 3;
  for (int i = 0; i < num_rb; ++i) {
    std::shared_ptr<arrow::RecordBatch> rb;
    ASSERT_NOT_OK(file_reader->ReadNext(&rb));
    ASSERT_NOT_OK(Equals(*output_batch, *rb));
  }
  ASSERT_NOT_OK(file_in->Close())
}

TEST_F(ShuffleTest, TestCustomCompressionCodec) {
  auto compression_codec = arrow::Compression::LZ4_FRAME;
  splitter_->set_compression_codec(compression_codec);

  std::vector<std::string> output_data = {"[null, null]", "[1, 3]",
                                          "[1, null]",    "[null, null]",
                                          "[null, 0]",    R"(["alice", null])"};

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::shared_ptr<arrow::RecordBatch> output_batch;
  MakeInputBatch(input_data_, schema_, &input_batch);
  MakeInputBatch(output_data, writer_schema_, &output_batch);

  ASSERT_NOT_OK(splitter_->Split(*input_batch))
  ASSERT_NOT_OK(splitter_->Stop())

  std::shared_ptr<arrow::io::ReadableFile> file_in;
  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_in,
                        arrow::io::ReadableFile::Open(splitter_->writer(1)->file_path()))

  ARROW_ASSIGN_OR_THROW(file_reader, arrow::ipc::RecordBatchStreamReader::Open(file_in))
  ASSERT_EQ(*file_reader->schema(), *writer_schema_);

  std::shared_ptr<arrow::RecordBatch> rb;
  ASSERT_NOT_OK(file_reader->ReadNext(&rb));
  ASSERT_NOT_OK(Equals(*rb, *output_batch));

  ASSERT_NOT_OK(file_in->Close())
}

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
