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

#pragma once

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>
#include <iostream>
#include <unordered_map>
#include <utility>
#include <vector>
#include "shuffle/partition_writer.h"
#include "shuffle/type.h"

namespace sparkcolumnarplugin {
namespace shuffle {

class Splitter {
 public:
  ~Splitter();

  static arrow::Result<std::shared_ptr<Splitter>> Make(
      const std::shared_ptr<arrow::Schema>& schema);

  std::shared_ptr<arrow::Schema> schema() const;

  Type::typeId column_type_id(int i) const;

  void set_buffer_size(int64_t buffer_size);

  void set_compression_codec(arrow::Compression::type compression_codec);

  arrow::Status Split(const arrow::RecordBatch&);

  /***
   * Stop all writers created by this splitter. If the data buffer managed by the writer
   * is not empty, write to output stream as RecordBatch. Then sort the temporary files by
   * partition id.
   * @return
   */
  arrow::Status Stop();

  const std::vector<std::pair<int32_t, std::string>>& GetPartitionFileInfo() const;

  arrow::Result<int64_t> TotalBytesWritten();

  // writer must be called after Split.
  std::shared_ptr<PartitionWriter> writer(int32_t pid);

 private:
  explicit Splitter(const std::shared_ptr<arrow::Schema>& schema);
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
