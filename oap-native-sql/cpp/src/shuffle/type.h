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

#include <arrow/extension_type.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>
#include <deque>

namespace sparkcolumnarplugin {
namespace shuffle {

static constexpr int64_t kDefaultSplitterBufferSize = 4096;

struct BufferMessage {
  std::shared_ptr<arrow::Buffer> validity_buffer;
  std::shared_ptr<arrow::Buffer> value_buffer;
  uint8_t* validity_addr;
  uint8_t* value_addr;
};

struct BufferAddr {
  uint8_t* validity_addr;
  uint8_t* value_addr;
};

namespace Type {
/// \brief Data type enumeration for shuffle splitter
///
/// This enumeration maps the types of arrow::Type::type with same length
/// to identical type

enum typeId : int {
  SHUFFLE_1BYTE,
  SHUFFLE_2BYTE,
  SHUFFLE_4BYTE,
  SHUFFLE_8BYTE,
  SHUFFLE_BIT,
  SHUFFLE_BINARY,
  SHUFFLE_LARGE_BINARY,
  SHUFFLE_NULL,
  NUM_TYPES,
  SHUFFLE_NOT_IMPLEMENTED
};

static const typeId all[] = {
    SHUFFLE_1BYTE, SHUFFLE_2BYTE,  SHUFFLE_4BYTE,        SHUFFLE_8BYTE,
    SHUFFLE_BIT,   SHUFFLE_BINARY, SHUFFLE_LARGE_BINARY, SHUFFLE_NULL,
};

// std::shared_ptr<arrow::DataType> fixed_size_binary(int32_t byte_width) {
//  return std::make_shared<arrow::FixedSizeBinaryType>(byte_width);
//}
//
// class Fixed1ByteType : public arrow::ExtensionType {
// public:
//  static constexpr Type::typeId shuffle_type_id = Type::SHUFFLE_1BYTE;
//
//  Fixed1ByteType() : arrow::ExtensionType(fixed_size_binary(1)) {}
//
//  std::string extension_name() const override { return "fixed_1_byte"; }
//
//  bool ExtensionEquals(const ExtensionType& other) const override {
//    return other.extension_name() == this->extension_name();
//  }
//
//  std::shared_ptr<arrow::Array> MakeArray(
//      std::shared_ptr<arrow::ArrayData> data) const override {
//    DCHECK_EQ(data->type->id(), arrow::Type::EXTENSION);
//    DCHECK_EQ("fixed_1_byte",
//              static_cast<const ExtensionType&>(*data->type).extension_name());
//    return std::make_shared<arrow::ExtensionArray>(data);
//  }
//
//  arrow::Status Deserialize(std::shared_ptr<DataType> storage_type,
//                            const std::string& serialized,
//                            std::shared_ptr<DataType>* out) const override {
//    if (serialized != "fixed-1-byte-type-unique-code") {
//      return arrow::Status::Invalid("Type identifier did not match");
//    }
//    DCHECK(storage_type->Equals(*fixed_size_binary(1)));
//    *out = std::make_shared<Fixed1ByteType>();
//    return arrow::Status::OK();
//  }
//
//  std::string Serialize() const override { return "fixed-1-byte-type-unique-code"; }
//};

}  // namespace Type

using BufferMessages = std::deque<std::unique_ptr<BufferMessage>>;
using TypeBufferMessages = std::vector<BufferMessages>;
using BinaryBuilders = std::deque<std::unique_ptr<arrow::BinaryBuilder>>;
using LargeBinaryBuilders = std::deque<std::unique_ptr<arrow::LargeBinaryBuilder>>;
using BufferPtr = std::shared_ptr<arrow::Buffer>;
using SrcBuffers = std::vector<BufferAddr>;
using SrcArrays = std::vector<std::shared_ptr<arrow::Array>>;
using SrcBinaryArrays = std::vector<std::shared_ptr<arrow::BinaryArray>>;
using SrcLargeBinaryArrays = std::vector<std::shared_ptr<arrow::LargeBinaryArray>>;

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
