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

#include "codegen/arrow_compute/ext/codegen_common.h"

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <sstream>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

std::string BaseCodes() {
  return R"(
#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>

#include <algorithm>
#include <iostream>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/common/result_iterator.h"
#include "sparsehash/sparse_hash_map.h"
#include "third_party/arrow/utils/hashing.h"

using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;

)";
}

std::string GetArrowTypeDefString(std::shared_ptr<arrow::DataType> type) {
  switch (type->id()) {
    case arrow::UInt8Type::type_id:
      return "uint8()";
    case arrow::Int8Type::type_id:
      return "int8()";
    case arrow::UInt16Type::type_id:
      return "uint16()";
    case arrow::Int16Type::type_id:
      return "int16()";
    case arrow::UInt32Type::type_id:
      return "uint32()";
    case arrow::Int32Type::type_id:
      return "int32()";
    case arrow::UInt64Type::type_id:
      return "uint64()";
    case arrow::Int64Type::type_id:
      return "int64()";
    case arrow::FloatType::type_id:
      return "float632()";
    case arrow::DoubleType::type_id:
      return "float64()";
    case arrow::Date32Type::type_id:
      return "date32()";
    case arrow::StringType::type_id:
      return "utf8()";
    default:
      std::cout << "GetTypeString can't convert " << type->ToString() << std::endl;
      throw;
  }
}
std::string GetCTypeString(std::shared_ptr<arrow::DataType> type) {
  switch (type->id()) {
    case arrow::UInt8Type::type_id:
      return "uint8_t";
    case arrow::Int8Type::type_id:
      return "int8_t";
    case arrow::UInt16Type::type_id:
      return "uint16_t";
    case arrow::Int16Type::type_id:
      return "int16_t";
    case arrow::UInt32Type::type_id:
      return "uint32_t";
    case arrow::Int32Type::type_id:
      return "int32_t";
    case arrow::UInt64Type::type_id:
      return "uint64_t";
    case arrow::Int64Type::type_id:
      return "int64_t";
    case arrow::FloatType::type_id:
      return "float";
    case arrow::DoubleType::type_id:
      return "double";
    case arrow::Date32Type::type_id:
      return "int32_t";
    case arrow::StringType::type_id:
      return "std::string";
    default:
      std::cout << "GetTypeString can't convert " << type->ToString() << std::endl;
      throw;
  }
}
std::string GetTypeString(std::shared_ptr<arrow::DataType> type, std::string tail) {
  switch (type->id()) {
    case arrow::UInt8Type::type_id:
      return "UInt8" + tail;
    case arrow::Int8Type::type_id:
      return "Int8" + tail;
    case arrow::UInt16Type::type_id:
      return "UInt16" + tail;
    case arrow::Int16Type::type_id:
      return "Int16" + tail;
    case arrow::UInt32Type::type_id:
      return "UInt32" + tail;
    case arrow::Int32Type::type_id:
      return "Int32" + tail;
    case arrow::UInt64Type::type_id:
      return "UInt64" + tail;
    case arrow::Int64Type::type_id:
      return "Int64" + tail;
    case arrow::FloatType::type_id:
      return "Float" + tail;
    case arrow::DoubleType::type_id:
      return "Double" + tail;
    case arrow::Date32Type::type_id:
      return "Date32" + tail;
    case arrow::StringType::type_id:
      return "String" + tail;
    default:
      std::cout << "GetTypeString can't convert " << type->ToString() << std::endl;
      throw;
  }
}

std::string GetTypedArrayDefineString(std::shared_ptr<arrow::DataType> type,
                                      std::string name) {
  return "std::vector<std::shared_ptr<arrow::" + GetTypeString(type, "Array") + ">> " +
         name + ";\n";
}

arrow::Status GetIndexList(const std::vector<std::shared_ptr<arrow::Field>>& target_list,
                           const std::vector<std::shared_ptr<arrow::Field>>& source_list,
                           std::vector<int>* out) {
  for (auto key_field : target_list) {
    int i = 0;
    for (auto field : source_list) {
      if (key_field->name() == field->name()) {
        break;
      }
      i++;
    }
    (*out).push_back(i);
  }
  return arrow::Status::OK();
}

arrow::Status GetIndexListFromSchema(
    const std::shared_ptr<arrow::Schema>& result_schema,
    const std::vector<std::shared_ptr<arrow::Field>>& field_list,
    std::vector<int>* index_list) {
  int i = 0;
  for (auto field : field_list) {
    auto indices = result_schema->GetAllFieldIndices(field->name());
    if (indices.size() == 1) {
      (*index_list).push_back(i);
    }
    i++;
  }
  return arrow::Status::OK();
}

std::string GetTempPath() {
  std::string tmp_dir_;
  const char* env_tmp_dir = std::getenv("NATIVESQL_TMP_DIR");
  if (env_tmp_dir != nullptr) {
    tmp_dir_ = std::string(env_tmp_dir);
  } else {
#ifdef NATIVESQL_SRC_PATH
    tmp_dir_ = NATIVESQL_SRC_PATH;
#else
    std::cerr << "envioroment variable NATIVESQL_TMP_DIR is not set" << std::endl;
    throw;
#endif
  }
  return tmp_dir_;
}

int GetBatchSize() {
  int batch_size;
  const char* env_batch_size = std::getenv("NATIVESQL_BATCH_SIZE");
  if (env_batch_size != nullptr) {
    batch_size = atoi(env_batch_size);
  } else {
    batch_size = 10000;
  }
  return batch_size;
}

int FileSpinLock() {
  std::string lockfile = GetTempPath() + "/nativesql_compile.lock";

  auto fd = open(lockfile.c_str(), O_CREAT, S_IRWXU | S_IRWXG);
  flock(fd, LOCK_EX);

  return fd;
}

void FileSpinUnLock(int fd) {
  flock(fd, LOCK_UN);
  close(fd);
}

arrow::Status CompileCodes(std::string codes, std::string signature) {
  // temporary cpp/library output files
  srand(time(NULL));
  std::string outpath = GetTempPath() + "/tmp/";
  mkdir(outpath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  std::string prefix = "/spark-columnar-plugin-codegen-";
  std::string cppfile = outpath + prefix + signature + ".cc";
  std::string libfile = outpath + prefix + signature + ".so";
  std::string logfile = outpath + prefix + signature + ".log";
  std::ofstream out(cppfile.c_str(), std::ofstream::out);

  // output code to file
  if (out.bad()) {
    std::cout << "cannot open " << cppfile << std::endl;
    exit(EXIT_FAILURE);
  }
  out << codes;
#ifdef DEBUG
  std::cout << "BatchSize is " << GetBatchSize() << std::endl;
  std::cout << codes << std::endl;
#endif
  out.flush();
  out.close();

  // compile the code
  const char* env_gcc_ = std::getenv("CC");
  if (env_gcc_ == nullptr) {
    env_gcc_ = "gcc";
  }
  std::string env_gcc = std::string(env_gcc_);

  const char* env_arrow_dir = std::getenv("LIBARROW_DIR");
  std::string arrow_header;
  std::string arrow_lib, arrow_lib2;
  std::string nativesql_header = " -I" + GetTempPath() + "/nativesql_include/ ";
  std::string nativesql_header_2 = " -I" + GetTempPath() + "/include/ ";
  std::string nativesql_lib = " -L" + GetTempPath() + " ";
  if (env_arrow_dir != nullptr) {
    arrow_header = " -I" + std::string(env_arrow_dir) + "/include ";
    arrow_lib = " -L" + std::string(env_arrow_dir) + "/lib64 ";
    // incase there's a different location for libarrow.so
    arrow_lib2 = " -L" + std::string(env_arrow_dir) + "/lib ";
  }
  // compile the code
  std::string cmd = env_gcc + " -std=c++14 -Wno-deprecated-declarations " + arrow_header +
                    arrow_lib + arrow_lib2 + nativesql_header + nativesql_header_2 +
                    nativesql_lib + cppfile + " -o " + libfile +
                    " -O3 -march=native -shared -fPIC -larrow -lspark_columnar_jni 2> " +
                    logfile;
  //#ifdef DEBUG
  std::cout << cmd << std::endl;
  //#endif
  int ret = system(cmd.c_str());
  if (WEXITSTATUS(ret) != EXIT_SUCCESS) {
    std::cout << "compilation failed, see " << logfile << std::endl;
    std::cout << cmd << std::endl;
    cmd = "ls -R -l " + GetTempPath() + "; cat " + logfile;
    system(cmd.c_str());
    exit(EXIT_FAILURE);
  }

  struct stat tstat;
  ret = stat(libfile.c_str(), &tstat);
  if (ret == -1) {
    std::cout << "stat failed: " << strerror(errno) << std::endl;
    exit(EXIT_FAILURE);
  }

  return arrow::Status::OK();
}

arrow::Status LoadLibrary(std::string signature, arrow::compute::FunctionContext* ctx,
                          std::shared_ptr<CodeGenBase>* out) {
  std::string outpath = GetTempPath() + "/tmp/";
  std::string prefix = "/spark-columnar-plugin-codegen-";
  std::string libfile = outpath + prefix + signature + ".so";
  // load dynamic library
  void* dynlib = dlopen(libfile.c_str(), RTLD_LAZY);
  if (!dynlib) {
    return arrow::Status::Invalid(libfile, " is not generated");
  }

  // loading symbol from library and assign to pointer
  // (to be cast to function pointer later)

  std::cout << "LoadLibrary " << libfile << std::endl;
  void (*MakeCodeGen)(arrow::compute::FunctionContext * ctx,
                      std::shared_ptr<CodeGenBase> * out);
  *(void**)(&MakeCodeGen) = dlsym(dynlib, "MakeCodeGen");
  const char* dlsym_error = dlerror();
  if (dlsym_error != NULL) {
    std::stringstream ss;
    ss << "error loading symbol:\n" << dlsym_error << std::endl;
    return arrow::Status::Invalid(ss.str());
  }

  MakeCodeGen(ctx, out);
  return arrow::Status::OK();
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
