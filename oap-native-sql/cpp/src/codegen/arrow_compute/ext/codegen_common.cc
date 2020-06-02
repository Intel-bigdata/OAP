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

template <typename T> class ResultIterator {
public:
  virtual bool HasNext() { return false; }
  virtual arrow::Status Next(std::shared_ptr<T> *out) {
    return arrow::Status::NotImplemented("ResultIterator abstract Next function");
  }
  virtual arrow::Status
  Process(std::vector<std::shared_ptr<arrow::Array>> in,
          std::shared_ptr<T> *out,
          const std::shared_ptr<arrow::Array> &selection = nullptr) {
    return arrow::Status::NotImplemented("ResultIterator abstract Process function");
  }
  virtual arrow::Status
  ProcessAndCacheOne(std::vector<std::shared_ptr<arrow::Array>> in,
                     const std::shared_ptr<arrow::Array> &selection = nullptr) {
    return arrow::Status::NotImplemented(
        "ResultIterator abstract ProcessAndCacheOne function");
  }
  virtual arrow::Status GetResult(std::shared_ptr<arrow::RecordBatch>* out) {
    return arrow::Status::NotImplemented("ResultIterator abstract GetResult function");
  }
  virtual std::string ToString() { return ""; }
};

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
struct ArrayItemIndex {
  uint64_t id = 0;
  uint64_t array_id = 0;
  ArrayItemIndex(uint64_t array_id, uint64_t id) : array_id(array_id), id(id) {}
};

class CodeGenBase {
 public:
  virtual arrow::Status Evaluate(const ArrayList& in) {
    return arrow::Status::NotImplemented("SortBase Evaluate is an abstract interface.");
  }
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("SortBase Finish is an abstract interface.");
  }
  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "SortBase MakeResultIterator is an abstract interface.");
  }
};)";
}

int FileSpinLock(std::string path) {
  std::string lockfile = path + "/nativesql_compile.lock";

  auto fd = open(lockfile.c_str(), O_CREAT, S_IRWXU | S_IRWXG);
  flock(fd, LOCK_EX);

  return fd;
}

void FileSpinUnLock(int fd) {
  flock(fd, LOCK_UN);
  close(fd);
}

std::string GetTypeString(std::shared_ptr<arrow::DataType> type) {
  switch (type->id()) {
    case arrow::UInt8Type::type_id:
      return "UInt8Type";
    case arrow::Int8Type::type_id:
      return "Int8Type";
    case arrow::UInt16Type::type_id:
      return "UInt16Type";
    case arrow::Int16Type::type_id:
      return "Int16Type";
    case arrow::UInt32Type::type_id:
      return "UInt32Type";
    case arrow::Int32Type::type_id:
      return "Int32Type";
    case arrow::UInt64Type::type_id:
      return "UInt64Type";
    case arrow::Int64Type::type_id:
      return "Int64Type";
    case arrow::FloatType::type_id:
      return "FloatType";
    case arrow::DoubleType::type_id:
      return "DoubleType";
    case arrow::Date32Type::type_id:
      return "Date32Type";
    case arrow::StringType::type_id:
      return "StringType";
    default:
      std::cout << "GetTypeString can't convert " << type->ToString() << std::endl;
      throw;
  }
}

arrow::Status CompileCodes(std::string codes, std::string signature) {
  // temporary cpp/library output files
  srand(time(NULL));
  std::string outpath = "/tmp";
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
  std::string arrow_lib;
  if (env_arrow_dir != nullptr) {
    arrow_header = " -I" + std::string(env_arrow_dir) + "/include ";
    arrow_lib = " -L" + std::string(env_arrow_dir) + "/lib64 ";
  }
  // compile the code
  std::string cmd = env_gcc + " -std=c++11 -Wall -Wextra " + arrow_header + arrow_lib +
                    cppfile + " -o " + libfile + " -O3 -shared -fPIC -larrow 2> " +
                    logfile;
  int ret = system(cmd.c_str());
  if (WEXITSTATUS(ret) != EXIT_SUCCESS) {
    std::cout << "compilation failed, see " << logfile << std::endl;
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
  std::string outpath = "/tmp";
  std::string prefix = "/spark-columnar-plugin-codegen-";
  std::string libfile = outpath + prefix + signature + ".so";
  std::cout << "LoadLibrary " << libfile << std::endl;
  // load dynamic library
  void* dynlib = dlopen(libfile.c_str(), RTLD_LAZY);
  if (!dynlib) {
    return arrow::Status::Invalid(libfile, " is not generated");
  }

  // loading symbol from library and assign to pointer
  // (to be cast to function pointer later)

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
