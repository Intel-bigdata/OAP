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

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/context.h>
#include <arrow/compute/kernel.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <arrow/visitor_inline.h>
#include <dlfcn.h>
#include <gandiva/configuration.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <unordered_map>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include "codegen/arrow_compute/ext/item_iterator.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/shuffle_v2_action.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  ConditionedShuffleArrayList  ////////////////
class ConditionedShuffleArrayListKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<gandiva::Node> func_node,
       std::vector<std::shared_ptr<arrow::Field>> left_field_list,
       std::vector<std::shared_ptr<arrow::Field>> right_field_list,
       std::vector<std::shared_ptr<arrow::Field>> output_field_list)
      : ctx_(ctx),
        func_node_(func_node),
        left_field_list_(left_field_list),
        right_field_list_(right_field_list) {
    if (func_node) {
      for (auto field : left_field_list) {
        std::shared_ptr<ItemIterator> iter;
        MakeArrayListItemIterator(field->type(), &iter);
        input_iterator_list_.push_back(iter);
      }
      for (auto field : right_field_list) {
        std::shared_ptr<ItemIterator> iter;
        MakeArrayItemIterator(field->type(), &iter);
        input_iterator_list_.push_back(iter);
      }
    }

    auto input_field_list = {left_field_list, right_field_list};
    input_cache_.resize(left_field_list.size());
    auto left_field_list_size = left_field_list.size();
    for (auto out_field : output_field_list) {
      int col_id = 0;
      bool found = false;
      for (auto arg : input_field_list) {
        for (auto col : arg) {
          if (col->name() == out_field->name()) {
            found = true;
            break;
          }
          col_id++;
        }
        if (found) break;
      }

      std::shared_ptr<ShuffleV2Action> action;
      if (col_id < left_field_list_size) {
        MakeShuffleV2Action(ctx_, out_field->type(), true, &action);
      } else {
        MakeShuffleV2Action(ctx_, out_field->type(), false, &action);
      }
      output_indices_.push_back(col_id);
      output_action_list_.push_back(action);
    }
  }

  ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    if (in.size() != input_cache_.size()) {
      return arrow::Status::Invalid(
          "ConditionedShuffleArrayListKernel input arrayList size does not match "
          "numCols "
          "in cache, which are ",
          in.size(), " and ", input_cache_.size());
    }
    // we need to convert std::vector<Batch> to std::vector<ArrayList>
    for (int col_id = 0; col_id < input_cache_.size(); col_id++) {
      input_cache_[col_id].push_back(in[col_id]);
    }

    return arrow::Status::OK();
  }

  arrow::Status SetDependencyIter(
      const std::shared_ptr<ResultIterator<arrow::RecordBatch>>& in, int index) {
    in_indices_iter_ = in;
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    // This Function suppose to return a lambda function for later ResultIterator
    auto start = std::chrono::steady_clock::now();
    if (func_node_) {
      LoadJITFunction(func_node_, left_field_list_, right_field_list_);
    }
    auto end = std::chrono::steady_clock::now();
    std::cout
        << "Code Generation took "
        << (std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() /
            1000)
        << " ms." << std::endl;
    std::function<arrow::Status(std::shared_ptr<arrow::Array>, std::vector<ArrayList>,
                                std::shared_ptr<arrow::Array>, ArrayList,
                                std::shared_ptr<arrow::RecordBatch>*)>
        eval_func;
    if (func_node_) {
      eval_func = [this, schema](std::shared_ptr<arrow::Array> left_selection,
                                 std::vector<ArrayList> left_in,
                                 std::shared_ptr<arrow::Array> right_selection,
                                 ArrayList right_in,
                                 std::shared_ptr<arrow::RecordBatch>* out) {
        int num_rows = right_selection->length();
        std::vector<std::function<bool()>> is_null_func_list;
        std::vector<std::function<arrow::Status()>> next_func_list;
        std::vector<std::function<void*()>> get_func_list;
        std::vector<std::function<arrow::Status()>> shuffle_func_list;
        std::function<bool()> is_null;
        std::function<arrow::Status()> next;
        std::function<void*()> get;
        std::function<arrow::Status()> shuffle;
        auto left_field_list_size = left_field_list_.size();
        int i = 0;
        for (auto array_list : left_in) {
          RETURN_NOT_OK(input_iterator_list_[i++]->Submit(array_list, left_selection,
                                                          &next, &is_null, &get));
          is_null_func_list.push_back(is_null);
          next_func_list.push_back(next);
          get_func_list.push_back(get);
        }
        for (auto array : right_in) {
          RETURN_NOT_OK(input_iterator_list_[i++]->Submit(array, right_selection, &next,
                                                          &is_null, &get));
          is_null_func_list.push_back(is_null);
          next_func_list.push_back(next);
          get_func_list.push_back(get);
        }
        i = 0;
        for (auto id : output_indices_) {
          if (id < left_field_list_size) {
            RETURN_NOT_OK(
                output_action_list_[i++]->Submit(left_in[id], left_selection, &shuffle));
            shuffle_func_list.push_back(shuffle);
          } else {
            RETURN_NOT_OK(output_action_list_[i++]->Submit(
                right_in[id - left_field_list_size], right_selection, &shuffle));
            shuffle_func_list.push_back(shuffle);
          }
        }
        ConditionShuffleCodeGen(num_rows, next_func_list, is_null_func_list,
                                get_func_list, shuffle_func_list);
        ArrayList out_arr_list;
        for (auto output_action : output_action_list_) {
          RETURN_NOT_OK(output_action->FinishAndReset(&out_arr_list));
        }
        *out = arrow::RecordBatch::Make(schema, out_arr_list[0]->length(), out_arr_list);
        // arrow::PrettyPrint(*(*out).get(), 2, &std::cout);

        return arrow::Status::OK();
      };
    } else {
      eval_func = [this, schema](std::shared_ptr<arrow::Array> left_selection,
                                 std::vector<ArrayList> left_in,
                                 std::shared_ptr<arrow::Array> right_selection,
                                 ArrayList right_in,
                                 std::shared_ptr<arrow::RecordBatch>* out) {
        int num_rows = right_selection->length();
        std::vector<std::function<bool()>> is_null_func_list;
        std::vector<std::function<arrow::Status()>> next_func_list;
        std::vector<std::function<void*()>> get_func_list;
        std::vector<std::function<arrow::Status()>> shuffle_func_list;
        std::function<bool()> is_null;
        std::function<arrow::Status()> next;
        std::function<void*()> get;
        std::function<arrow::Status()> shuffle;
        auto left_field_list_size = left_field_list_.size();
        int i = 0;
        for (auto id : output_indices_) {
          if (id < left_field_list_size) {
            RETURN_NOT_OK(
                output_action_list_[i++]->Submit(left_in[id], left_selection, &shuffle));
            shuffle_func_list.push_back(shuffle);
          } else {
            RETURN_NOT_OK(output_action_list_[i++]->Submit(
                right_in[id - left_field_list_size], right_selection, &shuffle));
            shuffle_func_list.push_back(shuffle);
          }
        }
        for (int row_id = 0; row_id < num_rows; row_id++) {
          for (auto exec : shuffle_func_list) {
            exec();
          }
        }
        ArrayList out_arr_list;
        for (auto output_action : output_action_list_) {
          RETURN_NOT_OK(output_action->FinishAndReset(&out_arr_list));
        }
        *out = arrow::RecordBatch::Make(schema, out_arr_list[0]->length(), out_arr_list);
        // arrow::PrettyPrint(*(*out).get(), 2, &std::cout);

        return arrow::Status::OK();
      };
    }
    *out = std::make_shared<ConditionShuffleCodeGenResultIterator>(
        ctx_, input_cache_, eval_func, in_indices_iter_);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  // input_cache_ is used to cache left table for later on probe
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> in_indices_iter_;
  std::vector<ArrayList> input_cache_;

  std::shared_ptr<gandiva::Node> func_node_;
  std::vector<std::shared_ptr<arrow::Field>> left_field_list_;
  std::vector<std::shared_ptr<arrow::Field>> right_field_list_;
  std::vector<std::shared_ptr<ItemIterator>> input_iterator_list_;
  std::vector<int> output_indices_;
  std::vector<std::shared_ptr<ShuffleV2Action>> output_action_list_;
  void (*ConditionShuffleCodeGen)(
      int num_rows, std::vector<std::function<arrow::Status()>> next_func_list,
      std::vector<std::function<bool()>> is_null_func_list,
      std::vector<std::function<void*()>> get_func_list,
      std::vector<std::function<arrow::Status()>> output_function_list);
  arrow::Status LoadJITFunction(
      std::shared_ptr<gandiva::Node> func_node,
      std::vector<std::shared_ptr<arrow::Field>> left_field_list,
      std::vector<std::shared_ptr<arrow::Field>> right_field_list) {
    // generate ddl signature
    std::stringstream func_args_ss;
    func_args_ss << func_node->ToString();
    for (auto field : left_field_list) {
      func_args_ss << field->ToString();
    }
    for (auto field : right_field_list) {
      func_args_ss << field->ToString();
    }

    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(func_args_ss.str());
    std::string signature = signature_ss.str();

    auto status = LoadLibrary(signature);
    if (!status.ok()) {
      // process
      auto codes = ProduceCodes(func_node_, left_field_list_, right_field_list_);
      // compile codes
      RETURN_NOT_OK(CompileCodes(codes, signature));
      RETURN_NOT_OK(LoadLibrary(signature));
    }
    return arrow::Status::OK();
  }

  std::string ProduceCodes(std::shared_ptr<gandiva::Node> func_node,
                           std::vector<std::shared_ptr<arrow::Field>> left_field_list,
                           std::vector<std::shared_ptr<arrow::Field>> right_field_list) {
    // CodeGen
    std::stringstream codes_ss;
    codes_ss << "#include <arrow/type.h>" << std::endl;
    codes_ss
        << R"(#include <sparkcolumnarplugin/codegen/arrow_compute/ext/item_iterator.h>)"
        << std::endl;
    codes_ss << "#define int8 int8_t" << std::endl;
    codes_ss << "#define int16 int16_t" << std::endl;
    codes_ss << "#define int32 int32_t" << std::endl;
    codes_ss << "#define int64 int64_t" << std::endl;
    codes_ss << "#define uint8 uint8_t" << std::endl;
    codes_ss << "#define uint16 uint16_t" << std::endl;
    codes_ss << "#define uint32 uint32_t" << std::endl;
    codes_ss << "#define uint64 uint64_t" << std::endl;
    codes_ss << "using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;"
             << std::endl;
    codes_ss << R"(extern "C" void ConditionShuffleCodeGen(int num_rows, 
      std::vector<std::function<arrow::Status()>> next_func_list,
      std::vector<std::function<bool()>> is_null_func_list,
      std::vector<std::function<void*()>> get_func_list,
      std::vector<std::function<arrow::Status()>> output_func_list) {)"
             << std::endl;
    codes_ss << "for (int row_id = 0; row_id < num_rows; row_id++) {" << std::endl;
    codes_ss << "  for (auto next : next_func_list) {" << std::endl;
    codes_ss << "      next();" << std::endl;
    codes_ss << "  }" << std::endl;
    std::shared_ptr<CodeGenNodeVisitor> func_node_visitor;
    int func_count = 0;
    MakeCodeGenNodeVisitor(func_node, {left_field_list, right_field_list}, &func_count,
                           &codes_ss, &func_node_visitor);
    codes_ss << "if (" << func_node_visitor->GetResult() << ") {" << std::endl;
    codes_ss << "  for (auto exec : output_func_list) {" << std::endl;
    codes_ss << "    exec();" << std::endl;
    codes_ss << "  }" << std::endl;
    codes_ss << "}" << std::endl;
    codes_ss << "}" << std::endl;
    codes_ss << "}" << std::endl;
    return codes_ss.str();
  }

  arrow::Status CompileCodes(std::string codes, std::string signature) {
    // temporary cpp/library output files
    srand(time(NULL));
    std::string randname = std::to_string(rand());
    std::string outpath = "/tmp";
    std::string prefix = "/spark-columnar-plugin-codegen-";
    std::string cppfile = outpath + prefix + signature + ".cc";
    std::string tmplibfile = outpath + prefix + signature + "." + randname + ".so";
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
    std::string cmd = "gcc -Wall -Wextra " + cppfile + " -o " + tmplibfile +
                      " -O3 -shared -fPIC -lspark_columnar_jni > " + logfile;
    int ret = system(cmd.c_str());
    if (WEXITSTATUS(ret) != EXIT_SUCCESS) {
      std::cout << "compilation failed, see " << logfile << std::endl;
      exit(EXIT_FAILURE);
    }

    cmd = "mv -n " + tmplibfile + " " + libfile;
    ret = system(cmd.c_str());
    cmd = "rm -rf " + tmplibfile;
    ret = system(cmd.c_str());

    return arrow::Status::OK();
  }

  arrow::Status LoadLibrary(std::string signature) {
    std::string outpath = "/tmp";
    std::string prefix = "/spark-columnar-plugin-codegen-";
    std::string libfile = outpath + prefix + signature + ".so";
    // load dynamic library
    void* dynlib = dlopen(libfile.c_str(), RTLD_LAZY);
    if (!dynlib) {
      return arrow::Status::Invalid(libfile, " is not generated");
    }

    // loading symbol from library and assign to pointer
    // (to be cast to function pointer later)
    *(void**)(&ConditionShuffleCodeGen) = dlsym(dynlib, "ConditionShuffleCodeGen");
    const char* dlsym_error = dlerror();
    if (dlsym_error != NULL) {
      std::cerr << "error loading symbol:\n" << dlsym_error << std::endl;
      exit(EXIT_FAILURE);
    }

    return arrow::Status::OK();
  }

  class ConditionShuffleCodeGenResultIterator
      : public ResultIterator<arrow::RecordBatch> {
   public:
    ConditionShuffleCodeGenResultIterator(
        arrow::compute::FunctionContext* ctx, std::vector<ArrayList> input_cache,
        std::function<arrow::Status(std::shared_ptr<arrow::Array>, std::vector<ArrayList>,
                                    std::shared_ptr<arrow::Array>, ArrayList,
                                    std::shared_ptr<arrow::RecordBatch>* out)>
            eval_func,
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> in_indices_iter)
        : ctx_(ctx),
          input_cache_(input_cache),
          eval_func_(eval_func),
          in_indices_iter_(in_indices_iter) {}

    std::string ToString() override { return "ConditionedShuffleArraysResultIterator"; }

    arrow::Status Process(
        std::vector<std::shared_ptr<arrow::Array>> in,
        std::shared_ptr<arrow::RecordBatch>* out,
        const std::shared_ptr<arrow::Array>& selection = nullptr) override {
      std::shared_ptr<arrow::RecordBatch> indices_out;
      if (!in_indices_iter_) {
        return arrow::Status::Invalid(
            "ConditionShuffleCodeGenResultIterator in_indices_iter_ is not set.");
      }
      RETURN_NOT_OK(in_indices_iter_->GetResult(&indices_out));
      auto left_selection = indices_out->column(0);
      auto right_selection = indices_out->column(1);
      RETURN_NOT_OK(eval_func_(left_selection, input_cache_, right_selection, in, out));
      return arrow::Status::OK();
    }

   private:
    arrow::compute::FunctionContext* ctx_;
    std::function<arrow::Status(std::shared_ptr<arrow::Array>, std::vector<ArrayList>,
                                std::shared_ptr<arrow::Array>, ArrayList,
                                std::shared_ptr<arrow::RecordBatch>* out)>
        eval_func_;
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> in_indices_iter_;
    std::vector<ArrayList> input_cache_;
  };
};

arrow::Status ConditionedShuffleArrayListKernel::Make(
    arrow::compute::FunctionContext* ctx, std::shared_ptr<gandiva::Node> func_node,
    std::vector<std::shared_ptr<arrow::Field>> left_field_list,
    std::vector<std::shared_ptr<arrow::Field>> right_field_list,
    std::vector<std::shared_ptr<arrow::Field>> output_field_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ConditionedShuffleArrayListKernel>(
      ctx, func_node, left_field_list, right_field_list, output_field_list);
  return arrow::Status::OK();
}

ConditionedShuffleArrayListKernel::ConditionedShuffleArrayListKernel(
    arrow::compute::FunctionContext* ctx, std::shared_ptr<gandiva::Node> func_node,
    std::vector<std::shared_ptr<arrow::Field>> left_field_list,
    std::vector<std::shared_ptr<arrow::Field>> right_field_list,
    std::vector<std::shared_ptr<arrow::Field>> output_field_list) {
  impl_.reset(
      new Impl(ctx, func_node, left_field_list, right_field_list, output_field_list));
  kernel_name_ = "ConditionedShuffleArrayListKernel";
}

arrow::Status ConditionedShuffleArrayListKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status ConditionedShuffleArrayListKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

arrow::Status ConditionedShuffleArrayListKernel::SetDependencyIter(
    const std::shared_ptr<ResultIterator<arrow::RecordBatch>>& in, int index) {
  return impl_->SetDependencyIter(in, index);
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
