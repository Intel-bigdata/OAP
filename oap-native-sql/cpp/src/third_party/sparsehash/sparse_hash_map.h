#include <arrow/compute/context.h>
#include <arrow/status.h>
#include "sparsehash/dense_hash_map"

using google::dense_hash_map;

#define NOTFOUND -1

template <typename Scalar>
class SparseHashMap {
 public:
  SparseHashMap() { dense_map_.set_empty_key(0); }
  SparseHashMap(arrow::MemoryPool* pool) { dense_map_.set_empty_key(0); }
  template <typename Func1, typename Func2>
  arrow::Status GetOrInsert(const Scalar& value, Func1&& on_found, Func2&& on_not_found,
                            int32_t* out_memo_index) {
    if (dense_map_.find(value) == dense_map_.end()) {
      auto index = size_++;
      dense_map_[value] = index;
      *out_memo_index = index;
      on_not_found(index);
    } else {
      auto index = dense_map_[value];
      *out_memo_index = index;
      on_found(index);
    }
    return arrow::Status::OK();
  }
  template <typename Func1, typename Func2>
  int32_t GetOrInsertNull(Func1&& on_found, Func2&& on_not_found) {
    if (!null_index_set_) {
      null_index_set_ = true;
      null_index_ = size_++;
      on_not_found(null_index_);
    } else {
      on_found(null_index_);
    }
    return null_index_;
  }
  int32_t Get(const Scalar& value) {
    if (dense_map_.find(value) == dense_map_.end()) {
      return NOTFOUND;
    } else {
      auto ret = dense_map_[value];
      return ret;
    }
  }
  int32_t GetNull() {
    if (!null_index_set_) {
      return NOTFOUND;
    } else {
      auto ret = null_index_;
      return ret;
    }
  }

 private:
  dense_hash_map<Scalar, int32_t> dense_map_;
  int32_t size_ = 0;
  bool null_index_set_ = false;
  int32_t null_index_;
};
