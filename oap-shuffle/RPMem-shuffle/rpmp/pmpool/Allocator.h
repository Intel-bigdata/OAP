/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Allocator.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Monday, December 9th 2019, 9:06:37 am
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_ALLOCATOR_H_
#define PMPOOL_ALLOCATOR_H_

#include <stdint.h>

#include <string>

class Chunk;

using std::string;

typedef uint64_t ptr_t;

#define TO_GLOB(addr, base, wid) \
  ((ptr_t)(addr) - (ptr_t)(base) + ((ptr_t)(wid) << 48))
#define GET_WID(global_address) ((ptr_t)(global_address) >> 48)

struct Addr {
  uint32_t aid;
  uint64_t offset;
  uint64_t size;
};

struct DiskInfo {
  DiskInfo(string& path_, uint64_t size_) : path(path_), size(size_) {}
  string path;
  uint64_t size;
};

class Allocator {
 public:
  virtual int init() = 0;
  virtual uint64_t allocate_and_write(uint64_t buffer_size,
                                      const char* content = nullptr) = 0;
  virtual int write(uint64_t address, const char* content, uint64_t size) = 0;
  virtual int release(uint64_t address) = 0;
  virtual int release_all() = 0;
  virtual int dump_all() = 0;
  virtual uint64_t get_virtual_address(uint64_t address) = 0;
  virtual Chunk* get_rma_chunk() = 0;
};
#endif  // PMPOOL_ALLOCATOR_H_
