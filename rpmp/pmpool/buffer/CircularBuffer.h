/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/buffer/CircularBuffer.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/buffer
 * Created Date: Monday, December 23rd 2019, 2:31:42 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_BUFFER_CIRCULARBUFFER_H_
#define PMPOOL_BUFFER_CIRCULARBUFFER_H_

#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>  // NOLINT
#include <iostream>
#include <mutex>  // NOLINT
#include <vector>

#include "../Common.h"
#include "../NetworkServer.h"
#include "../RmaBufferRegister.h"

#define p2align(x, a) (((x) + (a)-1) & ~((a)-1))

class CircularBuffer {
 public:
  CircularBuffer() = delete;
  CircularBuffer(const CircularBuffer &) = delete;
  CircularBuffer(uint64_t buffer_size, uint32_t buffer_num,
                 bool is_server = false, RmaBufferRegister *rbr = nullptr)
      : buffer_size_(buffer_size),
        buffer_num_(buffer_num),
        rbr_(rbr),
        read_(0),
        write_(0) {
    uint64_t total = buffer_num_ * buffer_size_;
    buffer_ = static_cast<char *>(mmap(0, buffer_num_ * buffer_size_,
                                       PROT_READ | PROT_WRITE,
                                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));

    // for the consideration of high performance,
    // we'd better do memory paging before starting service.
    // if (is_server) {
    //  for (uint64_t i = 0; i < total; i++) {
    //    buffer_[i] = 0;
    //  }
    // }

    if (rbr_) {
      ck_ = rbr_->register_rma_buffer(buffer_, buffer_num_ * buffer_size_);
    }

    for (int i = 0; i < buffer_num; i++) {
      bits.push_back(0);
    }
  }
  ~CircularBuffer() {
    munmap(buffer_, buffer_num_ * buffer_size_);
    buffer_ = nullptr;
  }
  char *get(uint64_t bytes) {
    uint64_t offset = 0;
    bool res = get(bytes, &offset);
    if (res == false) {
      return nullptr;
    }
    return buffer_ + offset * buffer_size_;
  }
  void put(const char *data, uint64_t bytes) {
    assert((data - buffer_) % buffer_size_ == 0);
    uint64_t offset = (data - buffer_) / buffer_size_;
    put(offset, bytes);
  }

  void dump() {
    std::cout << "********************************************" << std::endl;
    std::cout << "read_ " << read_ << " write_ " << write_ << std::endl;
    for (int i = 0; i < buffer_num_; i++) {
      std::cout << bits[i] << " ";
    }
    std::cout << std::endl;
    std::cout << "********************************************" << std::endl;
  }
  uint64_t get_read_() { return read_; }
  uint64_t get_write_() { return write_; }

  bool get(uint64_t bytes, uint64_t *offset) {
    uint32_t alloc_num = p2align(bytes, buffer_size_) / buffer_size_;
    if (alloc_num > buffer_num_) {
      return false;
    }
    std::lock_guard<spin_mutex> write_lk(write_mtx);
    std::unique_lock<std::mutex> read_lk(read_mtx);
    uint64_t available = 0;
    uint64_t end = 0;
    uint64_t index = 0;
  read_lt_write:
    if (write_ >= read_) {  // --------read_--------write_--------
      available = buffer_num_ - write_;
      if (available >= alloc_num) {
        index = write_;
        end = write_ + alloc_num;
        while (index < end) {
          bits[index++] = 1;
        }
        *offset = write_;
        write_ += alloc_num;
        if (write_ == buffer_num_) {
          write_ = 0;
        }
        goto success;
      } else {
        uint64_t index = write_;
        while (index < buffer_num_) {
          bits[index++] = 0;
        }
        write_ = 0;
        goto write_lt_read;
      }
    }
  write_lt_read:
    // --------write_--------read_-----------
    available = read_ - write_;
    if (available >= alloc_num) {
      index = write_;
      end = write_ + alloc_num;
      while (index < end) {
        bits[index++] = 1;
      }
      *offset = write_;
      write_ += alloc_num;
      if (write_ == buffer_num_) {
        write_ = 0;
      }
      goto success;
    } else {
      // wait
      while ((available = read_ - write_) < alloc_num) {
        read_cv.wait(read_lk);
        if (read_ == 0) {
          goto read_lt_write;
        }
      }
      index = write_;
      end = write_ + alloc_num;
      while (index < end) {
        bits[index++] = 1;
      }
      *offset = write_;
      write_ += alloc_num;
      if (write_ == buffer_num_) {
        write_ = 0;
      }
      goto success;
    }
  success:
    return true;
  }
  void put(uint64_t offset, uint64_t bytes) {
    uint32_t alloc_num = p2align(bytes, buffer_size_) / buffer_size_;
    assert(alloc_num <= buffer_num_ - read_);
    std::unique_lock<std::mutex> read_lk(read_mtx);
    uint64_t index = offset;
    uint64_t end = index + alloc_num;
    while (index < end) {
      bits[index] = 0;
      if (read_ == index) {
        read_++;
        if (read_ == buffer_num_) {
          read_ = 0;
        }
      }
      index++;
      read_cv.notify_all();
    }
    index = read_;
    while (bits[index] == 0) {
      read_++;
      index++;
      if (read_ == buffer_num_) {
        read_ = 0;
        read_cv.notify_all();
        break;
      } else {
        read_cv.notify_all();
      }
    }
  }
  Chunk *get_rma_chunk() { return ck_; }
  uint64_t get_offset(uint64_t data) { return (data - (uint64_t)buffer_); }

 private:
  char *buffer_;
  char *tmp_;
  uint64_t buffer_size_;
  uint64_t buffer_num_;
  RmaBufferRegister *rbr_;
  Chunk *ck_;
  std::vector<uint16_t> bits;
  uint64_t read_;
  uint64_t write_;
  std::mutex read_mtx;
  std::condition_variable read_cv;
  spin_mutex write_mtx;
  char tmp[4096];
};

#endif  // PMPOOL_BUFFER_CIRCULARBUFFER_H_
