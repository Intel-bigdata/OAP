/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/fb/Encoder.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/fb
 * Created Date: Friday, December 27th 2019, 3:05:51 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_BASE_H_
#define PMPOOL_BASE_H_

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <iostream>

#define CHK_ERR(function_name, result)                              \
  {                                                                 \
    if (result) {                                                   \
      fprintf(stderr, "%s: %s\n", function_name, strerror(result)); \
      return result;                                                \
    }                                                               \
  }

struct RequestMsg {
  uint32_t type;
  uint64_t rid;
  uint64_t address;
  uint64_t src_address;
  uint64_t src_rkey;
  uint64_t size;
  uint64_t key;
};

struct RequestReplyMsg {
  uint32_t type;
  uint32_t success;
  uint64_t rid;
  uint64_t address;
  uint64_t size;
  uint64_t key;
};

struct block_meta {
  block_meta() : block_meta(0, 0) {}
  block_meta(uint64_t _address, uint64_t _size)
      : address(_address), size(_size) {}
  uint64_t address;
  uint64_t size;
};

#endif  // PMPOOL_BASE_H_
