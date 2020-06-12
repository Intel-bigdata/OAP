/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Digest.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_DIGEST_H_
#define PMPOOL_DIGEST_H_

#include <cstdint>
#include <string>

#include <HPNL/ChunkMgr.h>
#include "xxhash/xxhash.h"
#include "xxhash/xxhash.hpp"

using std::string;

class Digest {
 public:
  Digest() = default;
  static void computeKeyHash(const string &key, uint64_t *hash) {
    *hash = xxh::xxhash<64>(key);
  }
};

#endif  // PMPOOL_DIGEST_H_
