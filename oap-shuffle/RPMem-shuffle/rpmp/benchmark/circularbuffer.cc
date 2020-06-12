/*
 * Filename: /mnt/spark-pmof/tool/rpmp/benchmark/circularbuffer.cc
 * Path: /mnt/spark-pmof/tool/rpmp/benchmark
 * Created Date: Monday, December 30th 2019, 9:57:10 am
 * Author: root
 *
 * Copyright (c) 2019 Your Company
 */

#include <string.h>

#include "pmpool/buffer/CircularBuffer.h"

#include <chrono>  // NOLINT

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

int main() {
  CircularBuffer circularbuffer(1024 * 1024, 2048);
  uint64_t start = timestamp_now();
  char str[1048576];
  memset(str, '0', 1048576);
  for (int i = 0; i < 20480; i++) {
    char* buf = circularbuffer.get(1048576);
    memcpy(buf, str, 1048576);
    circularbuffer.put(buf, 1048576);
  }
  uint64_t end = timestamp_now();
  std::cout << "pmemkv put test: 1048576 "
            << " bytes test, consumes " << (end - start) / 1000.0 << std::endl;
  return 0;
}