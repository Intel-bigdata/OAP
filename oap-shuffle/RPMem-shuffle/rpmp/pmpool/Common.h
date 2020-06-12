/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Common.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Wednesday, January 15th 2020, 7:44:44 pm
 * Author: root
 *
 * Copyright (c) 2020 Your Company
 */

#ifndef PMPOOL_COMMON_H_
#define PMPOOL_COMMON_H_

#include <atomic>

class spin_mutex {
 public:
  std::atomic_flag flag = ATOMIC_FLAG_INIT;
  spin_mutex() = default;
  spin_mutex(const spin_mutex &) = delete;
  spin_mutex &operator=(const spin_mutex &) = delete;
  void lock() {
    while (flag.test_and_set(std::memory_order_acquire)) {
    }
  }
  void unlock() { flag.clear(std::memory_order_release); }
};

#endif  // PMPOOL_COMMON_H_
