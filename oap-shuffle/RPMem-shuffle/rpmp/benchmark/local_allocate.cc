/*
 * Filename: /mnt/spark-pmof/tool/rpmp/benchmark/local_allocate.cc
 * Path: /mnt/spark-pmof/tool/rpmp/benchmark
 * Created Date: Tuesday, December 24th 2019, 8:54:38 am
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include <string.h>

#include <iostream>
#include <memory>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT
#include <vector>

#include "../pmpool/AllocatorProxy.h"
#include "../pmpool/Config.h"
#include "../pmpool/Log.h"
#include "gtest/gtest.h"

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

std::mutex mtx;
uint64_t count = 0;
char str[1048576];

void func(AllocatorProxy *proxy, int index) {
  while (true) {
    std::unique_lock<std::mutex> lk(mtx);
    uint64_t count_ = count++;
    lk.unlock();
    if (count_ < 20480) {
      uint64_t addr = proxy->allocate_and_write(1048576, nullptr, index);
      proxy->write(addr, str, 1048576);
    } else {
      break;
    }
  }
}

int main() {
  std::shared_ptr<Config> config = std::make_shared<Config>();
  config->init(0, nullptr);
  std::shared_ptr<Log> log = std::make_shared<Log>(config.get());
  auto allocatorProxy = new AllocatorProxy(config.get(), log.get(), nullptr);
  allocatorProxy->init();
  std::vector<std::thread *> threads;
  memset(str, '0', 1048576);

  uint64_t start = timestamp_now();
  int num = 0;
  for (int i = 0; i < 4; i++) {
    num++;
    auto t = new std::thread(func, allocatorProxy, i);
    threads.push_back(t);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    if (i == 0) {
      CPU_SET(2, &cpuset);
    } else if (i == 1) {
      CPU_SET(40, &cpuset);
    } else if (i == 2) {
      CPU_SET(27, &cpuset);
    } else {
      CPU_SET(60, &cpuset);
    }
    int rc =
        pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t), &cpuset);
  }
  for (int i = 0; i < num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  uint64_t end = timestamp_now();
  std::cout << "pmemkv put test: 1048576 "
            << " bytes test, consumes " << (end - start) / 1000.0
            << "s, throughput is " << 20480 / ((end - start) / 1000.0) << "MB/s"
            << std::endl;
  allocatorProxy->release_all();
}
