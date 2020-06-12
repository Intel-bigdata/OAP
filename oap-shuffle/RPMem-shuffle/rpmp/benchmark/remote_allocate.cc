/*
 * Filename: /mnt/spark-pmof/tool/rpmp/benchmark/allocate_perf.cc
 * Path: /mnt/spark-pmof/tool/rpmp/benchmark
 * Created Date: Friday, December 20th 2019, 8:29:23 am
 * Author: root
 * 
 * Copyright (c) 2019 Intel
 */

#include <thread> // NOLINT
#include <atomic>
#include "pmpool/client/PmPoolClient.h"

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

std::atomic<uint64_t> count = {0};
std::mutex mtx;
std::vector<PmPoolClient *> clients;
std::map<int, std::vector<uint64_t>> addresses;

void func(int i) {
  while (true) {
    uint64_t count_ = count++;
    if (count_ < 20480) {
      clients[i]->begin_tx();
      if (addresses.count(i) != 0) {
        auto vec = addresses[i];
        uint64_t addr = clients[i]->alloc(1048576);
        vec.push_back(addr);
      } else {
        std::vector<uint64_t> vec;
        uint64_t addr = clients[i]->alloc(1048576);
        vec.push_back(addr);
        addresses[i] = vec;
      }
      clients[i]->end_tx();
    } else {
      break;
    }
  }
}

int main() {
  std::vector<std::thread*> threads;
  int num = 0;
  for (int i = 0; i < 4; i++) {
    PmPoolClient *client = new PmPoolClient("172.168.0.40", "12346");
    client->begin_tx();
    client->init();
    client->end_tx();
    clients.push_back(client);
    num++;
  }
  uint64_t start = timestamp_now();
  for (int i = 0; i < num; i++) {
    auto t = new std::thread(func, i);
    threads.push_back(t);
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

  for (int i = 0; i < num; i++) {
    auto vec = addresses[i];
    while (!vec.empty()) {
      auto address = vec.back();
      vec.pop_back();
      clients[i]->free(address);
    }
  }
  std::cout << "freed." << std::endl;
  for (int i = 0; i < num; i++) {
    clients[i]->wait();
    delete clients[i];
  }
  return 0;
}
