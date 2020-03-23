/*
 * Filename: /mnt/spark-pmof/tool/rpmp/benchmark/allocate_perf.cc
 * Path: /mnt/spark-pmof/tool/rpmp/benchmark
 * Created Date: Friday, December 20th 2019, 8:29:23 am
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include <string.h>
#include <thread>  // NOLINT
#include "pmpool/client/PmPoolClient.h"

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

int count = 0;
std::mutex mtx;
uint64_t addresses[20480];
char str[1048576];

void func(PmPoolClient* client) {
  while (true) {
    std::unique_lock<std::mutex> lk(mtx);
    uint64_t count_ = count++;
    lk.unlock();
    if (count_ < 20480) {
      client->begin_tx();
      auto addr = client->alloc(1048576);
      client->end_tx();
      addresses[count_] = addr;
    } else {
      break;
    }
  }
}

void func1(PmPoolClient* client) {
  while (true) {
    std::unique_lock<std::mutex> lk(mtx);
    uint64_t count_ = count++;
    lk.unlock();
    if (count_ < 20480) {
      client->begin_tx();
      client->write(addresses[count_], str, 1048576);
      client->end_tx();
    } else {
      break;
    }
  }
}

int main() {
  std::vector<std::thread*> threads;
  PmPoolClient client("172.168.0.40", "12346");
  memset(str, '0', 1048576);
  client.init();

  int num = 0;
  for (int i = 0; i < 1; i++) {
    num++;
    auto t = new std::thread(func, &client);
    threads.push_back(t);
  }
  for (int i = 0; i < num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  std::cout << "start write." << std::endl;
  num = 0;
  count = 0;
  std::vector<std::thread*> threads_1;
  uint64_t start = timestamp_now();
  for (int i = 0; i < 8; i++) {
    num++;
    auto t = new std::thread(func1, &client);
    threads_1.push_back(t);
  }
  for (int i = 0; i < num; i++) {
    threads_1[i]->join();
    delete threads_1[i];
  }
  uint64_t end = timestamp_now();
  std::cout << "pmemkv put test: 1048576 "
            << " bytes test, consumes " << (end - start) / 1000.0
            << "s, throughput is " << 20480 / ((end - start) / 1000.0) << "MB/s"
            << std::endl;
  for (int i = 0; i < 20480; i++) {
    client.begin_tx();
    client.free(addresses[i]);
    client.end_tx();
  }
  std::cout << "freed." << std::endl;
  client.wait();
  return 0;
}
