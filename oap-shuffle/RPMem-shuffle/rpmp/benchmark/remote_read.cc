/*
 * Filename: /mnt/spark-pmof/tool/rpmp/benchmark/allocate_perf.cc
 * Path: /mnt/spark-pmof/tool/rpmp/benchmark
 * Created Date: Friday, December 20th 2019, 8:29:23 am
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include <string.h>
#include <atomic>
#include <thread>  // NOLINT
#include "pmpool/Event.h"
#include "pmpool/client/PmPoolClient.h"

char str[1048576];
char str_read[1048576];
std::atomic<uint64_t> count = {0};
std::mutex mtx;
std::vector<PmPoolClient *> clients;
std::vector<uint64_t> addresses;
uint64_t buffer_size = 1024*64;
uint64_t buffer_num = 1000000;
int thread_num = 1;

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

void func(uint64_t i) {
  while (true) {
    uint64_t count_ = count++;
    if (count_ < buffer_num) {
      clients[i]->begin_tx();
      clients[i]->read(addresses[i], str_read, buffer_size);
      clients[i]->end_tx();
    } else {
      break;
    }
  }
}

int main() {
  std::vector<std::thread *> threads;

  memset(str, '0', buffer_size);
  for (int i = 0; i < thread_num; i++) {
    PmPoolClient *client = new PmPoolClient("172.168.0.40", "12346");
    client->init();
    client->begin_tx();
    addresses.push_back(client->write(str, buffer_size));
    client->end_tx();
    clients.push_back(client);
  }
  uint64_t start = timestamp_now();
  for (int i = 0; i < thread_num; i++) {
    auto t = new std::thread(func, i);
    threads.push_back(t);
  }
  for (int i = 0; i < thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  uint64_t end = timestamp_now();
  std::cout << "pmemkv put test: " << buffer_size << " "
            << " bytes test, consumes " << (end - start) / 1000.0
            << "s, throughput is "
            << buffer_num / 1024.0 * buffer_size / 1024.0 /
                   ((end - start) / 1000.0)
            << "MB/s" << std::endl;
  for (int i = 0; i < thread_num; i++) {
    clients[i]->begin_tx();
    clients[i]->free(addresses[i]);
    clients[i]->end_tx();
  }
  std::cout << "finished." << std::endl;
  for (int i = 0; i < thread_num; i++) {
    clients[i]->wait();
  }
  return 0;
}
