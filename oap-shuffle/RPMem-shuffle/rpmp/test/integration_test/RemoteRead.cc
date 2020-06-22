/*
 * Filename: /mnt/spark-pmof/tool/rpmp/benchmark/allocate_perf.cc
 * Path: /mnt/spark-pmof/tool/rpmp/benchmark
 * Created Date: Friday, December 20th 2019, 8:29:23 am
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include <string.h>
#include <assert.h>
#include <thread>  // NOLINT
#include <vector>
#include "pmpool/client/PmPoolClient.h"

std::vector<char*> strs;
char str_read[4096];
int count = 0;
std::mutex mtx;
uint64_t address[2];

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

void func(PmPoolClient* client) {
  while (true) {
    std::unique_lock<std::mutex> lk(mtx);
    uint64_t count_ = count++;
    lk.unlock();
    if (count_ < 2) {
      client->read(address[count_], str_read, strlen(strs[count_]));
      assert(strncmp(str_read, strs[count_], strlen(strs[count_])) == 0);
    } else {
      break;
    }
  }
}

int main() {
  char str[] = "hello world";
  char str1[] = "hello rpmp";
  strs.push_back(str);
  strs.push_back(str1);
  std::vector<std::thread*> threads;
  int num = 0;
  PmPoolClient client("172.168.0.40", "12346");
  client.init();
  address[0] = client.write(strs[0], strlen(strs[0]));
  address[1] = client.write(strs[1], strlen(strs[1]));
  for (int i = 0; i < 1; i++) {
    num++;
    auto t = new std::thread(func, &client);
    threads.push_back(t);
  }
  for (int i = 0; i < num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  client.free(address[0]);
  client.free(address[1]);
  std::cout << "finished." << std::endl;
  client.shutdown();
  client.wait();
  return 0;
}
