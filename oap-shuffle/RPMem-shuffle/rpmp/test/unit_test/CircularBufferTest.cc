/*
 * Filename: /mnt/spark-pmof/tool/rpmp/test/CircularBufferTest.cc
 * Path: /mnt/spark-pmof/tool/rpmp/test
 * Created Date: Tuesday, December 24th 2019, 8:56:37 am
 * Author: root
 * 
 * Copyright (c) 2019 Intel
 */

#include <thread>  // NOLINT
#include <chrono>  // NOLINT
#include <iostream>

#include "../pmpool/buffer/CircularBuffer.h"
#include "gtest/gtest.h"

#define private public

TEST(circularbuffer, 1B) {
  CircularBuffer buffer(1, 10);
  uint64_t addr = 0;
  buffer.get(2, &addr);
  ASSERT_EQ(addr, 0);
  ASSERT_EQ(buffer.get_write_(), 2);
  buffer.get(2, &addr);
  ASSERT_EQ(addr, 2);
  ASSERT_EQ(buffer.get_write_(), 4);
  buffer.get(2, &addr);
  ASSERT_EQ(addr, 4);
  ASSERT_EQ(buffer.get_write_(), 6);
  buffer.get(2, &addr);
  ASSERT_EQ(addr, 6);
  ASSERT_EQ(buffer.get_write_(), 8);
  buffer.get(2, &addr);
  ASSERT_EQ(addr, 8);
  ASSERT_EQ(buffer.get_write_(), 0);
  buffer.put(addr, 2);
  ASSERT_EQ(buffer.get_read_(), 0);
  addr = 0;
  buffer.put(addr, 4);
  ASSERT_EQ(buffer.get_read_(), 4);
  buffer.get(3, &addr);
  ASSERT_EQ(addr, 0);
  ASSERT_EQ(buffer.get_write_(), 3);
  buffer.put(4, 4);
  ASSERT_EQ(buffer.get_read_(), 0);
  buffer.get(4, &addr);
  ASSERT_EQ(addr, 3);
  ASSERT_EQ(buffer.get_write_(), 7);
  buffer.get(3, &addr);
  ASSERT_EQ(addr, 7);
  ASSERT_EQ(buffer.get_write_(), 0);
  buffer.put(5, 4);
  ASSERT_EQ(buffer.get_read_(), 0);
  buffer.put(1, 4);
  ASSERT_EQ(buffer.get_read_(), 0);
  addr = 0;
  buffer.put(addr, 1);
  ASSERT_EQ(buffer.get_read_(), 9);
  buffer.put(9, 1);
  ASSERT_EQ(buffer.get_read_(), 0);
}

TEST(circularbuffer, 4K) {
  CircularBuffer buffer(4096, 4);
  uint64_t addr = 0;
  buffer.get(10, &addr);
  ASSERT_EQ(addr, 0);
  ASSERT_EQ(buffer.get_write_(), 1);
  buffer.get(10, &addr);
  ASSERT_EQ(addr, 1);
  ASSERT_EQ(buffer.get_write_(), 2);
  buffer.get(4097, &addr);
  ASSERT_EQ(addr, 2);
  ASSERT_EQ(buffer.get_write_(), 0);
  buffer.put(2, 2);
  ASSERT_EQ(buffer.get_read_(), 0);
  buffer.put(3, 2);
  ASSERT_EQ(buffer.get_read_(), 0);
  addr = 0;
  buffer.put(addr, 2);
  ASSERT_EQ(buffer.get_read_(), 1);
  buffer.put(1, 2);
  ASSERT_EQ(buffer.get_read_(), 0);
}

void func(CircularBuffer* buffer) {
  std::cout << "sleep..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));
  uint64_t addr = 0;
  buffer->put(addr, 2);
  std::cout << "put buffer [0, 1]..." << std::endl;
  buffer->dump();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  buffer->put(4, 2);
  std::cout << "put buffer [4, 5]..." << std::endl;
  buffer->dump();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  buffer->put(2, 2);
  std::cout << "put buffer [2, 3]..." << std::endl;
  buffer->dump();
}

TEST(circularbuffer, multithread) {
  CircularBuffer buffer(1, 8);
  uint64_t addr = 0;
  buffer.get(6, &addr);
  ASSERT_EQ(addr, 0);
  std::thread t(func, &buffer);
  buffer.get(8, &addr);
  std::cout << "get buffer..." << std::endl;
  ASSERT_EQ(addr, 0);
  t.join();
}
