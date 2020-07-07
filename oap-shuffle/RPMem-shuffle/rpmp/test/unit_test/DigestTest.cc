/*
 * Filename: /mnt/spark-pmof/tool/rpmp/test/DigestTest.cc
 * Path: /mnt/spark-pmof/tool/rpmp/test
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 * 
 * Copyright (c) 2019 Intel
 */

#include <iostream>
#include <memory>
#include <string>

#include "pmpool/Digest.h"
#include "gtest/gtest.h"

TEST(digest, compute) {
  std::string str = "hello world";
  uint64_t hash_value_1;
  uint64_t hash_value_2;
  Digest::computeKeyHash(str, &hash_value_1);
  Digest::computeKeyHash(str, &hash_value_2);
  ASSERT_TRUE(hash_value_1 == hash_value_2);
}
