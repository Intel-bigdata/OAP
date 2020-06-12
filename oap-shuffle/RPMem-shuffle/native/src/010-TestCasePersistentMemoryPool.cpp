#define CATCH_CONFIG_MAIN

#include "catch.hpp"
#include "pmemkv.h"
#include "PmemBuffer.h"

//#define LENGTH 262144 /*256KB*/
#define LENGTH 50
#define TOTAL_SIZE 10737418240

const char* expect_string = "hello world intel...";

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

int test_multithread_put(uint64_t index, pmemkv* kv) {
  std::string key = std::to_string(index);
  kv->put(key, expect_string, index);
  return 0;
}

TEST_CASE( "PmemBuffer operations", "[PmemBuffer]" ) {
  char data[LENGTH] = {};
  memset(data, 'a', LENGTH);

  PmemBuffer buf;

  SECTION( "write 256KB data to PmemBuffer" ) {
    buf.write(data, LENGTH);
    REQUIRE(buf.getRemaining() == LENGTH);
  }

  SECTION( "read 256KB data FROM PmemBuffer" ) {
    buf.write(data, LENGTH);
    char ret_data[LENGTH] = {};
    int size = buf.read(ret_data, LENGTH);
    REQUIRE(buf.getRemaining() == 0);
    REQUIRE(size == LENGTH);
  }

  SECTION( "read data exceeds remaining data size in PmemBuffer" ) {
    buf.write(data, LENGTH);
    char ret_data[LENGTH * 2] = {};
    int size = buf.read(ret_data, LENGTH * 2);
    REQUIRE(buf.getRemaining() == 0);
    REQUIRE(size == LENGTH);
  }

  SECTION( "do getDataForFlush twice check if only get same data once" ) {
    for (char c = 'a'; c < 'f'; c++) {
      memset(data + (c - 'a') * 3, c, 3);
    }
    /*data should be "aaabbbcccdddeee"*/
    buf.write(data, 15);

    char* firstTime = buf.getDataForFlush(buf.getRemaining());
    if (firstTime != nullptr) {
      firstTime[15] = 0;
      REQUIRE(strcmp(firstTime, "aaabbbcccdddeee") == 0);
    }

    char* secondTime = buf.getDataForFlush(buf.getRemaining());
    REQUIRE(secondTime == nullptr);
  }
}

TEST_CASE("pmemkv operations", "[pmemkv]") {
  SECTION("test open and close") {
    std::string key = "1";
    pmemkv* kv = new pmemkv("/dev/dax0.0");
    kv->put(key, "hello", 5);
    kv->put(key, " world", 6);
    delete kv;

    kv = new pmemkv("/dev/dax0.0");
    auto mb = (struct memory_block*)std::malloc(sizeof(struct memory_block));
    mb->data = (char*)std::malloc(11);
    mb->size = 11;
    kv->get(key, mb);
    const char* expect_string = "hello world";
    REQUIRE(strncmp(expect_string, mb->data, 11) == 0);
    std::free(mb->data);
    std::free(mb);
    kv->free_all();
    delete kv;
  }

  SECTION("test pmemkv metadata related operation") {
    std::string key = "1";
    pmemkv* kv = new pmemkv("/dev/dax0.0");
    kv->put(key, "hello", 5);
    kv->put(key, " world", 6);
    uint64_t size = 0;
    kv->get_meta_size(key, &size);
    struct memory_meta* mm = (struct memory_meta*)std::malloc(sizeof(struct memory_meta));
    mm->meta = (uint64_t*)std::malloc(size*2*sizeof(uint64_t));
    kv->get_meta(key, mm);
    uint64_t value_size = 0;
    for (int i = 0; i < size; i++) {
      value_size += mm->meta[i*2+1];
    }
    REQUIRE(value_size == 11);
    std::free(mm->meta);
    std::free(mm);
    kv->free_all();
    delete kv;
  }

  SECTION("test multithreaded put and get") {
    std::vector<std::thread> threads;
    pmemkv* kv = new pmemkv("/dev/dax0.0");
    for (uint64_t i = 0; i < 20; i++) {
      threads.emplace_back(test_multithread_put, i, kv);
    }
    for (uint64_t i = 0; i < 20; i++) {
      threads[i].join();
    }
    for (uint64_t i = 0; i < 20; i++) {
      struct memory_block* mb = (struct memory_block*)std::malloc(sizeof(struct memory_block));
      mb->data = (char*)std::malloc(i);
      mb->size = i;
      std::string key = std::to_string(i);
      kv->get(key, mb);
      REQUIRE(strncmp(mb->data, expect_string, i) == 0);
      std::free(mb->data);
      std::free(mb);
    }
    kv->free_all();
    delete kv;
    threads.clear();
  }

  SECTION("pmemkv put benchmark") {
    pmemkv* kv = new pmemkv("/dev/dax0.0");

    std::vector<uint64_t> benchmarks;
    benchmarks.push_back(4*1024*1024);
    //benchmarks.push_back(1*1024*1024);
    //benchmarks.push_back(512*1024);
    //benchmarks.push_back(256*1024);
    //benchmarks.push_back(128*1024);
    //benchmarks.push_back(64*1024);
    //benchmarks.push_back(32*1024);
    //benchmarks.push_back(16*1024);
    //benchmarks.push_back(8*1024);
    //benchmarks.push_back(4*1024);

    for (auto benchmark : benchmarks) {
      char* tmp = (char*)std::malloc(benchmark);
      memset(tmp, '0', benchmark);
      size_t count = TOTAL_SIZE/benchmark;
      uint64_t start = timestamp_now();
      for (size_t i = 0; i < count; i++) {
        std::string key = std::to_string(i);
        kv->put(key, tmp, benchmark);
      } 
      uint64_t end = timestamp_now();
      std::cout << "pmemkv put test: " << benchmark << " bytes test, consumes " << (end-start)/1000.0 << "s, throughput is " << TOTAL_SIZE/1024/1024/((end-start)/1000.0) << "MB/s" << std::endl;
      std::free(tmp);
    }
    kv->free_all();
    delete kv;
  }

  SECTION("pmemkv get benchmark") {
    pmemkv* kv = new pmemkv("/dev/dax0.0");

    std::vector<uint64_t> benchmarks;
    benchmarks.push_back(4*1024*1024);
    for (auto benchmark : benchmarks) {
      char* tmp = (char*)std::malloc(benchmark);
      memset(tmp, '0', benchmark);
      size_t count = TOTAL_SIZE/benchmark;
      uint64_t start = timestamp_now();
      for (size_t i = 0; i < count; i++) {
        std::string key = std::to_string(1);
        kv->put(key, tmp, benchmark);
      }
      uint64_t end = timestamp_now();
      std::free(tmp);
    }

    struct memory_block* mb = (struct memory_block*)std::malloc(sizeof(struct memory_block));
    mb->data = (char*)std::malloc(TOTAL_SIZE);
    mb->size = TOTAL_SIZE;
    std::string key = std::to_string(1);
    uint64_t start = timestamp_now();
    kv->get(key, mb);
    uint64_t end = timestamp_now();
    std::cout << "pmemkv get test: " << 1024*1024*4 << " bytes test, consumes " << (end-start)/1000.0 << "s, throughput is " << TOTAL_SIZE/1024/1024/((end-start)/1000.0) << "MB/s" << std::endl;
    kv->free_all();
    delete kv;
  }
}
