/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/client/PmPoolClient.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/client
 * Created Date: Friday, December 13th 2019, 3:43:04 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_CLIENT_PMPOOLCLIENT_H_
#define PMPOOL_CLIENT_PMPOOLCLIENT_H_

#define INITIAL_BUFFER_NUMBER 64

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Client.h>
#include <HPNL/Connection.h>

#include <atomic>
#include <condition_variable>  // NOLINT
#include <functional>
#include <future>  // NOLINT
#include <iostream>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../Base.h"
#include "../Common.h"
#include "../ThreadWrapper.h"

class NetworkClient;
class RequestHandler;
class Function;

using std::atomic;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::vector;

class PmPoolClient {
 public:
  PmPoolClient() = delete;
  PmPoolClient(const string &remote_address, const string &remote_port);
  ~PmPoolClient();
  int init();

  /// memory pool interface
  void begin_tx();
  /// Allocate the given size of memory from remote memory pool.
  /// Return the global address of memory pool.
  uint64_t alloc(uint64_t size);

  /// Free memory with the global address.
  /// Address is the global address that returned by alloc.
  /// Return 0 if succeed, return others value if fail.
  int free(uint64_t address);

  /// Write data to the address of remote memory pool.
  /// The size is number of bytes
  /// Return 0 if succeed, return others value if fail.
  int write(uint64_t address, const char *data, uint64_t size);

  /// Return global address if succeed, return -1 if fail.
  uint64_t write(const char *data, uint64_t size);

  /// Read from the global address of remote memory pool and copy to data
  /// pointer.
  /// Return 0 if succeed, return others value if fail.
  int read(uint64_t address, char *data, uint64_t size);

  int read(uint64_t address, char *data, uint64_t size,
           std::function<void(int)> func);
  void end_tx();

  /// key-value storage interface
  uint64_t put(const string &key, const char *value, uint64_t size);
  vector<block_meta> get(const string &key);
  int del(const string &key);

  void shutdown();
  void wait();

 private:
  shared_ptr<RequestHandler> requestHandler_;
  shared_ptr<NetworkClient> networkClient_;
  atomic<uint64_t> rid_ = {0};
  std::mutex tx_mtx;
  std::condition_variable tx_con;
  bool tx_finished;
  std::mutex op_mtx;
  bool op_finished;
};

#endif  // PMPOOL_CLIENT_PMPOOLCLIENT_H_
