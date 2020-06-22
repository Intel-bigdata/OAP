/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/client/NetworkClient.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/client
 * Created Date: Wednesday, December 11th 2019, 2:02:46 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_CLIENT_NETWORKCLIENT_H_
#define PMPOOL_CLIENT_NETWORKCLIENT_H_

#include <HPNL/Callback.h>
#include <HPNL/Client.h>

#include <atomic>
#include <condition_variable>  // NOLINT
#include <cstring>
#include <future>  // NOLINT
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "../Event.h"
#include "../RmaBufferRegister.h"
#include "../ThreadWrapper.h"
#include "../queue/blockingconcurrentqueue.h"
#include "../queue/concurrentqueue.h"

using moodycamel::BlockingConcurrentQueue;
using std::atomic;
using std::condition_variable;
using std::future;
using std::make_shared;
using std::mutex;
using std::promise;
using std::shared_ptr;
using std::string;
using std::unique_lock;
using std::unordered_map;

class NetworkClient;
class CircularBuffer;
class Connection;
class ChunkMgr;

typedef promise<RequestReplyContext> Promise;
typedef future<RequestReplyContext> Future;

class RequestHandler {
 public:
  explicit RequestHandler(NetworkClient *networkClient);
  ~RequestHandler() = default;
  void addTask(Request *request);
  void addTask(Request *request, std::function<void()> func);
  void notify(RequestReply *requestReply);
  void wait();
  RequestReplyContext &get();

 private:
  void handleRequest(Request *request);

 private:
  NetworkClient *networkClient_;
  BlockingConcurrentQueue<Request *> pendingRequestQueue_;
  std::mutex h_mtx;
  unordered_map<uint64_t, std::function<void()>> callback_map;
  uint64_t total_num = 0;
  uint64_t begin = 0;
  uint64_t end = 0;
  uint64_t time = 0;
  bool op_finished = false;
  std::condition_variable cv;
  RequestReplyContext requestReplyContext;
};

class ClientShutdownCallback : public Callback {
 public:
  ClientShutdownCallback() {}
  ~ClientShutdownCallback() = default;
  void operator()(void *param_1, void *param_2) {}
};

class ClientConnectedCallback : public Callback {
 public:
  explicit ClientConnectedCallback(NetworkClient *networkClient);
  ~ClientConnectedCallback() = default;
  void operator()(void *param_1, void *param_2);

 private:
  NetworkClient *networkClient_;
};

class ClientRecvCallback : public Callback {
 public:
  ClientRecvCallback(ChunkMgr *chunkMgr, RequestHandler *requestHandler);
  ~ClientRecvCallback() = default;
  void operator()(void *param_1, void *param_2);

 private:
  ChunkMgr *chunkMgr_;
  RequestHandler *requestHandler_;
  uint64_t count_ = 0;
  uint64_t time = 0;
  uint64_t start = 0;
  uint64_t end = 0;
  std::mutex mtx;
};

class ClientSendCallback : public Callback {
 public:
  explicit ClientSendCallback(ChunkMgr *chunkMgr) : chunkMgr_(chunkMgr) {}
  ~ClientSendCallback() = default;
  void operator()(void *param_1, void *param_2) {
    auto buffer_id_ = *static_cast<int *>(param_1);
    auto ck = chunkMgr_->get(buffer_id_);
    chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
  }

 private:
  ChunkMgr *chunkMgr_;
};

class NetworkClient : public RmaBufferRegister {
 public:
  friend ClientConnectedCallback;
  NetworkClient() = delete;
  NetworkClient(const string &remote_address, const string &remote_port);
  NetworkClient(const string &remote_address, const string &remote_port,
                int worker_num, int buffer_num_per_con, int buffer_size,
                int init_buffer_num);
  ~NetworkClient();
  int init(RequestHandler *requesthandler);
  void shutdown();
  void wait();
  Chunk *register_rma_buffer(char *rma_buffer, uint64_t size) override;
  void unregister_rma_buffer(int buffer_id) override;
  uint64_t get_dram_buffer(const char *data, uint64_t size);
  void reclaim_dram_buffer(uint64_t src_address, uint64_t size);
  uint64_t get_rkey();
  void connected(Connection *con);
  void send(char *data, uint64_t size);
  void read(Request *request);

 private:
  string remote_address_;
  string remote_port_;
  int worker_num_;
  int buffer_num_per_con_;
  int buffer_size_;
  int init_buffer_num_;
  Client *client_;
  ChunkMgr *chunkMgr_;
  Connection *con_;
  ClientShutdownCallback *shutdownCallback;
  ClientConnectedCallback *connectedCallback;
  ClientRecvCallback *recvCallback;
  ClientSendCallback *sendCallback;
  mutex con_mtx;
  bool connected_;
  condition_variable con_v;
  shared_ptr<CircularBuffer> circularBuffer_;
  atomic<uint64_t> buffer_id_{0};
};

#endif  // PMPOOL_CLIENT_NETWORKCLIENT_H_
