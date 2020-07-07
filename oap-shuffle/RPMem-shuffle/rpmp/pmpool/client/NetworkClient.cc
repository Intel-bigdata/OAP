/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/client/NetworkClient.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/client
 * Created Date: Monday, December 16th 2019, 1:16:16 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/client/NetworkClient.h"

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include "../Event.h"
#include "../buffer/CircularBuffer.h"

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

RequestHandler::RequestHandler(NetworkClient *networkClient)
    : networkClient_(networkClient) {}

void RequestHandler::addTask(Request *request) { handleRequest(request); }

void RequestHandler::addTask(Request *request, std::function<void()> func) {
  callback_map[request->get_rc().rid] = func;
  handleRequest(request);
}

void RequestHandler::wait() {
  unique_lock<mutex> lk(h_mtx);
  while (!op_finished) {
    cv.wait(lk);
  }
}

void RequestHandler::notify(RequestReply *requestReply) {
  unique_lock<mutex> lk(h_mtx);
  requestReplyContext = requestReply->get_rrc();
  op_finished = true;
  if (callback_map.count(requestReplyContext.rid) != 0) {
    callback_map[requestReplyContext.rid]();
    callback_map.erase(requestReplyContext.rid);
  } else {
    cv.notify_one();
    lk.unlock();
  }
}

void RequestHandler::handleRequest(Request *request) {
  op_finished = false;
  OpType rt = request->get_rc().type;
  switch (rt) {
    case ALLOC: {
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case FREE: {
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case WRITE: {
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case READ: {
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case PUT: {
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
    }
    case GET_META: {
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
    }
    default: {}
  }
}

RequestReplyContext &RequestHandler::get() { return requestReplyContext; }

ClientConnectedCallback::ClientConnectedCallback(NetworkClient *networkClient) {
  networkClient_ = networkClient;
}

void ClientConnectedCallback::operator()(void *param_1, void *param_2) {
  auto con = static_cast<Connection *>(param_1);
  networkClient_->connected(con);
}

ClientRecvCallback::ClientRecvCallback(ChunkMgr *chunkMgr,
                                       RequestHandler *requestHandler)
    : chunkMgr_(chunkMgr), requestHandler_(requestHandler) {}

void ClientRecvCallback::operator()(void *param_1, void *param_2) {
  int mid = *static_cast<int *>(param_1);
  auto ck = chunkMgr_->get(mid);

  // test start
  // auto con = reinterpret_cast<Connection*>(ck->con);
  // if (count_ == 0) {
  //   start = timestamp_now();
  // }
  // count_++;
  // if (count_ >= 1000000) {
  //   end = timestamp_now();
  //   std::cout << "consumes " << (end-start)/1000.0 << std::endl;
  //   return;
  // }
  // RequestContext rc = {};
  // rc.type = READ;
  // rc.rid = 0;
  // rc.size = 0;
  // rc.address = 0;
  // Request request(rc);
  // request.encode();
  // auto new_ck = chunkMgr_->get(con);
  // memcpy(new_ck->buffer, request.data_, request.size_);
  // new_ck->size = request.size_;
  // con->send(new_ck);
  // test end

  RequestReply requestReply(reinterpret_cast<char *>(ck->buffer), ck->size,
                            reinterpret_cast<Connection *>(ck->con));
  requestReply.decode();
  RequestReplyContext rrc = requestReply.get_rrc();
  switch (rrc.type) {
    case ALLOC_REPLY: {
      requestHandler_->notify(&requestReply);
      break;
    }
    case FREE_REPLY: {
      requestHandler_->notify(&requestReply);
      break;
    }
    case WRITE_REPLY: {
      requestHandler_->notify(&requestReply);
      break;
    }
    case READ_REPLY: {
      requestHandler_->notify(&requestReply);
      break;
    }
    default: {}
  }
  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

NetworkClient::NetworkClient(const string &remote_address,
                             const string &remote_port)
    : NetworkClient(remote_address, remote_port, 1, 32, 65536, 64) {}

NetworkClient::NetworkClient(const string &remote_address,
                             const string &remote_port, int worker_num,
                             int buffer_num_per_con, int buffer_size,
                             int init_buffer_num)
    : remote_address_(remote_address),
      remote_port_(remote_port),
      worker_num_(worker_num),
      buffer_num_per_con_(buffer_num_per_con),
      buffer_size_(buffer_size),
      init_buffer_num_(init_buffer_num),
      connected_(false) {}

NetworkClient::~NetworkClient() {
  delete shutdownCallback;
  delete connectedCallback;
  delete sendCallback;
  delete recvCallback;
}

int NetworkClient::init(RequestHandler *requestHandler) {
  client_ = new Client(worker_num_, buffer_num_per_con_);
  if ((client_->init()) != 0) {
    return -1;
  }
  chunkMgr_ = new ChunkPool(client_, buffer_size_, init_buffer_num_);

  client_->set_chunk_mgr(chunkMgr_);

  shutdownCallback = new ClientShutdownCallback();
  connectedCallback = new ClientConnectedCallback(this);
  recvCallback = new ClientRecvCallback(chunkMgr_, requestHandler);
  sendCallback = new ClientSendCallback(chunkMgr_);

  client_->set_shutdown_callback(shutdownCallback);
  client_->set_connected_callback(connectedCallback);
  client_->set_recv_callback(recvCallback);
  client_->set_send_callback(sendCallback);

  client_->start();
  int res = client_->connect(remote_address_.c_str(), remote_port_.c_str());
  unique_lock<mutex> lk(con_mtx);
  while (!connected_) {
    con_v.wait(lk);
  }

  circularBuffer_ = make_shared<CircularBuffer>(1024 * 1024, 512, false, this);
}

void NetworkClient::shutdown() { client_->shutdown(); }

void NetworkClient::wait() { client_->wait(); }

Chunk *NetworkClient::register_rma_buffer(char *rma_buffer, uint64_t size) {
  return client_->reg_rma_buffer(rma_buffer, size, buffer_id_++);
}

void NetworkClient::unregister_rma_buffer(int buffer_id) {
  client_->unreg_rma_buffer(buffer_id);
}

uint64_t NetworkClient::get_dram_buffer(const char *data, uint64_t size) {
  char *dest = circularBuffer_->get(size);
  if (data) {
    memcpy(dest, data, size);
  }
  return (uint64_t)dest;
}

void NetworkClient::reclaim_dram_buffer(uint64_t src_address, uint64_t size) {
  circularBuffer_->put(reinterpret_cast<char *>(src_address), size);
}

uint64_t NetworkClient::get_rkey() {
  return circularBuffer_->get_rma_chunk()->mr->key;
}

void NetworkClient::connected(Connection *con) {
  std::unique_lock<std::mutex> lk(con_mtx);
  con_ = con;
  connected_ = true;
  con_v.notify_all();
  lk.unlock();
}

void NetworkClient::send(char *data, uint64_t size) {
  auto ck = chunkMgr_->get(con_);
  std::memcpy(reinterpret_cast<char *>(ck->buffer), data, size);
  ck->size = size;
  con_->send(ck);
}

void NetworkClient::read(Request *request) {
  RequestContext rc = request->get_rc();
}
