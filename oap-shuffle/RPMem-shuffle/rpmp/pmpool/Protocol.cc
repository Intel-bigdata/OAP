/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Protocol.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/Protocol.h"

#include <assert.h>

#include "AllocatorProxy.h"
#include "Config.h"
#include "Digest.h"
#include "Event.h"
#include "Log.h"
#include "NetworkServer.h"

RecvCallback::RecvCallback(Protocol *protocol, ChunkMgr *chunkMgr)
    : protocol_(protocol), chunkMgr_(chunkMgr) {}

void RecvCallback::operator()(void *buffer_id, void *buffer_size) {
  auto buffer_id_ = *static_cast<int *>(buffer_id);
  Chunk *ck = chunkMgr_->get(buffer_id_);
  assert(*static_cast<uint64_t *>(buffer_size) == ck->size);
  Request *request = new Request(reinterpret_cast<char *>(ck->buffer), ck->size,
                                 reinterpret_cast<Connection *>(ck->con));
  request->decode();
  protocol_->enqueue_recv_msg(request);
  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

ReadCallback::ReadCallback(Protocol *protocol) : protocol_(protocol) {}

void ReadCallback::operator()(void *buffer_id, void *buffer_size) {
  auto buffer_id_ = *static_cast<int *>(buffer_id);
  protocol_->enqueue_rma_msg(buffer_id_);
}

SendCallback::SendCallback(ChunkMgr *chunkMgr) : chunkMgr_(chunkMgr) {}

void SendCallback::operator()(void *buffer_id, void *buffer_size) {
  auto buffer_id_ = *static_cast<int *>(buffer_id);
  auto ck = chunkMgr_->get(buffer_id_);

  /// free the memory of class RequestReply
  auto reqeustReply = static_cast<RequestReply *>(ck->ptr);
  delete reqeustReply;

  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

WriteCallback::WriteCallback(Protocol *protocol) : protocol_(protocol) {}

void WriteCallback::operator()(void *buffer_id, void *buffer_size) {
  auto buffer_id_ = *static_cast<int *>(buffer_id);
  protocol_->enqueue_rma_msg(buffer_id_);
}

RecvWorker::RecvWorker(Protocol *protocol, int index)
    : protocol_(protocol), index_(index) {
  init = false;
}

int RecvWorker::entry() {
  if (!init) {
    set_affinity(index_);
    init = true;
  }
  Request *request;
  bool res = pendingRecvRequestQueue_.wait_dequeue_timed(
      request, std::chrono::milliseconds(1000));
  if (res) {
    protocol_->handle_recv_msg(request);
  }
  return 0;
}

void RecvWorker::abort() {}

void RecvWorker::addTask(Request *request) {
  pendingRecvRequestQueue_.enqueue(request);
}

ReadWorker::ReadWorker(Protocol *protocol, int index)
    : protocol_(protocol), index_(index) {
  init = false;
}

int ReadWorker::entry() {
  if (!init) {
    set_affinity(index_);
    init = true;
  }
  RequestReply *requestReply;
  bool res = pendingReadRequestQueue_.wait_dequeue_timed(
      requestReply, std::chrono::milliseconds(1000));
  if (res) {
    protocol_->handle_rma_msg(requestReply);
  }
  return 0;
}

void ReadWorker::abort() {}

void ReadWorker::addTask(RequestReply *rr) {
  pendingReadRequestQueue_.enqueue(rr);
}

FinalizeWorker::FinalizeWorker(Protocol *protocol) : protocol_(protocol) {}

int FinalizeWorker::entry() {
  RequestReply *requestReply;
  bool res = pendingRequestReplyQueue_.wait_dequeue_timed(
      requestReply, std::chrono::milliseconds(1000));
  if (res) {
    protocol_->handle_finalize_msg(requestReply);
  }
  return 0;
}

void FinalizeWorker::abort() {}

void FinalizeWorker::addTask(RequestReply *requestReply) {
  pendingRequestReplyQueue_.enqueue(requestReply);
}

Protocol::Protocol(Config *config, Log *log, NetworkServer *server,
                   AllocatorProxy *allocatorProxy)
    : config_(config),
      log_(log),
      networkServer_(server),
      allocatorProxy_(allocatorProxy) {
  time = 0;
}

Protocol::~Protocol() {
  for (auto worker : recvWorkers_) {
    worker->stop();
    worker->join();
  }
  for (auto worker : readWorkers_) {
    worker->stop();
    worker->join();
  }
  finalizeWorker_->stop();
  finalizeWorker_->join();
}

int Protocol::init() {
  recvCallback_ =
      std::make_shared<RecvCallback>(this, networkServer_->get_chunk_mgr());
  sendCallback_ =
      std::make_shared<SendCallback>(networkServer_->get_chunk_mgr());
  readCallback_ = std::make_shared<ReadCallback>(this);
  writeCallback_ = std::make_shared<WriteCallback>(this);

  for (int i = 0; i < config_->get_pool_size(); i++) {
    auto recvWorker = new RecvWorker(this, config_->get_affinities_()[i] - 1);
    recvWorker->start();
    recvWorkers_.push_back(std::shared_ptr<RecvWorker>(recvWorker));
  }

  finalizeWorker_ = make_shared<FinalizeWorker>(this);
  finalizeWorker_->start();

  for (int i = 0; i < config_->get_pool_size(); i++) {
    auto readWorker = new ReadWorker(this, config_->get_affinities_()[i]);
    readWorker->start();
    readWorkers_.push_back(std::shared_ptr<ReadWorker>(readWorker));
  }

  networkServer_->set_recv_callback(recvCallback_.get());
  networkServer_->set_send_callback(sendCallback_.get());
  networkServer_->set_read_callback(readCallback_.get());
  networkServer_->set_write_callback(writeCallback_.get());
  return 0;
}

void Protocol::enqueue_recv_msg(Request *request) {
  RequestContext rc = request->get_rc();
  if (rc.address != 0) {
    auto wid = GET_WID(rc.address);
    recvWorkers_[wid]->addTask(request);
  } else {
    recvWorkers_[rc.rid % config_->get_pool_size()]->addTask(request);
  }
}

void Protocol::handle_recv_msg(Request *request) {
  RequestContext rc = request->get_rc();
  RequestReplyContext rrc;
  switch (rc.type) {
    case ALLOC: {
      uint64_t addr = allocatorProxy_->allocate_and_write(
          rc.size, nullptr, rc.rid % config_->get_pool_size());
      auto wid = GET_WID(addr);
      assert(wid == rc.rid % config_->get_pool_size());
      rrc.type = ALLOC_REPLY;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.address = addr;
      rrc.size = rc.size;
      rrc.con = rc.con;
      RequestReply *requestReply = new RequestReply(rrc);
      rrc.ck->ptr = requestReply;
      enqueue_finalize_msg(requestReply);
      break;
    }
    case FREE: {
      rrc.type = FREE_REPLY;
      rrc.success = allocatorProxy_->release(rc.address);
      rrc.rid = rc.rid;
      rrc.address = rc.address;
      rrc.size = rc.size;
      rrc.con = rc.con;
      RequestReply *requestReply = new RequestReply(rrc);
      rrc.ck->ptr = requestReply;
      enqueue_finalize_msg(requestReply);
      break;
    }
    case WRITE: {
      rrc.type = WRITE_REPLY;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.address = rc.address;
      rrc.src_address = rc.src_address;
      rrc.src_rkey = rc.src_rkey;
      rrc.size = rc.size;
      rrc.con = rc.con;
      networkServer_->get_dram_buffer(&rrc);
      RequestReply *requestReply = new RequestReply(rrc);
      rrc.ck->ptr = requestReply;

      std::unique_lock<std::mutex> lk(rrcMtx_);
      rrcMap_[rrc.ck->buffer_id] = requestReply;
      lk.unlock();
      networkServer_->read(requestReply);
      break;
    }
    case READ: {
      rrc.type = READ_REPLY;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.address = rc.address;
      rrc.src_address = rc.src_address;
      rrc.src_rkey = rc.src_rkey;
      rrc.size = rc.size;
      rrc.con = rc.con;
      rrc.dest_address = allocatorProxy_->get_virtual_address(rrc.address);
      rrc.ck = nullptr;
      Chunk *base_ck = allocatorProxy_->get_rma_chunk(rrc.address);
      networkServer_->get_pmem_buffer(&rrc, base_ck);
      RequestReply *requestReply = new RequestReply(rrc);
      rrc.ck->ptr = requestReply;

      std::unique_lock<std::mutex> lk(rrcMtx_);
      rrcMap_[rrc.ck->buffer_id] = requestReply;
      lk.unlock();
      networkServer_->write(requestReply);
      break;
    }
    case PUT: {
      rrc.type = PUT_REPLY;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.address = rc.address;
      rrc.src_address = rc.src_address;
      rrc.src_rkey = rc.src_rkey;
      rrc.size = rc.size;
      rrc.key = rc.key;
      rrc.con = rc.con;
      networkServer_->get_dram_buffer(&rrc);
      RequestReply *requestReply = new RequestReply(rrc);
      rrc.ck->ptr = requestReply;

      std::unique_lock<std::mutex> lk(rrcMtx_);
      rrcMap_[rrc.ck->buffer_id] = requestReply;
      lk.unlock();
      networkServer_->read(requestReply);
      break;
    }
    case GET_META: {
      rrc.type = GET_META_REPLY;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.size = rc.size;
      rrc.key = rc.key;
      rrc.con = rc.con;
      RequestReply *requestReply = new RequestReply(rrc);
      rrc.ck->ptr = requestReply;
      enqueue_finalize_msg(requestReply);
    }
    case DELETE: {
      rrc.type = DELETE_REPLY;
      rrc.key = rc.key;
      rrc.con = rc.con;
      rrc.rid = rc.rid;
      rrc.success = 0;
    }
    default: { break; }
  }

  delete request;
}

void Protocol::enqueue_finalize_msg(RequestReply *requestReply) {
  finalizeWorker_->addTask(requestReply);
}

void Protocol::handle_finalize_msg(RequestReply *requestReply) {
  RequestReplyContext rrc = requestReply->get_rrc();
  if (rrc.type == PUT_REPLY) {
    allocatorProxy_->cache_chunk(rrc.key, rrc.address, rrc.size);
  } else if (rrc.type == GET_META_REPLY) {
    auto bml = allocatorProxy_->get_cached_chunk(rrc.key);
    requestReply->requestReplyContext_.bml = bml;
  } else if (rrc.type == DELETE_REPLY) {
    auto bml = allocatorProxy_->get_cached_chunk(rrc.key);
    for (auto bm : bml) {
      rrc.success = allocatorProxy_->release(bm.address);
      if (rrc.success) {
        break;
      }
    }
    allocatorProxy_->del_chunk(rrc.key);
  } else {
  }
  requestReply->encode();
  networkServer_->send(reinterpret_cast<char *>(requestReply->data_),
                       requestReply->size_, rrc.con);
}

void Protocol::enqueue_rma_msg(uint64_t buffer_id) {
  std::unique_lock<std::mutex> lk(rrcMtx_);
  RequestReply *requestReply = rrcMap_[buffer_id];
  lk.unlock();
  RequestReplyContext rrc = requestReply->get_rrc();
  if (rrc.address != 0) {
    auto wid = GET_WID(rrc.address);
    readWorkers_[wid]->addTask(requestReply);
  } else {
    readWorkers_[rrc.rid % config_->get_pool_size()]->addTask(requestReply);
  }
}

void Protocol::handle_rma_msg(RequestReply *requestReply) {
  RequestReplyContext &rrc = requestReply->get_rrc();
  switch (rrc.type) {
    case WRITE_REPLY: {
      char *buffer = static_cast<char *>(rrc.ck->buffer);
      if (rrc.address == 0) {
        rrc.address = allocatorProxy_->allocate_and_write(
            rrc.size, buffer, rrc.rid % config_->get_pool_size());
      } else {
        allocatorProxy_->write(rrc.address, buffer, rrc.size);
      }
      networkServer_->reclaim_dram_buffer(&rrc);
      break;
    }
    case READ_REPLY: {
      networkServer_->reclaim_pmem_buffer(&rrc);
      break;
    }
    case PUT_REPLY: {
      char *buffer = static_cast<char *>(rrc.ck->buffer);
      assert(rrc.address == 0);
      rrc.address = allocatorProxy_->allocate_and_write(
          rrc.size, buffer, rrc.rid % config_->get_pool_size());
      networkServer_->reclaim_dram_buffer(&rrc);
      break;
    }
    default: { break; }
  }
  enqueue_finalize_msg(requestReply);
}
