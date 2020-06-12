/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/NetworkServer.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Tuesday, December 24th 2019, 7:29:48 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/NetworkServer.h"

#include "Base.h"
#include "Config.h"
#include "Event.h"
#include "Log.h"
#include "buffer/CircularBuffer.h"

NetworkServer::NetworkServer(Config *config, Log *log)
    : config_(config), log_(log) {
  time = 0;
}

NetworkServer::~NetworkServer() {
  for (int i = 0; i < buffer_id_; i++) {
    unregister_rma_buffer(i);
  }
}

int NetworkServer::init() {
  server_ = std::make_shared<Server>(config_->get_network_worker_num(),
                                     config_->get_network_buffer_num());
  CHK_ERR("hpnl server init", server_->init());

  chunkMgr_ = std::make_shared<ChunkPool>(server_.get(),
                                          config_->get_network_buffer_size(),
                                          config_->get_network_buffer_num());

  server_->set_chunk_mgr(chunkMgr_.get());
  return 0;
}

int NetworkServer::start() {
  server_->start();
  CHK_ERR("hpnl server listen", server_->listen(config_->get_ip().c_str(),
                                                config_->get_port().c_str()));

  circularBuffer_ =
      std::make_shared<CircularBuffer>(1024 * 1024, 4096, true, this);
  return 0;
}

void NetworkServer::wait() { server_->wait(); }

Chunk *NetworkServer::register_rma_buffer(char *rma_buffer, uint64_t size) {
  return server_->reg_rma_buffer(rma_buffer, size, buffer_id_++);
}

void NetworkServer::unregister_rma_buffer(int buffer_id) {
  server_->unreg_rma_buffer(buffer_id);
}

void NetworkServer::get_dram_buffer(RequestReplyContext *rrc) {
  char *buffer = circularBuffer_->get(rrc->size);
  rrc->dest_address = (uint64_t)buffer;

  Chunk *base_ck = circularBuffer_->get_rma_chunk();
  uint64_t offset = circularBuffer_->get_offset(rrc->dest_address);

  // encapsulate new chunk
  Chunk *ck = new Chunk();
  ck->buffer = static_cast<char *>(base_ck->buffer) + offset;
  ck->capacity = base_ck->capacity;
  ck->buffer_id = buffer_id_++;
  ck->mr = base_ck->mr;
  ck->size = rrc->size;
  rrc->ck = ck;
}

void NetworkServer::reclaim_dram_buffer(RequestReplyContext *rrc) {
  char *buffer_tmp = reinterpret_cast<char *>(rrc->dest_address);
  circularBuffer_->put(buffer_tmp, rrc->size);
  delete rrc->ck;
}

void NetworkServer::get_pmem_buffer(RequestReplyContext *rrc, Chunk *base_ck) {
  Chunk *ck = new Chunk();
  ck->buffer = reinterpret_cast<char *>(rrc->dest_address);
  ck->capacity = rrc->size;
  ck->buffer_id = buffer_id_++;
  ck->mr = base_ck->mr;
  ck->size = rrc->size;
  rrc->ck = ck;
}

void NetworkServer::reclaim_pmem_buffer(RequestReplyContext *rrc) {
  if (rrc->ck != nullptr) {
    delete rrc->ck;
  }
}

ChunkMgr *NetworkServer::get_chunk_mgr() { return chunkMgr_.get(); }

void NetworkServer::set_recv_callback(Callback *callback) {
  server_->set_recv_callback(callback);
}

void NetworkServer::set_send_callback(Callback *callback) {
  server_->set_send_callback(callback);
}

void NetworkServer::set_read_callback(Callback *callback) {
  server_->set_read_callback(callback);
}

void NetworkServer::set_write_callback(Callback *callback) {
  server_->set_write_callback(callback);
}

void NetworkServer::send(char *data, uint64_t size, Connection *con) {
  auto ck = chunkMgr_->get(con);
  std::memcpy(reinterpret_cast<char *>(ck->buffer), data, size);
  ck->size = size;
  con->send(ck);
}

void NetworkServer::read(RequestReply *rr) {
  RequestReplyContext rrc = rr->get_rrc();
  rrc.con->read(rrc.ck, 0, rrc.size, rrc.src_address, rrc.src_rkey);
}

void NetworkServer::write(RequestReply *rr) {
  RequestReplyContext rrc = rr->get_rrc();
  rrc.con->write(rrc.ck, 0, rrc.size, rrc.src_address, rrc.src_rkey);
}
