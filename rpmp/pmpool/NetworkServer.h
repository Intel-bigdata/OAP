/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/NetworkServer.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Tuesday, December 10th 2019, 3:14:59 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_NETWORKSERVER_H_
#define PMPOOL_NETWORKSERVER_H_

#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>
#include <HPNL/Server.h>

#include <atomic>
#include <memory>

#include "RmaBufferRegister.h"

class CircularBuffer;
class Config;
class RequestReply;
class RequestReplyContext;
class Log;

/**
 * @brief RPMP network service is based on HPNL, which is a completely
 * asynchronous network library. RPMP currently supports RDMA iWarp and RoCE V2
 * protocol.
 */
class NetworkServer : public RmaBufferRegister {
 public:
  NetworkServer() = delete;
  NetworkServer(Config *config, Log *log_);
  ~NetworkServer();
  int init();
  int start();
  void wait();
  /// register DRAM or Persistent Memory as RDMA region.
  /// Return chunk that is the wrapper of RDMA region if succeed,
  /// return nullptr if fail.
  Chunk *register_rma_buffer(char *rma_buffer, uint64_t size) override;

  /// unregister RDMA region for given buffer.
  void unregister_rma_buffer(int buffer_id) override;

  /// get DRAM buffer from circular buffer pool.
  void get_dram_buffer(RequestReplyContext *rrc);

  /// reclaim DRAM buffer from circular buffer pool.
  void reclaim_dram_buffer(RequestReplyContext *rrc);

  /// get Persistent Memory buffer from circular buffer pool
  void get_pmem_buffer(RequestReplyContext *rrc, Chunk *ck);

  /// reclaim Persistent Memory buffer form circular buffer pool
  void reclaim_pmem_buffer(RequestReplyContext *rrc);

  /// return the pointer of chunk manager.
  ChunkMgr *get_chunk_mgr();

  /// since the network implementation is asynchronous,
  /// we need to define callback better before starting network service.
  void set_recv_callback(Callback *callback);
  void set_send_callback(Callback *callback);
  void set_read_callback(Callback *callback);
  void set_write_callback(Callback *callback);

  void send(char *data, uint64_t size, Connection *con);
  void read(RequestReply *rrc);
  void write(RequestReply *rrc);

 private:
  Config *config_;
  Log* log_;
  std::shared_ptr<Server> server_;
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::shared_ptr<CircularBuffer> circularBuffer_;
  std::atomic<uint64_t> buffer_id_{0};
  uint64_t time;
};

#endif  // PMPOOL_NETWORKSERVER_H_
