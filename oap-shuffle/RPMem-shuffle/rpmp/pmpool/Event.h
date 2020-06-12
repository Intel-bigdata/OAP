/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Request.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Friday, December 13th 2019, 3:43:30 pm
 * Author: root
 *
 * Copyright (c) Intel
 */

#ifndef PMPOOL_EVENT_H_
#define PMPOOL_EVENT_H_

#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include <future>  // NOLINT
#include <vector>

#include "pmpool/Base.h"
#include "pmpool/PmemAllocator.h"

using std::future;
using std::promise;
using std::vector;

class RequestHandler;
class ClientRecvCallback;
class Protocol;

enum OpType : uint32_t {
  ALLOC = 1,
  FREE,
  PREPARE,
  WRITE,
  READ,
  PUT,
  GET,
  GET_META,
  DELETE,
  REPLY = 1 << 16,
  ALLOC_REPLY,
  FREE_REPLY,
  PREPARE_REPLY,
  WRITE_REPLY,
  READ_REPLY,
  PUT_REPLY,
  GET_REPLY,
  GET_META_REPLY,
  DELETE_REPLY
};

/**
 * @brief Define two types of event in this file: Request, RequestReply
 * Request: a event that client creates and sends to server.
 * RequestReply: a event that server creates and sends to client.
 * RequestContext and RequestReplyContext include the context information of the
 * previous two events.
 */
struct RequestReplyContext {
  OpType type;
  uint32_t success;
  uint64_t rid;
  uint64_t address;
  uint64_t src_address;
  uint64_t dest_address;
  uint64_t src_rkey;
  uint64_t size;
  uint64_t key;
  Connection* con;
  Chunk* ck;
  vector <block_meta> bml;
};

template <class T>
inline void encode_(T* t, char* data, uint64_t* size) {
  assert(t != nullptr);
  memcpy(data, t, sizeof(t));
  *size = sizeof(t);
}

template <class T>
inline void decode_(T* t, char* data, uint64_t size) {
  assert(t != nullptr);
  assert(size == sizeof(t));
  memcpy(t, data, size);
}

class RequestReply {
 public:
  RequestReply() = delete;
  explicit RequestReply(RequestReplyContext requestReplyContext);
  RequestReply(char* data, uint64_t size, Connection* con);
  ~RequestReply();
  RequestReplyContext& get_rrc();
  void decode();
  void encode();

 private:
  friend Protocol;
  char* data_;
  uint64_t size_;
  RequestReplyMsg requestReplyMsg_;
  RequestReplyContext requestReplyContext_;
};

typedef promise<RequestReplyContext> Promise;
typedef future<RequestReplyContext> Future;

struct RequestContext {
  OpType type;
  uint64_t rid;
  uint64_t address;
  uint64_t src_address;
  uint64_t src_rkey;
  uint64_t size;
  uint64_t key;
  Connection* con;
};

class Request {
 public:
  Request() = delete;
  explicit Request(RequestContext requestContext);
  Request(char* data, uint64_t size, Connection* con);
  ~Request();
  RequestContext& get_rc();
  void encode();
  void decode();

 private:
  friend RequestHandler;
  friend ClientRecvCallback;
  char* data_;
  uint64_t size_;
  RequestMsg requestMsg_;
  RequestContext requestContext_;
};

#endif  // PMPOOL_EVENT_H_
