/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Request.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, December 12th 2019, 1:36:18 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/Event.h"

#include "pmpool/buffer/CircularBuffer.h"

Request::Request(RequestContext requestContext)
    : data_(nullptr), size_(0), requestContext_(requestContext) {}

Request::Request(char *data, uint64_t size, Connection *con) : size_(size) {
  data_ = static_cast<char *>(std::malloc(size));
  memcpy(data_, data, size_);
  requestContext_.con = con;
}

Request::~Request() {
  if (data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

RequestContext &Request::get_rc() { return requestContext_; }

void Request::encode() {
  OpType rt = requestContext_.type;
  assert(rt == ALLOC || rt == FREE || rt == WRITE || rt == READ);
  requestMsg_.type = requestContext_.type;
  requestMsg_.rid = requestContext_.rid;
  requestMsg_.address = requestContext_.address;
  requestMsg_.src_address = requestContext_.src_address;
  requestMsg_.src_rkey = requestContext_.src_rkey;
  requestMsg_.size = requestContext_.size;
  requestMsg_.key = requestContext_.key;

  size_ = sizeof(requestMsg_);
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, &requestMsg_, size_);
}

void Request::decode() {
  assert(size_ == sizeof(requestMsg_));
  memcpy(&requestMsg_, data_, size_);
  requestContext_.type = (OpType)requestMsg_.type;
  requestContext_.rid = requestMsg_.rid;
  requestContext_.address = requestMsg_.address;
  requestContext_.src_address = requestMsg_.src_address;
  requestContext_.src_rkey = requestMsg_.src_rkey;
  requestContext_.size = requestMsg_.size;
  requestContext_.key = requestMsg_.key;
}

RequestReply::RequestReply(RequestReplyContext requestReplyContext)
    : data_(nullptr), size_(0), requestReplyContext_(requestReplyContext) {}

RequestReply::RequestReply(char *data, uint64_t size, Connection *con)
    : size_(size) {
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, data, size_);
  requestReplyContext_.con = con;
}

RequestReply::~RequestReply() {
  if (data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

RequestReplyContext &RequestReply::get_rrc() { return requestReplyContext_; }

void RequestReply::encode() {
  requestReplyMsg_.type = (OpType)requestReplyContext_.type;
  requestReplyMsg_.success = requestReplyContext_.success;
  requestReplyMsg_.rid = requestReplyContext_.rid;
  requestReplyMsg_.address = requestReplyContext_.address;
  requestReplyMsg_.size = requestReplyContext_.size;
  requestReplyMsg_.key = requestReplyContext_.key;
  auto msg_size = sizeof(requestReplyMsg_);
  size_ = msg_size;

  /// copy data from block metadata list
  uint32_t bml_size = 0;
  if (!requestReplyContext_.bml.empty()) {
    bml_size = sizeof(block_meta) * requestReplyContext_.bml.size();
    size_ += bml_size;
  }
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, &requestReplyMsg_, msg_size);
  if (bml_size != 0) {
    memcpy(data_ + msg_size, &requestReplyContext_.bml[0], bml_size);
  }
}

void RequestReply::decode() {
  memcpy(&requestReplyMsg_, data_, size_);
  requestReplyContext_.type = (OpType)requestReplyMsg_.type;
  requestReplyContext_.success = requestReplyMsg_.success;
  requestReplyContext_.rid = requestReplyMsg_.rid;
  requestReplyContext_.address = requestReplyMsg_.address;
  requestReplyContext_.size = requestReplyMsg_.size;
  requestReplyContext_.key = requestReplyMsg_.key;
  if (size_ > sizeof(requestReplyMsg_)) {
    auto bml_size = size_ - sizeof(requestReplyMsg_);
    requestReplyContext_.bml.resize(bml_size / sizeof(block_meta));
    memcpy(&requestReplyContext_.bml[0], data_ + sizeof(requestReplyMsg_),
           bml_size);
  }
}
