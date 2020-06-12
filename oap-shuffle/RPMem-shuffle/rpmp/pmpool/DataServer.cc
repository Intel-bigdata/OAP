/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/DataServer.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/DataServer.h"

#include "AllocatorProxy.h"
#include "Config.h"
#include "Digest.h"
#include "NetworkServer.h"
#include "Protocol.h"
#include "Log.h"

DataServer::DataServer(Config *config, Log *log) : config_(config), log_(log) {}

int DataServer::init() {
  networkServer_ = std::make_shared<NetworkServer>(config_, log_);
  CHK_ERR("network server init", networkServer_->init());
  log_->get_file_log()->info("network server initialized.");

  allocatorProxy_ =
      std::make_shared<AllocatorProxy>(config_, log_, networkServer_.get());
  CHK_ERR("allocator proxy init", allocatorProxy_->init());
  log_->get_file_log()->info("allocator proxy initialized.");

  protocol_ = std::make_shared<Protocol>(config_, log_, networkServer_.get(),
                                         allocatorProxy_.get());
  CHK_ERR("protocol init", protocol_->init());
  log_->get_file_log()->info("protocol initialized.");

  networkServer_->start();
  log_->get_file_log()->info("network server started.");
  log_->get_console_log()->info("RPMP started...");
  return 0;
}

void DataServer::wait() { networkServer_->wait(); }
