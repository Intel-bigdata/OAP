/*
 * Filename: /mnt/spark-pmof/tool/rpmp/main.cc
 * Path: /mnt/spark-pmof/tool/rpmp
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include <memory>

#include "pmpool/Config.h"
#include "pmpool/DataServer.h"
#include "pmpool/Base.h"
#include "pmpool/Log.h"

/**
 * @brief program entry of RPMP server
 * @param argc 
 * @param argv 
 * @return int 
 */
int ServerMain(int argc, char **argv) {
  /// initialize Config class
  std::shared_ptr<Config> config = std::make_shared<Config>();
  CHK_ERR("config init", config->init(argc, argv));
  /// initialize Log class
  std::shared_ptr<Log> log = std::make_shared<Log>(config.get());
  /// initialize DataServer class
  std::shared_ptr<DataServer> dataServer =
      std::make_shared<DataServer>(config.get(), log.get());
  log->get_file_log()->info("start to initialize data server.");
  CHK_ERR("data server init", dataServer->init());
  log->get_file_log()->info("data server initailized.");
  dataServer->wait();
  return 0;
}

int main(int argc, char **argv) {
  ServerMain(argc, argv);
  return 0;
}
