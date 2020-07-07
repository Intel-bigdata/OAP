/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Log.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Friday, February 28th 2020, 2:37:41 pm
 * Author: root
 *
 * Copyright (c) 2020 Intel
 */

#ifndef PMPOOL_LOG_H_
#define PMPOOL_LOG_H_

#include <memory>

#include "Config.h"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

class Log {
 public:
  explicit Log(Config *config) : config_(config) {
    file_log_ = spdlog::basic_logger_mt("file_logger", config_->get_log_path());
    if (config_->get_log_level() == "debug") {
      file_log_->set_level(spdlog::level::debug);
      file_log_->flush_on(spdlog::level::debug);
    } else if (config_->get_log_level() == "info") {
      file_log_->set_level(spdlog::level::info);
      file_log_->flush_on(spdlog::level::info);
    } else if (config_->get_log_level() == "warn") {
      file_log_->set_level(spdlog::level::warn);
      file_log_->flush_on(spdlog::level::warn);
    } else if (config_->get_log_level() == "error") {
      file_log_->set_level(spdlog::level::err);
      file_log_->flush_on(spdlog::level::err);
    } else {
    }
    console_log_ = spdlog::stdout_color_mt("console");
    console_log_->flush_on(spdlog::level::info);
  }

  std::shared_ptr<spdlog::logger> get_file_log() { return file_log_; }
  std::shared_ptr<spdlog::logger> get_console_log() { return console_log_; }

 private:
  Config *config_;
  std::shared_ptr<spdlog::logger> file_log_;
  std::shared_ptr<spdlog::logger> console_log_;
};

#endif  //  PMPOOL_LOG_H_
