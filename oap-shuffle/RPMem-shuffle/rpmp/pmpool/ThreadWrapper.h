/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/ThreadWrapper.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_THREADWRAPPER_H_
#define PMPOOL_THREADWRAPPER_H_

#include <assert.h>

#include <atomic>
#include <condition_variable> // NOLINT
#include <iostream>
#include <mutex>  // NOLINT
#include <thread> // NOLINT

class ThreadWrapper {
 public:
  ThreadWrapper() : done(false) {}
  virtual ~ThreadWrapper() = default;
  void join() {
    if (thread.joinable()) {
      thread.join();
    } else {
      std::unique_lock<std::mutex> l(join_mutex);
      join_event.wait(l, [=] { return done.load(); });
    }
  }
  void start(bool background_thread = false) {
    thread = std::thread(&ThreadWrapper::thread_body, this);
    if (background_thread) {
      thread.detach();
    }
  }
  void stop() { done.store(true); }
  void set_affinity(int cpu) {
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    int res = pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t),
                                     &cpuset);
    if (res) {
      abort();
    }
#endif
  }
  void thread_body() {
    try {
      while (true) {
        int ret = entry();
        if (done.load() || ret == -1) {
          if (!thread.joinable()) {
            join_event.notify_all();
          }
          break;
        }
      }
    } catch (ThreadAbortException &) {
      abort();
    } catch (std::exception &ex) {
      ExceptionCaught(ex);
    } catch (...) {
      UnknownExceptionCaught();
    }
  }

 private:
  class ThreadAbortException : std::exception {};

 protected:
  virtual int entry() = 0;
  virtual void abort() = 0;
  virtual void ExceptionCaught(const std::exception &exception) {}
  virtual void UnknownExceptionCaught() {}

 private:
  std::thread thread;
  std::mutex join_mutex;
  std::condition_variable join_event;
  std::atomic_bool done = {false};
};

#endif  // PMPOOL_THREADWRAPPER_H_
