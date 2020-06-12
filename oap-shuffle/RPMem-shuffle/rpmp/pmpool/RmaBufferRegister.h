/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/RmaBuffer.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Tuesday, December 24th 2019, 2:37:40 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_RMABUFFERREGISTER_H_
#define PMPOOL_RMABUFFERREGISTER_H_

#include <HPNL/ChunkMgr.h>
#include <stdint.h>

class RmaBufferRegister {
 public:
  virtual Chunk* register_rma_buffer(char* rma_buffer, uint64_t size) = 0;
  virtual void unregister_rma_buffer(int buffer_id) = 0;
};

#endif  // PMPOOL_RMABUFFERREGISTER_H_
