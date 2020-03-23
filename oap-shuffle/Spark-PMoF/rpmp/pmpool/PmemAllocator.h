/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/PmemObjAllocator.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Monday, December 9th 2019, 10:52:02 am
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_PMEMALLOCATOR_H_
#define PMPOOL_PMEMALLOCATOR_H_

#include <libpmemobj.h>

#include <atomic>
#include <chrono>  // NOLINT
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "Allocator.h"
#include "DataServer.h"
#include "Log.h"
#include "NetworkServer.h"

using std::shared_ptr;
using std::unordered_map;

#define PMEMOBJ_ALLOCATOR_LAYOUT_NAME "pmemobj_allocator_layout"

// block header stored in pmem
struct block_hdr {
  PMEMoid next;
  PMEMoid pre;
  uint64_t addr;
  uint64_t size;
};

// block data entry stored in pmem
struct block_entry {
  struct block_hdr hdr;
  PMEMoid data;
};

// pmem root entry
struct Base {
  PMEMoid head;
  PMEMoid tail;
  PMEMrwlock rwlock;
  uint64_t bytes_written;
};

struct PmemContext {
  PMEMobjpool *pop;
  PMEMoid poid;
  Base *base;
};

// pmem data allocation types
enum types { BLOCK_ENTRY_TYPE, DATA_TYPE, MAX_TYPE };

/**
 * @brief libpmemobj based implementation of Allocator interface.
 *
 */
class PmemObjAllocator : public Allocator {
 public:
  PmemObjAllocator() = delete;
  explicit PmemObjAllocator(Log *log, DiskInfo *diskInfos,
                            NetworkServer *server, int wid)
      : log_(log), diskInfo_(diskInfos), server_(server), wid_(wid) {}
  ~PmemObjAllocator() { close(); }

  int init() override {
    memset(str, '0', 1048576);
    if (create()) {
      int res = open();
      if (res) {
        string err_msg = pmemobj_errormsg();
        log_->get_file_log()->error("failed to open pmem pool, errmsg: " +
                                    err_msg);
      }
    }
    return 0;
  }

  uint64_t allocate_and_write(uint64_t size,
                              const char *content = nullptr) override {
    jmp_buf env;
    if (setjmp(env)) {
      // end the transaction
      (void)pmemobj_tx_end();
      return -1;
    }

    // begin a transaction, also acquiring the write lock for the data
    if (pmemobj_tx_begin(pmemContext_.pop, env, TX_PARAM_RWLOCK,
                         &pmemContext_.base->rwlock, TX_PARAM_NONE)) {
      perror("pmemobj_tx_begin failed in pmemkv put");
      return -1;
    }

    // allocate the new node to be inserted
    PMEMoid beo =
        pmemobj_tx_alloc(sizeof(struct block_entry), BLOCK_ENTRY_TYPE);
    if (beo.off == 0 && beo.pool_uuid_lo == 0) {
      (void)pmemobj_tx_end();
      perror("pmemobj_tx_alloc failed in pmemkv put");
      return -1;
    }
    struct block_entry *bep = (struct block_entry *)pmemobj_direct(beo);
    bep->data = pmemobj_tx_zalloc(size, DATA_TYPE);
    bep->hdr.next = OID_NULL;
    bep->hdr.addr = TO_GLOB((uint64_t)pmemobj_direct(bep->data),
                            (uint64_t)pmemContext_.pop, wid_);
    bep->hdr.size = size;

    uint64_t start =
        std::chrono::high_resolution_clock::now().time_since_epoch() /
        std::chrono::milliseconds(1);
    char *pmem_data = static_cast<char *>(pmemobj_direct(bep->data));
    if (content != nullptr) {
      memcpy(pmem_data, content, size);
    }
    uint64_t end =
        std::chrono::high_resolution_clock::now().time_since_epoch() /
        std::chrono::milliseconds(1);
    total += (end - start);
    // std::cout << "index " << wid_ << ", total is " << total / 1000.0
    //           << std::endl;

    // add the modified root object to the undo data
    pmemobj_tx_add_range(pmemContext_.poid, 0, sizeof(struct Base));
    if (pmemContext_.base->tail.off == 0) {
      // update head
      pmemContext_.base->head = beo;
      bep->hdr.pre = OID_NULL;
    } else {
      // add the modified tail entry to the undo data
      bep->hdr.pre = pmemContext_.base->tail;
      pmemobj_tx_add_range(pmemContext_.base->tail, 0,
                           sizeof(struct block_entry));
      ((struct block_entry *)pmemobj_direct(pmemContext_.base->tail))
          ->hdr.next = beo;
    }

    pmemContext_.base->tail = beo;  // update tail
    pmemContext_.base->bytes_written += size;
    pmemobj_tx_commit();
    (void)pmemobj_tx_end();

    // update in-memory index
    if (update_meta(beo)) {
      return -1;
    }

    return bep->hdr.addr;
  }

  int write(uint64_t address, const char *content, uint64_t size) override {
    std::unique_lock<std::mutex> l(mtx);
    if (!index_map.count(address)) {
      return -1;
    }
    PMEMoid data = index_map[address];
    struct block_entry *bep = (struct block_entry *)pmemobj_direct(data);
    char *pmem_data = static_cast<char *>(pmemobj_direct(bep->data));
    // pmemobj_memcpy_persist(pmemContext_.pop, pmem_data, content, size);
    uint64_t start =
        std::chrono::high_resolution_clock::now().time_since_epoch() /
        std::chrono::milliseconds(1);
    memcpy(pmem_data, content, size);
    uint64_t end =
        std::chrono::high_resolution_clock::now().time_since_epoch() /
        std::chrono::milliseconds(1);
    total += (end - start);
    l.unlock();
    return 0;
  }

  uint64_t get_virtual_address(uint64_t address) {
    std::unique_lock<std::mutex> l(mtx);
    if (!index_map.count(address)) {
      return -1;
    }
    PMEMoid data = index_map[address];
    struct block_entry *bep = (struct block_entry *)pmemobj_direct(data);
    char *pmem_data = static_cast<char *>(pmemobj_direct(bep->data));
    l.unlock();
    return (uint64_t)pmem_data;
  }

  int release(uint64_t address) override {
    jmp_buf env;
    if (setjmp(env)) {
      // end the transaction
      (void)pmemobj_tx_end();
      return -1;
    }

    // begin a transaction, also acquiring the write lock for the data
    if (pmemobj_tx_begin(pmemContext_.pop, env, TX_PARAM_RWLOCK,
                         &pmemContext_.base->rwlock, TX_PARAM_NONE)) {
      perror("pmemobj_tx_begin failed in pmemkv put");
      return -1;
    }
    if (!index_map.count(address)) {
      (void)pmemobj_tx_end();
      perror("address not found");
      return -1;
    }
    PMEMoid data = index_map[address];
    struct block_entry *bep = (struct block_entry *)pmemobj_direct(data);
    struct block_entry *prev_bep =
        (struct block_entry *)pmemobj_direct(bep->hdr.pre);
    struct block_entry *next_bep =
        (struct block_entry *)pmemobj_direct(bep->hdr.next);
    pmemobj_tx_add_range(pmemContext_.poid, 0, sizeof(struct Base));
    if (prev_bep == nullptr) {
      if (next_bep == nullptr) {
        pmemContext_.base->head = OID_NULL;
        pmemContext_.base->tail = OID_NULL;
      } else {
        pmemContext_.base->head = bep->hdr.next;
        next_bep->hdr.pre = OID_NULL;
      }
    } else {
      pmemobj_tx_add_range(bep->hdr.pre, 0, sizeof(struct Base));
      prev_bep->hdr.next = bep->hdr.next;
    }
    pmemContext_.base->bytes_written -= bep->hdr.size;
    pmemobj_tx_add_range(data, 0, sizeof(struct Base));
    bep->hdr.pre = OID_NULL;
    bep->hdr.next = OID_NULL;
    pmemobj_free(&data);
    pmemobj_free(&bep->data);

    pmemobj_tx_commit();
    (void)pmemobj_tx_end();

    return 0;
  }

  int release_all() override {
    PMEMoid cur_oid = pmemContext_.base->head;
    while (cur_oid.off != 0 && cur_oid.pool_uuid_lo != 0) {
      struct block_entry *cur_bep =
          (struct block_entry *)pmemobj_direct(cur_oid);
      PMEMoid next_oid = cur_bep->hdr.next;
      struct block_entry *next_bep =
          (struct block_entry *)pmemobj_direct(next_oid);
      pmemobj_free(&cur_oid);
      pmemobj_free(&cur_bep->data);
      cur_oid = next_oid;
    }
    pmemContext_.base->head = OID_NULL;
    pmemContext_.base->tail = OID_NULL;
    pmemContext_.base->bytes_written = 0;

    return 0;
  }

  int dump_all() override {
    std::cout << "******************worker " << wid_
              << " start dump*********************" << std::endl;
    if (pmemobj_rwlock_rdlock(pmemContext_.pop, &pmemContext_.base->rwlock) !=
        0) {
      return -1;
    }
    struct block_entry *next_bep =
        (struct block_entry *)pmemobj_direct(pmemContext_.base->head);
    uint64_t read_offset = 0;
    while (next_bep != nullptr) {
      char *pmem_data =
          reinterpret_cast<char *>(pmemobj_direct(next_bep->data));
      char *tmp = reinterpret_cast<char *>(std::malloc(next_bep->hdr.size));
      memcpy(tmp, pmem_data, next_bep->hdr.size);
      std::cout << "dump address " << next_bep->hdr.addr << std::endl;
      read_offset += next_bep->hdr.size;
      std::free(tmp);
      next_bep = (struct block_entry *)pmemobj_direct(next_bep->hdr.next);
    }
    pmemobj_rwlock_unlock(pmemContext_.pop, &pmemContext_.base->rwlock);
    std::cout << "total size " << pmemContext_.base->bytes_written << std::endl;
    std::cout << "******************worker " << wid_
              << " end dump*********************" << std::endl;
    return 0;
  }

  Chunk *get_rma_chunk() { return base_ck; }

 private:
  int create() {
    // debug setting
    int sds_write_value = 0;
    pmemobj_ctl_set(nullptr, "sds.at_create", &sds_write_value);

    pmemContext_.pop = pmemobj_create(diskInfo_->path.c_str(),
                                      PMEMOBJ_ALLOCATOR_LAYOUT_NAME, 0, 0666);
    if (pmemContext_.pop == nullptr) {
      string err_msg = pmemobj_errormsg();
      log_->get_file_log()->warn("failed to create pmem pool, errmsg: " +
                                 err_msg);
      return -1;
    }
    pmemContext_.poid = pmemobj_root(pmemContext_.pop, sizeof(struct Base));
    pmemContext_.base = (struct Base *)pmemobj_direct(pmemContext_.poid);
    pmemContext_.base->head = OID_NULL;
    pmemContext_.base->tail = OID_NULL;
    pmemContext_.base->bytes_written = 0;

    if (server_) {
      base_ck = server_->register_rma_buffer(
          reinterpret_cast<char *>(pmemContext_.pop), diskInfo_->size);
      assert(base_ck != nullptr);
      log_->get_console_log()->info(
          "successfully registered Persistent Memory(" + diskInfo_->path +
          ") as RDMA region");
    }
    return 0;
  }

  int open() {
    // debug setting
    int sds_write_value = 0;
    pmemobj_ctl_set(nullptr, "sds.at_create", &sds_write_value);

    pmemContext_.pop =
        pmemobj_open(diskInfo_->path.c_str(), PMEMOBJ_ALLOCATOR_LAYOUT_NAME);
    if (pmemContext_.pop == nullptr) {
      return -1;
    }

    if (server_) {
      base_ck = server_->register_rma_buffer(
          reinterpret_cast<char *>(pmemContext_.pop), diskInfo_->size);
      assert(base_ck != nullptr);
      log_->get_console_log()->info(
          "successfully registered Persistent Memory(" + diskInfo_->path +
          ") as RDMA region");
    }

    pmemContext_.poid = pmemobj_root(pmemContext_.pop, sizeof(struct Base));
    pmemContext_.base = (struct Base *)pmemobj_direct(pmemContext_.poid);
    PMEMoid next = pmemContext_.base->head;
    while (next.off != 0 && next.pool_uuid_lo != 0) {
      if (update_meta(next)) {
        return -1;
      }
      struct block_entry *bep = (struct block_entry *)pmemobj_direct(next);
      next = bep->hdr.next;
    }
    return 0;
  }

  void close() {
    pmemobj_close(pmemContext_.pop);
    free_meta();
  }

  int update_meta(const PMEMoid &oid) {
    std::lock_guard<std::mutex> l(mtx);
    struct block_entry *bep = (struct block_entry *)pmemobj_direct(oid);
    if (!index_map.count(bep->hdr.addr)) {
      index_map[bep->hdr.addr] = oid;
    } else {
      assert("invalide operation.");
    }
    return 0;
  }

  int free_meta() {
    std::lock_guard<std::mutex> l(mtx);
    index_map.clear();
  }

 private:
  Log *log_;
  DiskInfo *diskInfo_;
  NetworkServer *server_;
  int wid_;
  PmemContext pmemContext_;
  std::mutex mtx;
  unordered_map<uint64_t, PMEMoid> index_map;
  uint64_t total = 0;
  char str[1048576];
  Chunk *base_ck;
};

#endif  // PMPOOL_PMEMALLOCATOR_H_
