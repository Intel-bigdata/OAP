#ifndef PMEMSTORE_H
#define PMEMSTORE_H

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>

#include <string>
#include <iostream>
#include <cassert>
#include <atomic>

#include <libpmemobj.h>
#include <libcuckoo/cuckoohash_map.hh>

#include "xxhash.hpp"

#define PMEMKV_LAYOUT_NAME "pmemkv_layout"

// block header stored in pmem
struct block_hdr {
  PMEMoid next;
  PMEMoid pre;
  uint64_t key;
  uint64_t size;
};

// block data entry stored in pmem
struct block_entry {
  struct block_hdr hdr;
  PMEMoid data;
};

// pmem root entry
struct base {
  PMEMoid head;
  PMEMoid tail;
  PMEMrwlock rwlock;
  uint64_t bytes_written;
};

// block metadata stored in memory
struct block_meta {
  block_meta* next;
  uint64_t off;
  uint64_t size;
  block_entry* bep;
  PMEMoid beo;
};

struct block_meta_list {
  block_meta* head;
  block_meta* tail;
  uint64_t total_size;
  uint64_t length;
};

// block data stored in memory 
struct memory_block {
  char* data;
  uint64_t size;
};

// block metadata stored in memory
struct memory_meta {
  uint64_t* meta;
  uint64_t length;
};

// pmem data allocation types
enum types {
  BLOCK_ENTRY_TYPE,
  DATA_TYPE,
  MAX_TYPE
};

/*
pmemkv data and index were stored in persistent memory.
data and index structure:
base[head,                                                       tail]
     |                                                            |
     block_entry_1[block_hdr[next, key, size], data]              |
                          |                                       |
                          block_entry_2[...[next...]]             |
                                         |                        |
                                         block_entry_3[...[next...]]

index map was stored in memory, rebuild index map when opening pmemkv
index structure:
key_1 --> block_meta_list_1[block_meta, block_meta, block_meta]
key_2 --> block_meta_list_2[block_meta, block_meta, block_meta]
key_3 --> block_meta_list_3[block_meta, block_meta, block_meta]
*/
class pmemkv {
  public:
    explicit pmemkv(const char* dev_path_) : pmem_pool(nullptr), dev_path(dev_path_), bp(nullptr) {
      if (create()) {
        int res = open();
        if (res) {
          std::cout << "failed to open pmem pool, errmsg: " << pmemobj_errormsg() << std::endl; 
        }
      }
    }

    ~pmemkv() {
      close();
    }

    pmemkv(const pmemkv&) = delete;
    pmemkv& operator= (const pmemkv&) = delete;

    int put(std::string &key, const char* buf, const uint64_t count) {
      xxh::hash64_t key_i = xxh::xxhash<64>(key);
      // set the return point
      jmp_buf env;
      if (setjmp(env)) {
        // end the transaction
        (void) pmemobj_tx_end();
        return -1;
      }

      // begin a transaction, also acquiring the write lock for the data
      if (pmemobj_tx_begin(pmem_pool, env, TX_PARAM_RWLOCK, &bp->rwlock,
          TX_PARAM_NONE)) {
        perror("pmemobj_tx_begin failed in pmemkv put");
        return -1;
      }
      // allocate the new node to be inserted
      PMEMoid beo = pmemobj_tx_alloc(sizeof(struct block_entry), BLOCK_ENTRY_TYPE);
      if (beo.off == 0) {
        (void) pmemobj_tx_end();
        perror("pmemobj_tx_alloc failed in pmemkv put");
        return -1;
      }
      struct block_entry* bep = (struct block_entry*)pmemobj_direct(beo);
      bep->data = pmemobj_tx_zalloc(count, DATA_TYPE);
      if (bep->data.off == 0) {
        (void) pmemobj_tx_end();
        perror("pmemobj_tx_zalloc failed in pmemkv put");
        return -1;
      }
      char* pmem_data = (char*)pmemobj_direct(bep->data);
      memcpy(pmem_data, buf, count);
      bep->hdr.pre = bp->tail;
      bep->hdr.next = OID_NULL;
      bep->hdr.key = key_i;
      bep->hdr.size = count;

      // add the modified root object to the undo data
      pmemobj_tx_add_range(bo, 0, sizeof(struct base));
      if (bp->tail.off == 0) {
        // update head
        bp->head = beo;
      } else {
        // add the modified tail entry to the undo data
        pmemobj_tx_add_range(bp->tail, 0, sizeof(struct block_entry));
        ((struct block_entry*)pmemobj_direct(bp->tail))->hdr.next = beo;
      }

      bp->tail = beo; // update tail
      bp->bytes_written += count;
      pmemobj_tx_commit();
      (void) pmemobj_tx_end();

      // update in-memory index
      if (update_meta(bep, beo)) {
        return -1;
      }
      return 0;
    }

    int get(std::string &key, struct memory_block *mb) {
      xxh::hash64_t key_i = xxh::xxhash<64>(key);
      if (!index_map.contains(key_i)) {
        perror("no such key in index_map");
        return -1;
      }
      if (pmemobj_rwlock_rdlock(pmem_pool, &bp->rwlock) != 0) {
		    return -1;
      }
      struct block_meta_list* bml = nullptr;
      index_map.find(key_i, bml);
      assert(bml != nullptr);
      struct block_meta* bm = bml->head;
      uint64_t read_offset = 0;
      while (bm != nullptr && (read_offset+bm->size <= mb->size)) {
        memcpy(mb->data+read_offset, (char*)bm->off, bm->size);
        read_offset += bm->size;
        assert(read_offset <= mb->size);
        bm = bm->next;
      }
      pmemobj_rwlock_unlock(pmem_pool, &bp->rwlock);
      return 0;
    }

    int removeBlocks(uint64_t shuffleId, uint64_t mapId, uint64_t partitionId){
        for (int i = 0; i < partitionId; i++){
            std::string key = "shuffle_" + std::to_string(shuffleId) + "_" + std::to_string(mapId) + "_" + std::to_string(i);
            //std::cout<<"key="<<key<<std::endl;
            remove(key);
        }
        return 0;
    }

    int getBytesWritten(){
        return bp->bytes_written;
    }

    int remove(std::string &key){
      std::lock_guard<std::mutex> l(mtx);
      xxh::hash64_t key_i = xxh::xxhash<64>(key);
      if (!index_map.contains(key_i)){
        //std::cout<<"Data with key="<<key_i<<" doesn't exist"<<std::endl;
        return -1;
      }

      // set the return point
      jmp_buf env;
      if (setjmp(env)) {
        // end the transaction
        (void) pmemobj_tx_end();
        return -1;
      }

      // begin a transaction, also acquiring the write lock for the data
      if (pmemobj_tx_begin(pmem_pool, env, TX_PARAM_RWLOCK, &bp->rwlock,TX_PARAM_NONE)) {
        std::cout<<"pmemobj_tx_begin failed in pmemkv put"<<std::endl;
        perror("pmemobj_tx_begin failed in pmemkv put");
        return -1;
      }

      // Remove data by block meta list
      struct block_meta_list* bml;
      index_map.find(key_i, bml);

      struct block_meta* cur = bml->head;

      //Delete block_entry in bml one by one
      while (cur != nullptr) {
        block_entry* bep = cur->bep;
        //Node to be deleted is at the head
        if(bep == (struct block_entry*)pmemobj_direct(bp->head)){
            if (pmemobj_direct(bep->hdr.next) == nullptr){
                //There is only one block_entry
                bp->head = OID_NULL;
                bp->tail = OID_NULL;
                bp->bytes_written = bp->bytes_written - bep->hdr.size;
                pmemobj_free(&bep->data);
                pmemobj_free(&cur->beo);
                struct block_meta *next = cur->next;
                cur->next = nullptr;
                std::free(cur);
                cur = next;
                bytes_allocated -= sizeof(block_meta);
                //std::cout<<"Only key in head is removed. key="<<key<<std::endl;
                continue;
            }

            //There are two or more block_entry
            bp->head = bep->hdr.next;
            block_entry* new_head_pointer = (struct block_entry*)pmemobj_direct(bp->head);
            new_head_pointer->hdr.pre = OID_NULL;
            bp->bytes_written = bp->bytes_written - bep->hdr.size;
            pmemobj_free(&bep->data);
            pmemobj_free(&cur->beo);
            struct block_meta *next = cur->next;
            cur->next = nullptr;
            std::free(cur);
            cur = next;
            bytes_allocated -= sizeof(block_meta);
            //std::cout<<"Key in head is removed. key="<<key<<std::endl;
            continue;
        }
        //Node to be deleted is at the tail
        if (pmemobj_direct(bep->hdr.next) == nullptr){
            //The one node scenario is already covered in head judgement, there are two or more nodes here
            struct block_entry* prebep = (struct block_entry*)pmemobj_direct(bep->hdr.pre);
            prebep->hdr.next = OID_NULL;
            bp->tail = bep->hdr.pre;
            if((struct block_entry*)pmemobj_direct(bp->tail) == nullptr){
                std::cout<<"Error. The bp->tail should not be nullptr"<<std::endl;
            }
            bp->bytes_written = bp->bytes_written - bep->hdr.size;
            pmemobj_free(&bep->data);
            pmemobj_free(&cur->beo);
            struct block_meta *next = cur->next;
            cur->next = nullptr;
            std::free(cur);
            cur = next;
            bytes_allocated -= sizeof(block_meta);
            //std::cout<<"Key in tail is removed. key="<<key<<std::endl;
            continue;
        }

        //Node to be deleted is at the middle, no head or tail, there are at least three nodes
        struct block_entry* prebep = (struct block_entry*)pmemobj_direct(bep->hdr.pre);
        prebep->hdr.next = bep->hdr.next;
        struct block_entry* nextbep = (struct block_entry*)pmemobj_direct(bep->hdr.next);
        nextbep->hdr.pre = bep->hdr.pre;
        bp->bytes_written = bp->bytes_written - bep->hdr.size;
        pmemobj_free(&bep->data);
        pmemobj_free(&cur->beo);
        struct block_meta *next = cur->next;
        cur->next = nullptr;
        std::free(cur);
        cur = next;
        bytes_allocated -= sizeof(block_meta);
        //std::cout<<"Key neither in head nor in tail is removed. key="<<key<<std::endl;
      }

      bytes_allocated -= sizeof(block_meta_list);
      std::free(bml);
      index_map.erase(key_i);
      pmemobj_tx_commit();
      (void) pmemobj_tx_end();
      return 0;
    }

    int get_value_size(std::string &key, uint64_t* size) {
      xxh::hash64_t key_i = xxh::xxhash<64>(key);
      if (!index_map.contains(key_i)) {
        *size = 0;
        return -1;
      } else {
        struct block_meta_list* bml;
        index_map.find(key_i, bml);
        *size = bml->total_size;
        return 0;
      }
    }

    int get_meta(std::string &key, struct memory_meta* mm) {
      xxh::hash64_t key_i = xxh::xxhash<64>(key);
      if (!index_map.contains(key_i)) {
        mm = nullptr;
      } else {
        struct block_meta_list* bml;
        index_map.find(key_i, bml);
        uint64_t index = 0;
        struct block_meta *next = bml->head;
        while (next != nullptr) {
          assert(mm->meta != nullptr);
          mm->meta[index++] = next->off;
          mm->meta[index++] = next->size;
          next = next->next;
        }
        mm->length = index;
      }
      return 0;
    }

    int get_meta_size(std::string &key, uint64_t* size) {
      xxh::hash64_t key_i = xxh::xxhash<64>(key);
      if (!index_map.contains(key_i)) {
        *size = 0;
        return -1;
      } else {
        struct block_meta_list* bml;
        index_map.find(key_i, bml);
        *size = bml->length;
        return 0;
      }
    }

    int dump_all() {
      if (pmemobj_rwlock_rdlock(pmem_pool, &bp->rwlock) != 0) {
		    return -1;
      }
      struct block_entry* next_bep = (struct block_entry*)pmemobj_direct(bp->head);
      uint64_t read_offset = 0;
      while (next_bep != nullptr) {
        char* pmem_data = (char*)pmemobj_direct(next_bep->data);
        char* tmp = (char*)std::malloc(next_bep->hdr.size);
        memcpy(tmp, pmem_data, next_bep->hdr.size);
        std::cout << "key " << next_bep->hdr.key << " value " << pmem_data << std::endl;
        read_offset += next_bep->hdr.size;
        std::free(tmp);
        next_bep = (struct block_entry*)pmemobj_direct(next_bep->hdr.next);
      }
      pmemobj_rwlock_unlock(pmem_pool, &bp->rwlock);
      return 0; 
    }

    int reverse_dump_all() {
      if (pmemobj_rwlock_rdlock(pmem_pool, &bp->rwlock) != 0) {
            return -1;
      }
      struct block_entry* next_bep = (struct block_entry*)pmemobj_direct(bp->tail);
      uint64_t read_offset = 0;
      while (next_bep != nullptr) {
        char* pmem_data = (char*)pmemobj_direct(next_bep->data);
        char* tmp = (char*)std::malloc(next_bep->hdr.size);
        memcpy(tmp, pmem_data, next_bep->hdr.size);
        std::cout << "key " << next_bep->hdr.key << " value " << pmem_data << std::endl;
        read_offset += next_bep->hdr.size;
        std::free(tmp);
        next_bep = (struct block_entry*)pmemobj_direct(next_bep->hdr.pre);
      }
      pmemobj_rwlock_unlock(pmem_pool, &bp->rwlock);
      return 0;
    }

    int dump_meta() {
      std::cout << "pmemkv total bytes written " << bp->bytes_written << std::endl;
      std::lock_guard<std::mutex> l(mtx);
      auto locked_index_map = index_map.lock_table();
      for (const auto &it : locked_index_map) {
        struct block_meta_list* bml = it.second;
        for (struct block_meta* ptr = bml->head; ptr != nullptr; ptr = ptr->next) {
          std::cout << "off " << ptr->off << " size " << ptr->size << std::endl;
        }
      }
      return 0;
    }

    int free_all() {
      //std::cout<<"free's begining: bp->bytes_written: "<<bp->bytes_written<<std::endl;
      // don't implement transaction here, if any issue happens, we need to rebuild the pmem pool.
      PMEMoid next_beo = bp->head;
      struct block_entry* next_bep = (struct block_entry*)pmemobj_direct(next_beo);
      while (next_bep != nullptr) {
        // add block entry to undo log
        PMEMoid pre_beo =  next_beo;
        struct block_entry* pre_bep = next_bep;
        pmemobj_free(&pre_beo);
        pmemobj_free(&pre_bep->data);
        next_beo = next_bep->hdr.next;
        next_bep = (struct block_entry*)pmemobj_direct(next_beo);
        bp->bytes_written -= pre_bep->hdr.size;
      }
      // add root block to undo log
      bp->head = OID_NULL;
      bp->tail = OID_NULL;
      //std::cout<<"free: bp->bytes_written: "<<bp->bytes_written<<std::endl;
      assert(bp->bytes_written == 0);

      // free metadata
      if (free_meta()) {
        return -1;
      }

      return 0;
    }

    uint64_t get_root() {
      return (uint64_t)pmem_pool;
    }
  private:
    int create() {
      // debug setting
      int sds_write_value = 0;
      pmemobj_ctl_set(nullptr, "sds.at_create", &sds_write_value);

      pmem_pool = pmemobj_create(dev_path, PMEMKV_LAYOUT_NAME, 0, 0666);
      if (pmem_pool == nullptr) {
        return -1;
      }
      bo = pmemobj_root(pmem_pool, sizeof(struct base));
      bp = (struct base*)pmemobj_direct(bo);
      bp->head = OID_NULL;
      bp->tail = OID_NULL;
      bp->bytes_written = 0;

      return 0;
    }

    int open() {
      // debug setting
      int sds_write_value = 0;
      pmemobj_ctl_set(nullptr, "sds.at_create", &sds_write_value);
      pmem_pool = pmemobj_open(dev_path, PMEMKV_LAYOUT_NAME);
      if (pmem_pool == nullptr) {
        return -1;
      }
      // rebuild in-memory index
      // walk through all the block entry in pmem, don't need lock here
      bo = pmemobj_root(pmem_pool, sizeof(struct base));
      bp = (struct base*)pmemobj_direct(bo);
      struct block_entry *next = (struct block_entry*)pmemobj_direct(bp->head);
      PMEMoid next_beo = bp->head;
      while (next != nullptr) {
        if (update_meta(next, next_beo)) {
          return -1;
        }
        next_beo = next->hdr.next;
        next = (struct block_entry*)pmemobj_direct(next_beo);
      }
      return 0;
    }
    
    void close() {
      pmemobj_close(pmem_pool);
      free_meta();
    }

    int free_meta() {
      std::lock_guard<std::mutex> l(mtx);
      // iterate index map, free all the in-memory index
      auto locked_index_map = index_map.lock_table();
      for (const auto &it : locked_index_map) {
        struct block_meta_list* bml = it.second;
        struct block_meta* cur = bml->head;
        while (cur != nullptr) {
          struct block_meta* next = cur->next;
          cur->next = nullptr;
          std::free(cur);
          bytes_allocated -= sizeof(block_meta);
          cur = next;
        }
        std::free(bml);
        bytes_allocated -= sizeof(block_meta_list);
        bml = nullptr;
      }
      locked_index_map.clear();
      assert(bytes_allocated == 0);
      return 0;
    }

    int update_meta(struct block_entry* bep, PMEMoid beo) {
      std::lock_guard<std::mutex> l(mtx);
      if (!index_map.contains(bep->hdr.key)) {  // allocate new block_meta_list
        struct block_meta* bm = (struct block_meta*)std::malloc(sizeof(block_meta));
        if (!bm) {
          perror("malloc error in pmemkv update_meta");
          return -1;
        }
        bytes_allocated += sizeof(block_meta);
        bm->off = (uint64_t)pmemobj_direct(bep->data);
        bm->size = bep->hdr.size;
        bm->next = nullptr;
        bm->bep = bep;
        bm->beo = beo;
        struct block_meta_list* bml = (struct block_meta_list*)std::malloc(sizeof(block_meta_list));
        if (!bml) {
          perror("malloc error in pmemkv update_meta");
          return -1;
        }
        bytes_allocated += sizeof(block_meta_list);
        bml->head = bm;
        bml->tail = bml->head;
        bml->total_size = 0;
        bml->total_size += bm->size;
        bml->length = 0;
        bml->length += 1;
        index_map.insert(bep->hdr.key, bml);
      } else {   // append block_meta to existing block_meta_list
        struct block_meta_list* bml = nullptr;
        index_map.find(bep->hdr.key, bml);
        struct block_meta* bm = (struct block_meta*)std::malloc(sizeof(block_meta));
        if (!bm) {
          perror("malloc error in pmemkv update_meta");
          return -1;
        }
        bytes_allocated += sizeof(block_meta);
        bm->off = (uint64_t)pmemobj_direct(bep->data);
        bm->size = bep->hdr.size;
        bm->next = nullptr;
        bm->bep = bep;
        bm->beo = beo;
        bml->tail->next = bm;
        bml->tail = bm;
        bml->total_size += bm->size;
        bml->length += 1;
      }
      return 0;
    }

  private:
    PMEMobjpool* pmem_pool;
    const char* dev_path;
    struct base* bp;
    PMEMoid bo;
    libcuckoo::cuckoohash_map<uint64_t, block_meta_list*> index_map;
    std::mutex mtx;
    std::atomic<uint64_t> bytes_allocated{0};
};

#endif
