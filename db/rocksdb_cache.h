#ifndef ROCKSDB_CACHE_H
#define ROCKSDB_CACHE_H

#include <csignal>
#define ASSERT(TEST) if(!(TEST)) raise(SIGTRAP);

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <cstddef>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <gflags/gflags.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "rocksdb/statistics.h"
#include "rocksdb/options.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/statistics.h"
#include "rocksdb/perf_context.h"
#include "port/port.h"
#include "util/bit_set.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stack_trace.h"
#include "util/string_util.h"
#include "util/statistics.h"
#include "util/testutil.h"
#include "hdfs/env_hdfs.h"
#include "utilities/merge_operators.h"

#include "../ssd-exp/ripq_cpp/utils.h"
#include "../ssd-exp/ripq_cpp/snlru.h"

#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <string>
#include <fstream>

using namespace rocksdb;
using boost::lockfree::queue;
using namespace std;

static const bool MyDebug = true;
// DECLARE_string(trace_file);
// DECLARE_int32(num_segments);
// DECLARE_int32(queue_length);
// DECLARE_int64(rocksdb_cache_size);

struct WriteEntry{
  string* key;
  int size;
  // 1 if writing, 0 if deleting
  bool write;
  WriteEntry(string* key_=NULL, int size_=0, bool write_=true) :
      key(key_), size(size_), write(write_) {
  }
};

struct RocksDBCache {
  DB* db_;
  WriteOptions& write_options_;

  queue<string*> read_queue;
  queue<WriteEntry> write_queue;
  queue<string*> delete_queue;
  atomic<int> read_queue_size, write_queue_size, delete_queue_size;

  atomic<bool> running;
  atomic<int64_t> successful_reads, failed_reads;
  atomic<int64_t> read_bytes, write_bytes, deleted_items;

  int entries_per_batch;

  int64_t last_hit, last_miss, last_hit_bytes, last_miss_bytes;
  timespec recent_ts;

  int64_t last_read_bytes, last_write_bytes;
  double rt_hit_rate, rt_byte_hit_rate;

  int lines_read;

  RandomGenerator gen;
  SNLRUCache* cache;

  // static RocksDBCache* getInstance(DB* db, WriteOptions& write_options){
  //   if (instance == NULL) {
  //     instance = new RocksDBCache(db, write_options);
  //   }
  //   return instance;
  // }

  RocksDBCache(DB* db, WriteOptions& write_options) :
      db_(db), write_options_(write_options),
      read_queue(FLAGS_queue_length), write_queue(FLAGS_queue_length),
      delete_queue(FLAGS_queue_length)
    {
    entries_per_batch = 100;
    running = true;

    successful_reads = 0;
    failed_reads = 0;
    read_bytes = 0;
    write_bytes = 0;
    deleted_items = 0;

    read_queue_size = 0;
    write_queue_size = 0;
    delete_queue_size = 0;

    last_hit = 0;
    last_miss = 0;
    last_hit_bytes = 0;
    last_miss_bytes = 0;
    last_read_bytes = 0;
    last_write_bytes = 0;
    rt_hit_rate = 0.0;
    rt_byte_hit_rate = 0.0;

    clock_gettime(MY_TIMER, &recent_ts);
  }

  void procTrace(ThreadState* thread) {
    auto cache_size = (int64_t) FLAGS_rocksdb_cache_size;
    auto num_segments = (int) FLAGS_num_segments;
    cache = new SNLRUCache(cache_size, num_segments);

    string trace_file = FLAGS_trace_file;
      if (MyDebug) {
        cout << trace_file << endl;
      }
    ifstream fin(trace_file);
    string line;
    lines_read = 0;
    string delim = "\t";
    while( getline(fin, line) ) {
      auto ele = split(line, delim);
      auto key = ele[0];
      auto size = stoi(ele[1]);

      bool hit = cache->insert(key, size);
      if (hit)
        cache->record_hit(size);
      else
        cache->record_miss(size);

      auto key_ptr = new string(key);
      if (hit) {
        while(!read_queue.bounded_push(key_ptr));
        read_queue_size++;
      }
      else {
        while(!write_queue.bounded_push(
                WriteEntry(key_ptr, size, true)));
        write_queue_size++;
      }

      for(auto& evicted_key : *cache->get_evicted()) {
        // can't evict the newly inserted item
        ASSERT(evicted_key != key);
        auto key_ptr = new string(evicted_key);
        (void)key_ptr;
        while(!write_queue.bounded_push(
                WriteEntry(key_ptr, -1, false)));
        write_queue_size++;
      }
      cache->clear_evicted();
      ++lines_read;

      // ASSERT(lines_read % 100000 != 0);
      if (lines_read % 100000 == 0) {
        report_rt_stats(lines_read);
      }
    }

    running = false;
    cout << "total lines read " << lines_read << endl;
    printf("read_bytes %.1e, write_bytes %.1e, deleted_items %.1e\n",
           read_bytes+0.0, write_bytes+0.0, deleted_items+0.0);
    cache->report_stats();
  }

  void report_rt_stats(int lines_read) {
    double ti = -1;
    if ((ti = elapsed_seconds(recent_ts)) >= 1) {
      double read_tt = (read_bytes - last_read_bytes) / ti / (1 << 20);
      double write_tt = (write_bytes - last_write_bytes) / ti / (1 << 20);
      last_read_bytes = read_bytes;
      last_write_bytes = write_bytes;

      double rt_requests = cache->hit + cache->miss - last_hit - last_miss;
      double rt_bytes = cache->hit_bytes + cache->miss_bytes \
        - last_hit_bytes - last_miss_bytes;
      rt_hit_rate = (cache->hit - last_hit) / rt_requests;
      rt_byte_hit_rate = (cache->hit_bytes - last_hit_bytes) / rt_bytes;

      ASSERT(rt_hit_rate > 0);

      last_hit = cache->hit;
      last_miss = cache->miss;
      last_hit_bytes = cache->hit_bytes;
      last_miss_bytes = cache->miss_bytes;

      double rqs = read_queue_size;
      double wqs = write_queue_size;
      double dqs = delete_queue_size;

      // auto trailer = lines_read % 100000 == 0 ? "\n" : "\r";
      auto trailer = "\n";
      printf("lines_read %d, read-throughput: %.1f MB/sec, write-throughput: %.2f MB/sec, "
             "rt-hit-rate: %.3f, rt-byte-hit-rate: %.3f, "
             "rqs: %.1e, wqs: %.1e, dqs: %.1e"
             "%s",
             lines_read,
             read_tt, write_tt, rt_hit_rate, rt_byte_hit_rate,
             rqs, wqs, dqs,
             trailer);

      clock_gettime(MY_TIMER, &recent_ts);
    }
  }

  void readFromQueue(ThreadState* thread) {
    // TODO true?
    ReadOptions options(FLAGS_verify_checksum, true);

    string *key = NULL;
    while(running) {
      if (!read_queue.pop(key))
        continue;
      read_queue_size--;

      // TODO trailing? prefix_seek?
      options.tailing = true;
      options.prefix_seek = (FLAGS_prefix_size == 0);
      std::string value;

      if (db_->Get(options, *key, &value).ok()) {
        successful_reads++;
        read_bytes += value.size();
      }
      else {
        failed_reads++;
      }
      delete key;
      thread->stats.FinishedSingleOp(db_);
    }
  }

  void writeFromQueue(ThreadState* thread) {
    WriteBatch batch;
    WriteEntry entry;
    int batch_size;

    batch.Clear();
    batch_size = 0;

    while(running) {
      if (!write_queue.pop(entry)) {
        continue;
      }
      write_queue_size--;

      if (entry.write) {
        batch.Put(*entry.key, gen.Generate(entry.size));
        write_bytes += entry.size;
      }
      else {
        batch.Delete(*entry.key);
        deleted_items += 1;
      }

      batch_size += 1;
      delete entry.key;

      if (batch_size >= entries_per_batch) {
        auto s = db_->Write(write_options_, &batch);
        thread->stats.FinishedSingleOp(db_);
        batch.Clear();
        batch_size = 0;
      }
    }
  }

  // void deleteFromQueue(ThreadState* thread) {
  //   WriteBatch batch;
  //   WriteEntry entry;
  //   int batch_size;

  //   batch.Clear();
  //   batch_size = 0;

  //   string *key;
  //   while(running) {
  //     if (!delete_queue.pop(key)) {
  //       continue;
  //     }
  //     delete_queue_size--;

  //     batch.Delete(*key);
  //     deleted_items += 1;
  //     batch_size += 1;
  //     delete key;

  //     if (batch_size >= entries_per_batch) {
  //       auto s = db_->Write(write_options_, &batch);
  //       thread->stats.FinishedSingleOp(db_);
  //       // if (MyDebug)
  //       //   cout << "delete " << batch_size
  //       //        << " deleted items " << deleted_items << endl;
  //       batch.Clear();
  //       batch_size = 0;
  //     }

  //   }
  // }

};

#endif
