//
//  RedisAdapterLite.hpp
//
//  A simplified Redis adapter using hiredis directly.
//  No templates, no cluster support, memcpy-based serialization.
//

#pragma once

#include "RAL_Types.hpp"
#include "RAL_Helpers.hpp"
#include "HiredisConnection.hpp"
#include "ThreadPool.hpp"
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

#ifndef REDIS_ADAPTER_GIT_COMMIT
#define REDIS_ADAPTER_GIT_COMMIT unknown
#endif
#define STRINGIFY(s) #s
#define STRINGIFY_DEFINE(s) STRINGIFY(s)
#define RAL_VERSION STRINGIFY_DEFINE(REDIS_ADAPTER_GIT_COMMIT)

class RedisAdapterLite
{
public:
  //--- Construction / Destruction ---
  RedisAdapterLite(const std::string& baseKey, const RAL_Options& options = {});
  ~RedisAdapterLite();

  RedisAdapterLite(const RedisAdapterLite&) = delete;
  RedisAdapterLite& operator=(const RedisAdapterLite&) = delete;

  //--- Add single values ---
  RAL_Time addString(const std::string& subKey, const std::string& data,
                     const RAL_AddArgs& args = {});
  RAL_Time addDouble(const std::string& subKey, double data,
                     const RAL_AddArgs& args = {});
  RAL_Time addInt(const std::string& subKey, int64_t data,
                  const RAL_AddArgs& args = {});
  RAL_Time addBlob(const std::string& subKey, const void* data, size_t size,
                   const RAL_AddArgs& args = {});
  RAL_Time addAttrs(const std::string& subKey, const Attrs& data,
                    const RAL_AddArgs& args = {});

  //--- Batch add blobs (pipelined, single round-trip) ---
  struct BlobEntry {
    std::string subKey;
    const void* data;
    size_t size;
    RAL_AddArgs args;
  };
  std::vector<RAL_Time> addBlobBatch(const std::vector<BlobEntry>& entries);

  //--- Get single value (most recent at or before maxTime) ---
  RAL_Time getString(const std::string& subKey, std::string& dest,
                     const RAL_GetArgs& args = {});
  RAL_Time getDouble(const std::string& subKey, double& dest,
                     const RAL_GetArgs& args = {});
  RAL_Time getInt(const std::string& subKey, int64_t& dest,
                  const RAL_GetArgs& args = {});
  RAL_Time getBlob(const std::string& subKey, std::vector<uint8_t>& dest,
                   const RAL_GetArgs& args = {});
  RAL_Time getAttrs(const std::string& subKey, Attrs& dest,
                    const RAL_GetArgs& args = {});

  //--- Get range (forward, minTime to maxTime) ---
  TimeStringList getStrings(const std::string& subKey, const RAL_GetArgs& args = {});
  TimeDoubleList getDoubles(const std::string& subKey, const RAL_GetArgs& args = {});
  TimeIntList    getInts(const std::string& subKey, const RAL_GetArgs& args = {});
  TimeBlobList   getBlobs(const std::string& subKey, const RAL_GetArgs& args = {});
  TimeAttrsList  getAttrsRange(const std::string& subKey, const RAL_GetArgs& args = {});

  //--- Get range (reverse, before maxTime with count) ---
  TimeStringList getStringsBefore(const std::string& subKey, const RAL_GetArgs& args = {});
  TimeDoubleList getDoublesBefore(const std::string& subKey, const RAL_GetArgs& args = {});
  TimeIntList    getIntsBefore(const std::string& subKey, const RAL_GetArgs& args = {});
  TimeBlobList   getBlobsBefore(const std::string& subKey, const RAL_GetArgs& args = {});
  TimeAttrsList  getAttrsBefore(const std::string& subKey, const RAL_GetArgs& args = {});

  //--- Bulk add (multiple timestamped items) ---
  std::vector<RAL_Time> addStrings(const std::string& subKey, const TimeStringList& data,
                                    uint32_t trim = 1);
  std::vector<RAL_Time> addDoubles(const std::string& subKey, const TimeDoubleList& data,
                                    uint32_t trim = 1);
  std::vector<RAL_Time> addInts(const std::string& subKey, const TimeIntList& data,
                                 uint32_t trim = 1);
  std::vector<RAL_Time> addBlobs(const std::string& subKey, const TimeBlobList& data,
                                  uint32_t trim = 1);
  std::vector<RAL_Time> addAttrsBatch(const std::string& subKey, const TimeAttrsList& data,
                                       uint32_t trim = 1);

  //--- Connection ---
  bool connected();

  //--- Watchdog ---
  bool addWatchdog(const std::string& dogname, uint32_t expiration);
  bool petWatchdog(const std::string& dogname, uint32_t expiration);
  std::vector<std::string> getWatchdogs();

  //--- Key management ---
  bool copy(const std::string& srcSubKey, const std::string& dstSubKey,
            const std::string& baseKey = "");
  bool rename(const std::string& srcSubKey, const std::string& dstSubKey);
  bool del(const std::string& subKey);

  //--- Pub/Sub ---
  bool publish(const std::string& subKey, const std::string& message,
               const std::string& baseKey = "");
  bool subscribe(const std::string& subKey, SubCallback func,
                 const std::string& baseKey = "");
  bool unsubscribe(const std::string& subKey,
                   const std::string& baseKey = "");

  //--- Stream Readers ---
  bool setDeferReaders(bool defer);
  bool addReader(const std::string& subKey, ReaderCallback func,
                 const std::string& baseKey = "");
  bool removeReader(const std::string& subKey,
                    const std::string& baseKey = "");

private:
  //--- Key building ---
  std::string build_key(const std::string& subKey,
                        const std::string& baseKey = "") const;
  std::pair<std::string, std::string> split_key(const std::string& key) const;

  //--- Internal stream helpers ---
  RAL_Time add_entry(const std::string& key, const Attrs& attrs, const RAL_AddArgs& args);
  RAL_Time add_entry_single(const std::string& key,
                             const char* data, size_t size,
                             const RAL_AddArgs& args);

  TimeAttrsList get_forward(const std::string& key,
                            RAL_Time minTime, RAL_Time maxTime, uint32_t count);
  TimeAttrsList get_reverse(const std::string& key,
                            RAL_Time maxTime, uint32_t count);

  //--- Reader infrastructure ---
  struct ReaderInfo
  {
    std::thread thread;
    redisContext* ctx = nullptr;    // per-reader context for blocking XREAD
    std::unordered_map<std::string, std::vector<ReaderCallback>> subs;
    std::unordered_map<std::string, std::string> key_ids;
    std::string stop_key;
    std::atomic<bool> run{false};
  };

  uint32_t reader_token(const std::string& key);
  bool start_reader(uint32_t token);
  bool stop_reader(uint32_t token);

  std::mutex _reader_mutex;
  std::unordered_map<uint32_t, ReaderInfo> _readers;
  std::atomic<bool> _readers_defer{false};

  //--- Pub/Sub infrastructure ---
  struct SubInfo
  {
    std::thread thread;
    redisContext* ctx = nullptr;
    std::unordered_map<std::string, SubCallback> channels;
    std::atomic<bool> run{false};
  };
  SubInfo _sub;
  std::mutex _sub_mutex;
  bool restart_subscriber();
  void stop_subscriber();

  //--- Reconnect ---
  int32_t check_reconnect(int32_t result);
  std::atomic<bool> _connecting{false};
  std::thread _reconnect_thread;

  //--- Watchdog ---
  std::string _watchdog_key;
  std::thread _watchdog_thd;
  std::mutex _watchdog_mutex;
  std::condition_variable _watchdog_cv;
  std::atomic<bool> _watchdog_run{false};

  //--- Core state ---
  RAL_Options _options;
  HiredisConnection _redis;
  std::string _base_key;
  ThreadPool _worker_pool;

  const std::string STOP_STUB = "<$-STOP-$>";
};

// For the old template-based API, include "RedisAdapter.hpp" instead.
