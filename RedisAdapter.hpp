//
//  RedisAdapter.hpp
//
//  This file contains the RedisAdapter class definition

#pragma once
#include "RedisStreamData.hpp"
#include "RedisTime.hpp"

#if defined(MOCK_REDIS_ADAPTER)
#include "mock/MockRedisAdapter.hpp"
using RedisAdapter = MockRedisAdapter;
#else // defined(MOCK_REDIS_ADAPTER)
#include "RedisConnection.hpp"
#include "ThreadPool.hpp"
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <cstring>
#include <limits>
#include <memory>
#include <type_traits>
#include <queue>
#include <optional>
#include <tuple>

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  define RA_VERSION
//
//  The version of this RedisAdapter (the stringified git commit hash)
//  Get git commit hash during build and pass to compile using -D as REDIS_ADAPTER_GIT_COMMIT
//
#ifndef REDIS_ADAPTER_GIT_COMMIT
#define REDIS_ADAPTER_GIT_COMMIT unknown
#endif
#define STRINGIFY(s) #s
#define STRINGIFY_DEFINE(s) STRINGIFY(s)
#define RA_VERSION STRINGIFY_DEFINE(REDIS_ADAPTER_GIT_COMMIT)

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  struct RA_ArgsGet, struct RA_ArgsAdd
//
//  Parameter packages used as arguments to various RedisAdapter functions, these
//  provide default parameter values, but allow you to override any of them as desired -
//  note that not all parameters are used by every function, see the comments for each
//  function for the set of parameters that are applicable
//
//  It is not expected that a user would need to use these struct names, rather it is
//  suggested that an appropriate initializer list be used directly in RedisAdapter
//  function calls - for example:
//
//    redis.getValues<string>("abc", { .minTime=1000, .maxTime=2000 });
//
struct RA_ArgsGet
{ std::string baseKey; RA_Time minTime; RA_Time maxTime; uint32_t count = 1; };

struct RA_ArgsAdd
{
  RA_Time time;
  uint32_t trim = 1;
  // Redis MAXLEN trimming is approximate by default for compatibility and
  // throughput. Set false when a hard memory/entry bound is part of the data
  // contract.
  bool approximateTrim = true;
};

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  struct RA_Options
//
struct RA_Options
{
  RedisConnection::Options cxn;
  std::string dogname;
  uint16_t workers = 1;
  uint16_t readers = 1;
  uint32_t readerProbeMs = 0;  // optional continuity inspection; per-subscription override
  uint32_t readerBatchCount = 64;  // per stream; zero is normalized to one
};

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  class RedisAdapter
//
//  Provides a framework for AD Instrumentation front-ends and back-ends to exchange
//  data, settings, status and control information via a Redis server or cluster
//
class RedisAdapter : public RedisStreamData
{
  struct ReaderOwner;
  struct ReaderRegistration;
  struct reader_info;

public:
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Containers for stream data suggested by the redis++ readme.md
  //    https://github.com/sewenew/redis-plus-plus#redis-stream
  //
  struct StreamSnapshot {
    bool connected = false;
    bool rejected = false;
    std::string id = "$";
    Attrs fields;
    bool present() const { return connected && !fields.empty() && id != "0-0"; }
  };

  using StreamKind = RedisConnection::StreamKind;
  using EpochStreamCallback = std::function<void(const std::string&, const std::string&, const StreamBatch&, uint64_t)>;
  struct ReaderStatus {
    bool active = false, connected = false, inspected = false, hasData = false;
    StreamKind streamKind = StreamKind::Unknown;
    // Empty means an unresolved future-only cursor, never a comparable ID.
    std::string cursor, observedCursor;
    uint64_t epoch = 0, readFailures = 0, readRejections = 0, socketTimeouts = 0;
    uint64_t reconnects = 0, streamResets = 0, disappearances = 0, retentionGaps = 0;
    uint64_t inspectionFailures = 0, inspectionRejections = 0;
    uint64_t callbacks = 0, entries = 0, callbackErrors = 0;
    std::chrono::steady_clock::time_point lastReceived{};
  };

  // Cancellation fences callbacks that have not passed their active check. A
  // callback past that check may still enter; consumers must fence mutations.
  class ReaderHandle {
  public:
    ReaderHandle() = default;
    ~ReaderHandle();
    ReaderHandle(ReaderHandle&& other) noexcept;
    ReaderHandle& operator=(ReaderHandle&& other) noexcept;
    ReaderHandle(const ReaderHandle&) = delete;
    ReaderHandle& operator=(const ReaderHandle&) = delete;
    void reset() noexcept;
    explicit operator bool() const;
    [[nodiscard]] ReaderStatus status() const;
  private:
    friend class RedisAdapter;
    ReaderHandle(std::weak_ptr<ReaderOwner> owner, std::shared_ptr<ReaderRegistration> registration);
    std::weak_ptr<ReaderOwner> owner_;
    std::shared_ptr<ReaderRegistration> registration_;
  };

  [[nodiscard]] StreamSnapshot getStreamSnapshot(const std::string& subKey, const std::string& baseKey = "");
  [[nodiscard]] ReaderHandle subscribeStream(const std::string& subKey, StreamCallback callback,
                               const std::string& afterId = "$", const std::string& baseKey = "",
                               uint32_t probeMs = UINT32_MAX);
  struct SubscriptionOptions {
    std::string baseKey;
    std::string afterId = "$";
    std::optional<uint32_t> probeMs;
  };
  // Throws invalid_argument for empty callbacks/invalid IDs and runtime_error
  // during shutdown. Retain the returned handle for the registration lifetime.
  [[nodiscard]] ReaderHandle subscribeStream(const std::string& subKey, StreamCallback callback,
                                              const SubscriptionOptions& options) {
    return subscribeStream(subKey, std::move(callback), options.afterId, options.baseKey, options.probeMs.value_or(UINT32_MAX));
  }
  [[nodiscard]] ReaderHandle subscribeStreamWithEpoch(const std::string& subKey, EpochStreamCallback callback,
                                                       const SubscriptionOptions& options);
  [[nodiscard]] ReaderHandle subscribeStreamWithEpoch(const std::string& subKey, EpochStreamCallback callback) {
    return subscribeStreamWithEpoch(subKey, std::move(callback), SubscriptionOptions{});
  }
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Containers for getting/setting data using RedisAdapter methods
  //
  template<typename T> using TimeVal = std::pair<RA_Time, T>;         //  analagous to Item
  template<typename T> using TimeValList = std::vector<TimeVal<T>>;   //  analagous to ItemStream

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Construction / Destruction
  //
  RedisAdapter(const std::string& baseKey, const RA_Options& options = {});

  RedisAdapter(const RedisAdapter& ra) = delete;       //  copy construction not allowed
  RedisAdapter& operator=(const RedisAdapter& ra) = delete;   //  assignment not allowed

  virtual ~RedisAdapter();

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getValues       : get data as T (T is trivial, string or Attrs) between minTime and maxTime
  //  getLists        : get data as type vector<T> (T is trivial) between minTime and maxTime
  //  getValuesBefore : get data as T (T is trivial, string or Attrs) before maxTime
  //  getListsBefore  : get data as type vector<T> (T is trivial) before maxTime
  //  getValuesAfter  : get data as T (T is trivial, string or Attrs) after minTime
  //  getListsAfter   : get data as type vector<T> (T is trivial) after minTime
  //
  //    baseKey : base key of device
  //    subKey  : sub key to get data from
  //    minTime : lowest time to get data for
  //    maxTime : highest time to get data for
  //    count   : max number of items to get
  //    return  : TimeValList of TimeVal<T>
  //
  template<typename T> TimeValList<T>
  getValues(const std::string& subKey, const RA_ArgsGet& args = {})  //  count ignored
    { return get_forward_stream_helper<T>(args.baseKey, subKey, args.minTime, args.maxTime, 0); }

  template<typename T> TimeValList<std::vector<T>>
  getLists(const std::string& subKey, const RA_ArgsGet& args = {})  //  count ignored
    { return get_forward_stream_list_helper<T>(args.baseKey, subKey, args.minTime, args.maxTime, 0); }

  template<typename T> TimeValList<T>
  getValuesBefore(const std::string& subKey, const RA_ArgsGet& args = {})  //  minTime ignored
    { return get_reverse_stream_helper<T>(args.baseKey, subKey, args.maxTime, args.count); }

  template<typename T> TimeValList<std::vector<T>>
  getListsBefore(const std::string& subKey, const RA_ArgsGet& args = {})  //  minTime ignored
    { return get_reverse_stream_list_helper<T>(args.baseKey, subKey, args.maxTime, args.count); }

  template<typename T> TimeValList<T>
  getValuesAfter(const std::string& subKey, const RA_ArgsGet& args = {})   //  maxTime ignored
    { return get_forward_stream_helper<T>(args.baseKey, subKey, args.minTime, 0, args.count); }

  template<typename T> TimeValList<std::vector<T>>
  getListsAfter(const std::string& subKey, const RA_ArgsGet& args = {})   //  maxTime ignored
    { return get_forward_stream_list_helper<T>(args.baseKey, subKey, args.minTime, 0, args.count); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getSingleValue  : get data as T (T is trivial, string or Attrs) at or before maxTime
  //  getSingleList   : get data as type vector<T> (T is trivial) at or before maxTime
  //
  //    baseKey : base key of device
  //    subKey  : sub key to get data from
  //    dest    : destination to copy data to
  //    maxTime : time that equals or exceeds the data to get
  //    return  : time of the data item if successful, zero on failure
  //
  template<typename T> RA_Time
  getSingleValue(const std::string& subKey, T& dest, const RA_ArgsGet& args = {})
    { return get_single_stream_helper<T>(args.baseKey, subKey, dest, args.maxTime); }

  template<typename T> RA_Time
  getSingleList(const std::string& subKey, std::vector<T>& dest, const RA_ArgsGet& args = {})
    { return get_single_stream_list_helper<T>(args.baseKey, subKey, dest, args.maxTime); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addValues : add multiple data items of type T (T is trivial, string or Attrs)
  //  addLists  : add multiple vector<T> as data items (T is trivial)
  //
  //    subKey : sub key to add data to
  //    data   : times and data to add (0 time means host time)
  //    trim   : number of items to trim the stream to
  //    return : vector of ids of successfully added data items
  //
  template<typename T> std::vector<RA_Time>
  addValues(const std::string& subKey, const TimeValList<T>& data, uint32_t trim = 1, bool approximateTrim = true);

  template<typename T> std::vector<RA_Time>
  addLists(const std::string& subKey, const TimeValList<std::vector<T>>& data, uint32_t trim = 1, bool approximateTrim = true);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addSingleValue  : add a single data item of type T (T is trivial, string or Attrs) at specified/current time
  //  addSingleDouble : add a single data item of type double at specified/current time
  //
  //    subKey : sub key to add data to
  //    data   : data to add
  //    time   : time to add the data at
  //    trim   : number of items to trim the stream to
  //    return : time of the added data item if successful, zero on failure
  //
  template<typename T> RA_Time
  addSingleValue(const std::string& subKey, const T& data, const RA_ArgsAdd& args = {});

  RA_Time addSingleDouble(const std::string& subKey, double data, const RA_ArgsAdd& args = {});

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addSingleList : add a container<T> item (T is trivial) at specified/current time
  //                  note: container must implement 'T* data()' and 'size_t size()'
  //
  //    subKey : sub key to add data to
  //    data   : pointer to buffer of type T data to add
  //    time   : time to add the data at
  //    trim   : number of items to trim the stream to
  //    return : time of the added data item if successful, zero on failure

  //  overload for array and span
  template<template<typename T, size_t S> class C, typename T, size_t S> RA_Time
  addSingleList(const std::string& subKey, const C<T, S>& data, const RA_ArgsAdd& args = {})
    { return add_single_stream_list_helper(subKey, args.time, data.data(), data.size(), args.trim,
                                           args.approximateTrim); }

  //  overload for vector
  template<typename T> RA_Time
  addSingleList(const std::string& subKey, const std::vector<T>& data, const RA_ArgsAdd& args = {})
    { return add_single_stream_list_helper(subKey, args.time, data.data(), data.size(), args.trim,
                                           args.approximateTrim); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  connected : test if server is connected and responsive
  //
  //    return : true if connected, false if not connected
  //
  bool connected() { return reconnect(_redis.ping()); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addWatchdog : add a watchdog to the set of watchdogs
  //
  //    dogname    : the name of the watchdog
  //    expiration : the number of seconds to expire the watchdog
  //    return     : true if successful, false if not successful
  //
  bool addWatchdog(const std::string& dogname, uint32_t expiration)
    { return _redis.hset(_watchdog_key, dogname, RA_VERSION) && petWatchdog(dogname, expiration); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  petWatchdog : refresh the expiration of a watchdog
  //
  //    dogname    : the name of the watchdog
  //    expiration : the number of seconds to expire the watchdog
  //    return     : true if successful, false if not successful
  //
  bool petWatchdog(const std::string& dogname, uint32_t expiration)
    { return reconnect(_redis.hexpire(_watchdog_key, dogname, expiration) != -1); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getWatchdogs : get the list of watchdogs
  //
  //    return : list of watchdogs
  //
  std::vector<std::string> getWatchdogs() { return _redis.hkeys(_watchdog_key); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  copy : copy any RA stream key to a home stream key (dest key must not exist)
  //
  //    baseKey   : base key of source
  //    srcSubKey : sub key of source
  //    dstSubKey : sub key of destination
  //    return    : true if successful, false if unsuccessful
  //
  bool copy(const std::string& srcSubKey, const std::string& dstSubKey, const std::string& baseKey = "");

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  rename : rename a home stream key (dest key must not exist)
  //
  //    srcSubKey : sub key of source
  //    dstSubKey : sub key of destination
  //    return    : true if successful, false if unsuccessful
  //
  bool rename(const std::string& subKeySrc, const std::string& subKeyDst)
    { return reconnect(_redis.rename(build_key(subKeySrc), build_key(subKeyDst))); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  del : delete a home stream key
  //
  //    subKey : sub key to delete
  //    return : true if successful, false if unsuccessful
  //
  bool del(const std::string& subKey) { return reconnect(_redis.del(build_key(subKey)) >= 0); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  exists : check if a home stream key exists
  //
  //    subKey : sub key to check
  //    return : true if the key exists, false otherwise
  //
  bool exists(const std::string& subKey) { auto ret = _redis.exists(build_key(subKey)); reconnect(ret != -1); return ret > 0; }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  ListenSubFn : callback function type for pub/sub notification
  //
  using ListenSubFn = std::function<void(const std::string& baseKey, const std::string& subKey, const std::string& message)>;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  publish : publish a message to a channel made up of base key and sub key
  //
  //    baseKey : the base key to construct the channel from
  //    subKey  : the sub key to construct the channel from
  //    message : the message to send
  //    return  : true on success, false on failure
  //
  bool publish(const std::string& subKey, const std::string& message, const std::string& baseKey = "")
    { return reconnect(_redis.publish(build_key(subKey, baseKey), message) != -1); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  subscribe   : subscribe for messages on a single channel
  //  psubscribe  : pattern subscribe for messages on a set of channels matching a pattern
  //  unsubscribe : unsubscribe a single channel or pattern
  //
  //    baseKey : the base key to construct the channel from
  //    subKey  : the sub key to construct the channel from
  //    func    : the function to call when message received on this channel
  //    return  : true on success, false on failure
  //
  bool subscribe(const std::string& subKey, ListenSubFn func, const std::string& baseKey = "");

  bool psubscribe(const std::string& subKey, ListenSubFn func, const std::string& baseKey = "");

  bool unsubscribe(const std::string& subKey, const std::string& baseKey = "");

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  setDeferReaders : defer or un-defer addition and removal of readers
  //                    - deferring cancels all reads and stops all reader threads until un-defer
  //                    - un-deferring starts all reader threads
  //                    this prevents redundant thread destruction/creation and is the
  //                    preferred way to add/remove multiple readers at one time
  //
  //    defer   : whether to defer or un-defer addition and removal of readers
  //    return  : true on success, false on failure
  //
  bool setDeferReaders(bool defer);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  ReaderSubFn : callback function type for stream reader notification
  //
  template<typename T>
  using ReaderSubFn = std::function<void(const std::string& baseKey, const std::string& subKey, const TimeValList<T>& data)>;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addValuesReader  : add a stream reader for a data key (trivial type, string or Attr)
  //  addListsReader   : add a stream reader for a data key (vector of trivial type)
  //
  //    baseKey : the base key to read from
  //    subKey  : the sub key to read from
  //    func    : the function to call when information is read on a key
  //    return  : true on success, false on failure
  //
  template<typename T>
  bool addValuesReader(const std::string& subKey, ReaderSubFn<T> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, subKey, make_reader_callback(func)); }

  template<typename T>
  bool addListsReader(const std::string& subKey, ReaderSubFn<std::vector<T>> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, subKey, make_list_reader_callback(func)); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addGenericReader : add a reader for a key that does NOT follow RedisAdapter schema
  //
  //    key     : the key to add (must NOT be a RedisAdapter schema key)
  //    func    : function to call when data is read - data will be Attrs
  //    return  : true if reader started, false if reader failed to start
  //
  bool addGenericReader(const std::string& key, ReaderSubFn<Attrs> func);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  removeReader : remove all readers for a stream key
  //
  //    baseKey : the base key to remove
  //    subKey  : the sub key to remove
  //    return  : true on success, false on failure
  //
  bool removeReader(const std::string& subKey, const std::string& baseKey = "")
    { return remove_reader_helper(baseKey, subKey); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  removeGenericReader : remove all readers for key that does NOT follow RedisAdapter schema
  //
  //    key    : the key to remove (must NOT be a RedisAdapter schema key)
  //    return : true if reader started, false if reader failed to start
  //
  bool removeGenericReader(const std::string& key);

private:
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Containers for stream data suggested by the redis++ readme.md
  //    https://github.com/sewenew/redis-plus-plus#redis-stream
  //
  using Item = StreamEntry;
  using ItemStream = StreamBatch;
  using Streams = std::unordered_map<std::string, ItemStream>;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Redis key and field constants
  //

  std::string build_key(const std::string& subKey, const std::string& baseKey = "") const;

  std::pair<std::string, std::string> split_key(const std::string& key) const;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions adding and removing stream readers
  //
  using reader_sub_fn = std::function<void(const std::string& baseKey, const std::string& subKey, const ItemStream& data)>;

  uint32_t reader_token(const std::string& key);

  bool add_reader_helper(const std::string& baseKey, const std::string& subKey, reader_sub_fn func);
  std::shared_ptr<ReaderRegistration> register_reader(const std::string& key,
                                                     reader_sub_fn func,
                                                     const std::string& afterId,
                                                     bool resolveTail = true, uint32_t probeMs = UINT32_MAX,
                                                     EpochStreamCallback epochCallback = {});
  void remove_registration(uint64_t id);

  template<typename T> reader_sub_fn make_reader_callback(ReaderSubFn<T> func) const;

  template<typename T> reader_sub_fn make_list_reader_callback(ReaderSubFn<std::vector<T>> func) const;

  bool remove_reader_helper(const std::string& baseKey, const std::string& subKey);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getting and setting DEFAULT_FIELD in Attrs
  //
  template<typename T> static auto default_field_value(const Attrs& attrs);

  template<typename T> Attrs default_field_attrs(const T* data, size_t size) const;

  template<typename T> Attrs default_field_attrs(const T& data) const;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getting and adding data
  //
  template<typename T> TimeValList<T>
  get_forward_stream_helper(const std::string& baseKey, const std::string& subKey, RA_Time minTime, RA_Time maxTime, uint32_t count);

  template<typename T> TimeValList<std::vector<T>>
  get_forward_stream_list_helper(const std::string& baseKey, const std::string& subKey, RA_Time minTime, RA_Time maxTime, uint32_t count);

  template<typename T> TimeValList<T>
  get_reverse_stream_helper(const std::string& baseKey, const std::string& subKey, RA_Time maxTime, uint32_t count);

  template<typename T> TimeValList<std::vector<T>>
  get_reverse_stream_list_helper(const std::string& baseKey, const std::string& subKey, RA_Time maxTme, uint32_t count);

  template<typename T> RA_Time
  get_single_stream_helper(const std::string& baseKey, const std::string& subKey, T& dest, RA_Time maxTime);

  template<typename T> RA_Time
  get_single_stream_list_helper(const std::string& baseKey, const std::string& subKey, std::vector<T>& dest, RA_Time maxTime);

  template<typename T> RA_Time
  add_single_stream_list_helper(const std::string& subKey, RA_Time time, const T* data, size_t size,
                                uint32_t trim, bool approximateTrim);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Redis server
  //
  RA_Options _options;
  RedisConnection _redis;
  std::string _base_key;

  int32_t reconnect(int32_t result);
  RA_Time finishWrite(const RedisConnection::WriteResult& result);
  void finishBatch(const std::string& key, size_t accepted, uint32_t trim, bool refreshNeeded, bool approximateTrim);
  std::atomic_bool _connecting;
  std::thread _reconnect_thd;
  std::mutex _reconnect_mtx;
  std::atomic<bool> _shutdown{false};

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Watchdog
  //
  std::string _watchdog_key;
  std::thread _watchdog_thd;
  std::condition_variable _watchdog_cv;
  std::atomic<bool> _watchdog_run;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Stream readers
  //
  size_t resolve_reader_tails(reader_info& info);
  bool start_reader(uint32_t token);
  bool stop_reader(uint32_t token);

  std::atomic<bool> _readers_defer;

  std::mutex _reader_mtx;

  struct ReaderOwner {
    std::mutex mutex;
    RedisAdapter* adapter = nullptr;
  };
  struct ReaderRegistration {
    uint64_t id = 0;
    std::atomic<bool> active{true};
    std::mutex mutex;
    std::string cursor;
    reader_sub_fn callback;
    EpochStreamCallback epochCallback;
    uint32_t probeMs = 0;
    bool everConnected = false, continuityCheck = true;
    std::string observedCursor, gapSignature;
    ReaderStatus status;
  };
  std::shared_ptr<ReaderOwner> _reader_owner = std::make_shared<ReaderOwner>();
  uint64_t _next_reader_id = 0;

  struct reader_info
  {
    std::thread thread;
    std::unordered_map<std::string, std::vector<std::shared_ptr<ReaderRegistration>>> subs;
    std::unordered_map<std::string, std::string> keyids;
    std::string stop;
    struct Boundary {
      std::chrono::steady_clock::time_point nextProbe{};
      uint32_t intervalMs = 0, backoff = 1;
      uint64_t readVersion = 0, lastProbeVersion = 0;
      bool denied = false;
    };
    struct Quarantine { std::chrono::steady_clock::time_point nextCheck{}; bool wrongType = false; };
    struct ProbeTicket { std::chrono::steady_clock::time_point due; uint64_t order; std::string key; };
    struct ProbeLater {
      bool operator()(const ProbeTicket& a, const ProbeTicket& b) const { return std::tie(a.due, a.order) > std::tie(b.due, b.order); }
    };
    std::unordered_map<std::string, Boundary> boundaries;
    std::unordered_map<std::string, Quarantine> quarantined;
    std::priority_queue<ProbeTicket, std::vector<ProbeTicket>, ProbeLater> probes;
    uint64_t probeOrder = 0;
    bool readConnected = false, controlReadable = true;
    std::atomic<bool> run = false;

    //  used by start_reader() to confirm the reader thread has begun its read loop -
    //  these live here (in the _reader map) rather than as locals in start_reader()
    //  so their lifetime safely covers the reader thread's lifetime, not just one call
    std::mutex start_mx;
    std::condition_variable start_cv;
    bool started = false;
  };
  std::unordered_map<uint32_t, reader_info> _reader;

  void prepare_probes(reader_info& info);
  void inspect_readers(reader_info& info);
  void reader_result(reader_info& info, RedisConnection::ReadStatus result, bool socketTimedOut = false);
  void check_readable(reader_info& info, const std::vector<std::string>& keys);
  void reset_stream(reader_info& info, const std::string& key, StreamKind kind, bool allReaders);
  void apply_bounds(reader_info& info, const std::string& key, const RedisConnection::StreamBounds& bounds);
  uint32_t read_interval(const reader_info& info, bool& blocking) const;
  ThreadPool _replier_pool;
};

#include "RedisAdapterTempl.hpp"


#endif // defined(MOCK_REDIS_ADAPTER)
