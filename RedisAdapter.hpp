//
//  RedisAdapter.hpp
//
//  This file contains the RedisAdapter class definition

#pragma once

#include "RedisConnection.hpp"
#include "ThreadPool.hpp"
#include <thread>
#include <atomic>

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  struct RA_Time
//
//  Nanosecond time since epoch, provided as a result timestamp
//  for 'get' methods, and specified as a new time for 'add' methods
//
//  An RA_Time with value = 0 is illegal (uninitialized)
//  An RA_Time with value < 0 is illegal (error code)
//
struct RA_Time
{
  RA_Time(int64_t nanos = 0) : value(nanos) {}
  RA_Time(const std::string& id);

  bool ok() const { return value > 0; }

  operator int64_t()  const { return ok() ? value : 0; }
  operator uint64_t() const { return ok() ? value : 0; }

  uint32_t err() const { return ok() ? 0 : -value; }

  std::string id() const;
  std::string id_or_now() const;

  std::string id_or_min() const { return ok() ? id() : "-"; }
  std::string id_or_max() const { return ok() ? id() : "+"; }

  int64_t value;
};

const RA_Time RA_NOT_CONNECTED(-1);

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
{ RA_Time time; uint32_t trim = 1; };

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  struct RA_Options
//
struct RA_Options : public RedisConnection::Options
{
  std::string dogname;
  uint16_t workers = 1;
};

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  class RedisAdapter
//
//  Provides a framework for AD Instrumentation front-ends and back-ends to exchange
//  data, settings, status and control information via a Redis server or cluster
//
class RedisAdapter
{
public:
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Containers for stream data suggested by the redis++ readme.md
  //    https://github.com/sewenew/redis-plus-plus#redis-stream
  //
  using Attrs = std::unordered_map<std::string, std::string>;

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
  addValues(const std::string& subKey, const TimeValList<T>& data, uint32_t trim = 1);

  template<typename T> std::vector<RA_Time>
  addLists(const std::string& subKey, const TimeValList<std::vector<T>>& data, uint32_t trim = 1);

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
    { return add_single_stream_list_helper(subKey, args.time, data.data(), data.size(), args.trim); }

  //  overload for vector
  template<typename T> RA_Time
  addSingleList(const std::string& subKey, const std::vector<T>& data, const RA_ArgsAdd& args = {})
    { return add_single_stream_list_helper(subKey, args.time, data.data(), data.size(), args.trim); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  connected : test if server is connected and responsive
  //
  //    return : true if connected, false if not connected
  //
  bool connected() { return connect(_redis.ping()); }

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
    { return connect(_redis.rename(build_key(subKeySrc), build_key(subKeyDst))); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  del : delete a home stream key
  //
  //    subKey : sub key to delete
  //    return : true if successful, false if unsuccessful
  //
  bool del(const std::string& subKey) { return connect(_redis.del(build_key(subKey)) >= 0); }

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
    { return connect(_redis.publish(build_key(subKey, baseKey), message) >= 0); }

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
  using Item = std::pair<std::string, Attrs>;
  using ItemStream = std::vector<Item>;
  using Streams = std::unordered_map<std::string, ItemStream>;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Redis key and field constants
  //
  const std::string DEFAULT_FIELD = "_";            //  default field in stream Attrs
  const std::string STOP_STUB     = "<$-STOP-$>";   //  stream stub to stop reader thread

  std::string build_key(const std::string& subKey, const std::string& baseKey = "") const;

  std::pair<std::string, std::string> split_key(const std::string& key) const;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions adding and removing stream readers
  //
  using reader_sub_fn = std::function<void(const std::string& baseKey, const std::string& subKey, const ItemStream& data)>;

  bool add_reader_helper(const std::string& baseKey, const std::string& subKey, reader_sub_fn func);

  template<typename T> reader_sub_fn make_reader_callback(ReaderSubFn<T> func) const;

  template<typename T> reader_sub_fn make_list_reader_callback(ReaderSubFn<std::vector<T>> func) const;

  bool remove_reader_helper(const std::string& baseKey, const std::string& subKey);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getting and setting DEFAULT_FIELD in Attrs
  //
  template<typename T> auto default_field_value(const Attrs& attrs) const;

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
  add_single_stream_list_helper(const std::string& subKey, RA_Time time, const T* data, size_t size, uint32_t trim);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Redis stuff
  //
  RA_Options _options;
  RedisConnection _redis;
  std::string _base_key;

  int32_t connect(int32_t result);
  std::atomic_bool _connecting;

  std::thread _watchdog;
  bool _watchdog_run;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Pub/Sub Listener and Stream Reader
  //
  bool start_listener();
  bool stop_listener();

  std::thread _listener;
  bool _listener_run;

  std::unordered_map<std::string, std::vector<ListenSubFn>> _pattern_subs;
  std::unordered_map<std::string, std::vector<ListenSubFn>> _command_subs;

  bool start_reader(uint16_t slot);
  bool stop_reader(uint16_t slot);

  bool _readers_defer;

  struct reader_info
  {
    std::thread thread;
    std::unordered_map<std::string, std::vector<reader_sub_fn>> subs;
    std::unordered_map<std::string, std::string> keyids;
    std::string stop;
    bool run = false;
  };
  std::unordered_map<uint16_t, reader_info> _reader;

  ThreadPool _workers;
};

#include "RedisAdapterTempl.hpp"
