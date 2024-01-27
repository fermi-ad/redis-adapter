//
//  RedisAdapter.hpp
//
//  This file contains the RedisAdapter class definition

#pragma once

#include "RedisConnection.hpp"
#include <thread>

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  struct RA_StatusArgs
//  struct RA_GetLogArgs, struct RA_AddLogArgs
//  struct RA_GetStreamArgs, struct RA_AddStreamArgs
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
//    redis.getStream<string>("abc", { .minTime=1000, .maxTime=2000 });
//
struct RA_StatusArgs
{ std::string baseKey; std::string subKey; };

struct RA_GetLogArgs
{ std::string subKey; uint64_t minTime = 0; uint64_t maxTime = 0; uint32_t count = 100; };

struct RA_AddLogArgs
{ std::string subKey; uint32_t trim = 1000; };

struct RA_GetStreamArgs
{ std::string baseKey; uint64_t minTime = 0; uint64_t maxTime = 0; uint32_t count = 1; };

struct RA_AddStreamArgs
{ uint64_t time = 0; uint32_t trim = 1; };

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
  template<typename T> using TimeVal = std::pair<uint64_t, T>;        //  analagous to Item
  template<typename T> using TimeValList = std::vector<TimeVal<T>>;   //  analagous to ItemStream

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Construction / Destruction
  //
  RedisAdapter(const std::string& baseKey, const RedisConnection::Options& options, uint32_t timeout = 500);
  RedisAdapter(const std::string& baseKey, const std::string& host = "", uint16_t port = 0, uint32_t timeout = 500);

  RedisAdapter(const RedisAdapter& ra) = delete;       //  copy construction not allowed
  RedisAdapter& operator=(const RedisAdapter& ra) = delete;   //  assignment not allowed

  virtual ~RedisAdapter();

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Status

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getStatus : get status as string
  //
  //    baseKey : base key to get status from
  //    subKey  : sub key to get status from
  //    return  : string with status on success, empty string on failure
  //
  std::string getStatus(const RA_StatusArgs& args = {});

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  setStatus : set status for home device as string
  //
  //    value  : status value to set
  //    subKey : sub key to set status on
  //    return : true on success, false on failure
  //
  bool setStatus(const std::string& value, const std::string& subKey = "");

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Log

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getLog        : get log for home device between specified times (minTime, maxTime)
  //  getLogAfter   : get log for home device after specified time (minTime)
  //  getLogBefore  : get log for home device before specified time (maxTime)
  //
  //    subKey  : sub key to get log from
  //    minTime : lowest time to get log for
  //    maxTime : highest time to get log for
  //    count   : greatest number of log items to get
  //    return  : TimeValList of TimeVal<string> log items
  //
  TimeValList<std::string> getLog(const RA_GetLogArgs& args = {});       //  count ignored
  TimeValList<std::string> getLogAfter(const RA_GetLogArgs& args = {});  //  maxTime ignored
  TimeValList<std::string> getLogBefore(const RA_GetLogArgs& args = {}); //  minTime ignored

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addLog : add a log message for home device
//
//    message : log message to add
//    subKey  : sub key to add log to
//    trim    : number of items to trim log stream to
//    return  : true for success, false for failure
//
  bool addLog(const std::string& message, const RA_AddLogArgs& args = {});

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Stream (get)

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getStream           : get data as T (T is trivial, string or Attrs) between minTime and maxTime
  //  getStreamList       : get data as type vector<T> (T is trivial) between minTime and maxTime
  //  getStreamBefore     : get data as T (T is trivial, string or Attrs) before maxTime
  //  getStreamListBefore : get data as type vector<T> (T is trivial) before maxTime
  //  getStreamAfter      : get data as T (T is trivial, string or Attrs) after minTime
  //  getStreamListAfter  : get data as type vector<T> (T is trivial) after minTime
  //
  //    baseKey : base key of device
  //    subKey  : sub key to get data from
  //    minTime : lowest time to get data for
  //    maxTime : highest time to get data for
  //    count   : max number of items to get
  //    return  : TimeValList of TimeVal<T>
  //
  template<typename T> TimeValList<T>
  getStream(const std::string& subKey, const RA_GetStreamArgs& args = {})  //  count ignored
    { return get_forward_stream_helper<T>(args.baseKey, subKey, args.minTime, args.maxTime, 0); }

  template<typename T> TimeValList<std::vector<T>>
  getStreamList(const std::string& subKey, const RA_GetStreamArgs& args = {})  //  count ignored
    { return get_forward_stream_list_helper<T>(args.baseKey, subKey, args.minTime, args.maxTime, 0); }

  template<typename T> TimeValList<T>
  getStreamBefore(const std::string& subKey, const RA_GetStreamArgs& args = {})  //  minTime ignored
    { return get_reverse_stream_helper<T>(args.baseKey, subKey, args.maxTime, args.count); }

  template<typename T> TimeValList<std::vector<T>>
  getStreamListBefore(const std::string& subKey, const RA_GetStreamArgs& args = {})  //  minTime ignored
    { return get_reverse_stream_list_helper<T>(args.baseKey, subKey, args.maxTime, args.count); }

  template<typename T> TimeValList<T>
  getStreamAfter(const std::string& subKey, const RA_GetStreamArgs& args = {})   //  maxTime ignored
    { return get_forward_stream_helper<T>(args.baseKey, subKey, args.minTime, 0, args.count); }

  template<typename T> TimeValList<std::vector<T>>
  getStreamListAfter(const std::string& subKey, const RA_GetStreamArgs& args = {})   //  maxTime ignored
    { return get_forward_stream_list_helper<T>(args.baseKey, subKey, args.minTime, 0, args.count); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getStreamSingle     : get data as T (T is trivial, string or Attrs) at or before maxTime
  //  getStreamListSingle : get data as type vector<T> (T is trivial) at or before maxTime
  //
  //    baseKey : base key of device
  //    subKey  : sub key to get data from
  //    dest    : destination to copy data to
  //    maxTime : time that equals or exceeds the data to get
  //    return  : time of the data item if successful, zero on failure
  //
  template<typename T> uint64_t
  getStreamSingle(const std::string& subKey, T& dest, const RA_GetStreamArgs& args = {})
    { return get_single_stream_helper<T>(args.baseKey, subKey, dest, args.maxTime); }

  template<typename T> uint64_t
  getStreamListSingle(const std::string& subKey, std::vector<T>& dest, const RA_GetStreamArgs& args = {})
    { return get_single_stream_list_helper<T>(args.baseKey, subKey, dest, args.maxTime); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Stream (add)

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addStream     : add multiple data items of type T (T is trivial, string or Attrs)
  //  addStreamList : add multiple vector<T> as data items (T is trivial)
  //
  //    subKey : sub key to add data to
  //    data   : times and data to add (0 time means host time)
  //    trim   : number of items to trim the stream to
  //    return : vector of ids of successfully added data items
  //
  template<typename T> std::vector<uint64_t>
  addStream(const std::string& subKey, const TimeValList<T>& data, uint32_t trim = 1);

  template<typename T> std::vector<uint64_t>
  addStreamList(const std::string& subKey, const TimeValList<std::vector<T>>& data, uint32_t trim = 1);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addStreamSingle       : add a single data item of type T (T is trivial, string or Attrs) at specified/current time
  //  addStreamSingleDouble : add a single data item of type double at specified/current time
  //
  //    subKey : sub key to add data to
  //    data   : data to add
  //    time   : time to add the data at
  //    trim   : number of items to trim the stream to
  //    return : time of the added data item if successful, zero on failure
  //
  template<typename T> uint64_t
  addStreamSingle(const std::string& subKey, const T& data, const RA_AddStreamArgs& args = {});

  uint64_t addStreamSingleDouble(const std::string& subKey, double data, const RA_AddStreamArgs& args = {});

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addStreamSingleList : add a container<T> item (T is trivial) at specified/current time
  //                        note: container must implement 'T* data()' and 'size_t size()'
  //
  //    subKey : sub key to add data to
  //    data   : pointer to buffer of type T data to add
  //    time   : time to add the data at
  //    trim   : number of items to trim the stream to
  //    return : time of the added data item if successful, zero on failure

  //  overload for array and span
  template<template<typename T, size_t S> class C, typename T, size_t S> uint64_t
  addStreamSingleList(const std::string& subKey, const C<T, S>& data, const RA_AddStreamArgs& args = {})
    { return add_single_stream_list_helper(subKey, args.time, data.data(), data.size(), args.trim); }

  //  overload for vector
  template<typename T> uint64_t
  addStreamSingleList(const std::string& subKey, const std::vector<T>& data, const RA_AddStreamArgs& args = {})
    { return add_single_stream_list_helper(subKey, args.time, data.data(), data.size(), args.trim); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Utility

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  connected : test if server is connected and responsive
  //
  //    return : true if connected, false if not connected
  //
  bool connected() { return _redis->ping(); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  copyStream : copy any RA stream key to a home stream key (dest key must not exist)
  //
  //    baseKey   : base key of source
  //    srcSubKey : sub key of source
  //    dstSubKey : sub key of destination
  //    return    : true if successful, false if unsuccessful
  //
  bool copyStream(const std::string& srcSubKey, const std::string& dstSubKey, const std::string& baseKey = "");

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  renameStream : rename a home stream key (dest key must not exist)
  //
  //    srcSubKey : sub key of source
  //    dstSubKey : sub key of destination
  //    return    : true if successful, false if unsuccessful
  //
  bool renameStream(const std::string& subKeySrc, const std::string& subKeyDst)
    { return _redis->rename(build_key(STREAM_STUB, subKeySrc), build_key(STREAM_STUB, subKeyDst)); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  deleteStream : delete a home stream key
  //
  //    subKey : sub key to delete
  //    return : true if successful, false if unsuccessful
  //
  bool deleteStream(const std::string& subKey) { return _redis->del(build_key(STREAM_STUB, subKey)) >= 0; }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getServerTime : get time since epoch in nanoseconds from server
  //
  //    return : nanoseconds if successful, zero if unsuccessful
  //
  uint64_t getServerTime();

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Publish/Subscribe

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
    { return _redis->publish(build_key(CHANNEL_STUB, subKey, baseKey), message) >= 0; }

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
  //  Stream Readers

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  ReaderSubFn : callback function type for stream reader notification
  //
  template<typename T>
  using ReaderSubFn = std::function<void(const std::string& baseKey, const std::string& subKey, const TimeValList<T>& data)>;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addStatusReader     : add a stream reader for a status key (type string)
  //  addLogReader        : add a stream reader for a log key (type string)
  //  addStreamReader     : add a stream reader for a data key (trivial type, string or Attr)
  //  addStreamListReader : add a stream reader for a data key (vector of trivial type)
  //
  //    baseKey : the base key to read from
  //    subKey  : the sub key to read from
  //    func    : the function to call when information is read on a key
  //    return  : true on success, false on failure
  //
  bool addStatusReader(ReaderSubFn<std::string> func, const RA_StatusArgs& args = {})
    { return add_reader_helper(args.baseKey, STATUS_STUB, args.subKey, make_reader_callback(func)); }

  bool addLogReader(ReaderSubFn<std::string> func, const std::string& subKey = "")
    { return add_reader_helper("", LOG_STUB, subKey, make_reader_callback(func)); }

  template<typename T>
  bool addStreamReader(const std::string& subKey, ReaderSubFn<T> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, STREAM_STUB, subKey, make_reader_callback(func)); }

  template<typename T>
  bool addStreamListReader(const std::string& subKey, ReaderSubFn<std::vector<T>> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, STREAM_STUB, subKey, make_list_reader_callback(func)); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addGenericReader : add a reader for a key that does NOT follow RedisAdapter schema
  //
  //    key     : the key to add (must NOT be a RedisAdapter schema key)
  //    func    : function to call when data is read - data will be Attrs
  //    return  : true if reader started, false if reader failed to start
  //
  bool addGenericReader(const std::string& key, ReaderSubFn<Attrs> func);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  removeStatusReader : remove all readers for a status key
  //  removeLogReader    : remove all readers for the log key
  //  removeStreamReader : remove all readers for a stream key
  //
  //    baseKey : the base key to remove
  //    subKey  : the sub key to remove
  //    return  : true on success, false on failure
  //
  bool removeStatusReader(const RA_StatusArgs& args = {})
    { return remove_reader_helper(args.baseKey, STATUS_STUB, args.subKey); }

  bool removeLogReader(const std::string& subKey = "")
    { return remove_reader_helper("", LOG_STUB, subKey); }

  bool removeStreamReader(const std::string& subKey, const std::string& baseKey = "")
    { return remove_reader_helper(baseKey, STREAM_STUB, subKey); }

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
  const std::string DEFAULT_FIELD = "_";

  const std::string LOG_STUB      = "[*-LOG-*]";
  const std::string STATUS_STUB   = "[*-STATUS-*]";
  const std::string STREAM_STUB   = "[*-STREAM-*]";
  const std::string CHANNEL_STUB  = "<$-CHANNEL-$>";

  const std::string STOP_STUB = "[*-STOP-*]";

  std::string build_key(const std::string& keyStub, const std::string& subKey = "", const std::string& baseKey = "") const;

  std::pair<std::string, std::string> split_key(const std::string& key) const;

  uint64_t id_to_time(const std::string& id) const { try { return std::stoull(id); } catch(...) {} return 0; }

  std::string time_to_id(uint64_t time = 0) const { return std::to_string(time ? time : get_host_time()) + "-0"; }

  std::string min_time_to_id(uint64_t time) const { return time ? time_to_id(time) : "-"; }

  std::string max_time_to_id(uint64_t time) const { return time ? time_to_id(time) : "+"; }

  uint64_t get_host_time() const;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions adding and removing stream readers
  //
  using reader_sub_fn = std::function<void(const std::string& baseKey, const std::string& subKey, const ItemStream& data)>;

  bool add_reader_helper(const std::string& baseKey, const std::string& keyStub, const std::string& subKey, reader_sub_fn func);

  template<typename T> reader_sub_fn make_reader_callback(ReaderSubFn<T> func) const;

  template<typename T> reader_sub_fn make_list_reader_callback(ReaderSubFn<std::vector<T>> func) const;

  bool remove_reader_helper(const std::string& baseKey, const std::string& keyStub, const std::string& subKey);

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
  get_forward_stream_helper(const std::string& baseKey, const std::string& subKey, uint64_t minTime, uint64_t maxTime, uint32_t count);

  template<typename T> TimeValList<std::vector<T>>
  get_forward_stream_list_helper(const std::string& baseKey, const std::string& subKey, uint64_t minTime, uint64_t maxTime, uint32_t count);

  template<typename T> TimeValList<T>
  get_reverse_stream_helper(const std::string& baseKey, const std::string& subKey, uint64_t maxTime, uint32_t count);

  template<typename T> TimeValList<std::vector<T>>
  get_reverse_stream_list_helper(const std::string& baseKey, const std::string& subKey, uint64_t maxTme, uint32_t count);

  template<typename T> uint64_t
  get_single_stream_helper(const std::string& baseKey, const std::string& subKey, T& dest, uint64_t maxTime);

  template<typename T> uint64_t
  get_single_stream_list_helper(const std::string& baseKey, const std::string& subKey, std::vector<T>& dest, uint64_t maxTime);

  template<typename T> uint64_t
  add_single_stream_list_helper(const std::string& subKey, uint64_t time, const T* data, size_t size, uint32_t trim);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Redis stuff
  //
  std::unique_ptr<RedisConnection> _redis;

  std::string _base_key;

  uint32_t _timeout;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Pub/Sub Listener and Stream Reader
  //
  bool start_listener();
  std::thread _listener;
  bool _listener_run;
  bool stop_listener();

  std::unordered_map<std::string, std::vector<ListenSubFn>> _pattern_subs;
  std::unordered_map<std::string, std::vector<ListenSubFn>> _command_subs;

  bool start_reader(uint16_t slot);
  bool stop_reader(uint16_t slot);

  struct reader_info
  {
    std::thread thread;
    std::unordered_map<std::string, std::vector<reader_sub_fn>> subs;
    std::unordered_map<std::string, std::string> keyids;
    std::string stop;
    bool run = false;
  };
  std::unordered_map<uint16_t, reader_info> _reader;
};

#include "RedisAdapterTempl.hpp"
