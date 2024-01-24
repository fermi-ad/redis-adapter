//
//  RedisAdapter.hpp
//
//  This file contains the RedisAdapter class definition

#pragma once

#include "RedisConnection.hpp"
#include <thread>

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  class RedisAdapter
//
//  Provides a framework for AD Instrumentation front-ends and back-ends to exchange
//  data, settings, status and control information via a Redis server or cluster
//
class RedisAdapter
{
public:
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Containers for stream data suggested by the redis++ readme.md
  //    https://github.com/sewenew/redis-plus-plus#redis-stream
  //
  using Attrs = std::unordered_map<std::string, std::string>;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Containers for getting/setting data using RedisAdapter methods
  //
  template<typename T> using TimeVal = std::pair<uint64_t, T>;        //  analagous to Item
  template<typename T> using TimeValList = std::vector<TimeVal<T>>;   //  analagous to ItemStream

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Construction / Destruction
  //
  RedisAdapter(const std::string& baseKey, const RedisConnection::Options& options, uint32_t timeout = 500);
  RedisAdapter(const std::string& baseKey, const std::string& host = "", uint16_t port = 0, uint32_t timeout = 500);

  RedisAdapter(const RedisAdapter& ra) = delete;       //  copy construction not allowed
  RedisAdapter& operator=(const RedisAdapter& ra) = delete;   //  assignment not allowed

  virtual ~RedisAdapter();

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Status

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getStatus : get status as string
  //
  //    subKey  : sub key to get status from
  //    baseKey : base key to get status from
  //    return  : string with status on success, empty string on failure
  //
  std::string getStatus(const std::string& subKey, const std::string& baseKey = "");

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  setStatus : set status for home device as string
  //
  //    subKey : sub key to set status on
  //    value  : status value to set
  //    return : true on success, false on failure
  //
  bool setStatus(const std::string& subKey, const std::string& value);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Log

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getLog        : get log for home device between specified times
  //  getLogAfter   : get log for home device after specified time
  //  getLogBefore  : get log for home device before specified time
  //  getLogCount   : get latest count of log for home device
  //
  //    minID  : lowest time to get log for
  //    maxID  : highest time to get log for
  //    count  : greatest number of log items to get
  //    return : TimeValList of TimeVal<string> log items
  //
  TimeValList<std::string> getLog(uint64_t minTime, uint64_t maxTime = 0);
  TimeValList<std::string> getLogAfter(uint64_t minTime = 0, uint32_t count = 100);
  TimeValList<std::string> getLogBefore(uint64_t maxTime = 0, uint32_t count = 100);
  TimeValList<std::string> getLogCount(uint32_t count) { return getLogBefore(0, count); }

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addLog : add a log message for home device
//
//    message : log message to add
//    trim    : number of items to trim log stream to
//    return  : true for success, false for failure
//
  bool addLog(const std::string& message, uint32_t trim = 1000);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Settings

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getSetting      : get setting as type T (T is trivial, string)
  //  getSettingList  : get setting as type vector<T> (T is trivial)
  //
  //    subKey  : sub key to get setting from
  //    baseKey : base key to get setting from
  //    return  : string or optional with setting value if successful
  //              empty string or optional if unsuccessful
  //
  template<typename T> auto getSetting(const std::string& subKey, const std::string& baseKey = "");
  template<typename T> std::vector<T> getSettingList(const std::string& subKey, const std::string& baseKey = "");

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  setSetting        : set setting as type T (T is trivial, string)
  //  setSettingList    : set setting as vector<T> (T is trivial)
  //  setSettingDouble  : set setting as type double
  //
  //    subKey : sub key to set setting on
  //    value  : setting value to set
  //    return : true on success, false on failure
  //
  template<typename T> bool setSetting(const std::string& subKey, const T& value);
  template<typename T> bool setSettingList(const std::string& subKey, const std::vector<T>& value);
  bool setSettingDouble(const std::string& subKey, double value);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Data (get)

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getData     : get data as T (T is trivial, string or Attrs) between minTime and maxTime
  //  getDataList : get data as type vector<T> (T is trivial) between minTime and maxTime
  //  getDataBefore     : get data as T (T is trivial, string or Attrs) before maxTime
  //  getDataListBefore : get data as type vector<T> (T is trivial) before maxTime
  //  getDataAfter      : get data as T (T is trivial, string or Attrs) after minTime
  //  getDataListAfter  : get data as type vector<T> (T is trivial) after minTime
  //
  //    baseKey : base key of device
  //    subKey  : sub key to get data from
  //    minTime : lowest time to get data for
  //    maxTime : highest time to get data for
  //    count   : max number of items to get
  //    return  : TimeValList of TimeVal<T>
  //
  template<typename T> TimeValList<T>
  getData(const std::string& subKey, const uint64_t minTime, uint64_t maxTime, const std::string& baseKey = "")
    { return get_forward_data_helper<T>(baseKey, subKey, minTime, maxTime, 0); }

  template<typename T> TimeValList<std::vector<T>>
  getDataList(const std::string& subKey, uint64_t minTime, uint64_t maxTime, const std::string& baseKey = "")
    { return get_forward_data_list_helper<T>(baseKey, subKey, minTime, maxTime, 0); }

  struct GetDataArgs        //  GetDataArgs : structure for providing arguments to
  {                         //                getData functions - each field can be
    std::string baseKey;    //                overidden or left as the default value
    uint64_t minTime = 0;   //  Suggested usage:
    uint64_t maxTime = 0;   //    using GDA = RedisAdapter::GetDataArgs;
    uint32_t count = 1;     //    redis.getDataAfter("my:subkey",
  };                        //                       GDA{ .minTime=1000, .count=10 });

  template<typename T> TimeValList<T>
  getDataBefore(const std::string& subKey, const GetDataArgs& args = {})
    { return get_reverse_data_helper<T>(args.baseKey, subKey, args.maxTime, args.count); }

  template<typename T> TimeValList<std::vector<T>>
  getDataListBefore(const std::string& subKey, const GetDataArgs& args = {})
    { return get_reverse_data_list_helper<T>(args.baseKey, subKey, args.maxTime, args.count); }

  template<typename T> TimeValList<T>
  getDataAfter(const std::string& subKey, const GetDataArgs& args = {})
    { return get_forward_data_helper<T>(args.baseKey, subKey, args.minTime, 0, args.count); }

  template<typename T> TimeValList<std::vector<T>>
  getDataListAfter(const std::string& subKey, const GetDataArgs& args = {})
    { return get_forward_data_list_helper<T>(args.baseKey, subKey, args.minTime, 0, args.count); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getDataSingle     : get data as T (T is trivial, string or Attrs) at or before maxTime
  //  getDataListSingle : get data as type vector<T> (T is trivial) at or before maxTime
  //
  //    baseKey : base key of device
  //    subKey  : sub key to get data from
  //    dest    : destination to copy data to
  //    maxTime : time that equals or exceeds the data to get
  //    return  : time of the data item if successful, zero on failure
  //
  template<typename T> uint64_t
  getDataSingle(const std::string& subKey, T& dest, const GetDataArgs& args = {})
    { return get_single_data_helper<T>(args.baseKey, subKey, dest, args.maxTime); }

  template<typename T> uint64_t
  getDataListSingle(const std::string& subKey, std::vector<T>& dest, const GetDataArgs& args = {})
    { return get_single_data_list_helper<T>(args.baseKey, subKey, dest, args.maxTime); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Data (add)

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addData     : add multiple data items of type T (T is trivial, string or Attrs)
  //  addDataList : add multiple vector<T> as data items (T is trivial)
  //
  //    subKey : sub key to add data to
  //    data   : times and data to add (0 time means host time)
  //    trim   : trim to greater of this value or number of data items
  //    return : vector of ids of successfully added data items
  //
  template<typename T> std::vector<uint64_t>
  addData(const std::string& subKey, const TimeValList<T>& data, uint32_t trim = 1);

  template<typename T> std::vector<uint64_t>
  addDataList(const std::string& subKey, const TimeValList<std::vector<T>>& data, uint32_t trim = 1);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addDataSingleAt : add a data item of type T (T is trivial, string or Attrs) at specified time
  //  addDataSingle   : add a data item of type T (T is trivial, string or Attrs) at current time
  //  addDataDoubleAt : add a data item of type double at specified time
  //  addDataDouble   : add a data item of type double at current time
  //
  //    subKey : sub key to add data to
  //    time   : time to add the data at (0 for current host time)
  //    data   : data to add
  //    trim   : number of items to trim the stream to
  //    return : time of the added data item if successful, zero on failure
  //
  template<typename T> uint64_t
  addDataSingleAt(const std::string& subKey, uint64_t time, const T& data, uint32_t trim = 1);

  template<typename T> uint64_t
  addDataSingle(const std::string& subKey, const T& data, uint32_t trim = 1)
    { return addDataSingleAt(subKey, 0, data, trim); }

  uint64_t addDataDoubleAt(const std::string& subKey, uint64_t time, double data, uint32_t trim = 1);

  uint64_t addDataDouble(const std::string& subKey, double data, uint32_t trim = 1)
    { return addDataDoubleAt(subKey, 0, data, trim); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addDataListSingleAt : add a container<T> data item (T is trivial) at specified time
  //  addDataListSingle   : add a container<T> data item (T is trivial) at current time
  //
  //  note - container must implement 'T* data()' and 'size_t size()' (vector, array or span)
  //
  //    subKey : sub key to add data to
  //    time   : time to add the data at (0 is current host time)
  //    data   : pointer to buffer of type T data to add
  //    trim   : number of items to trim the stream to
  //    return : time of the added data item if successful, zero on failure

  //  overload for array and span
  template<template<typename T, size_t S> class C, typename T, size_t S> uint64_t
  addDataListSingleAt(const std::string& subKey, uint64_t time, const C<T, S>& data, uint32_t trim = 1)
    { return add_single_data_list_helper(subKey, time, data.data(), data.size(), trim); }

  //  overload for vector
  template<typename T> uint64_t
  addDataListSingleAt(const std::string& subKey, uint64_t time, const std::vector<T>& data, uint32_t trim = 1)
    { return add_single_data_list_helper(subKey, time, data.data(), data.size(), trim); }

  //  overload for array and span
  template<template<typename T, size_t S> class C, typename T, size_t S> uint64_t
  addDataListSingle(const std::string& subKey, const C<T, S>& data, uint32_t trim = 1)
    { return add_single_data_list_helper(subKey, 0, data.data(), data.size(), trim); }

  //  overload for vector
  template<typename T> uint64_t
  addDataListSingle(const std::string& subKey, const std::vector<T>& data, uint32_t trim = 1)
    { return add_single_data_list_helper(subKey, 0, data.data(), data.size(), trim); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Utility

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  connected : test if server is connected and responsive
  //
  //    return : true if connected, false if not connected
  //
  bool connected() { return _redis->ping(); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  copySetting : copy any setting key to a home setting key (dest key must not exist)
  //  copyData    : copy any data key to a home data key (dest key must not exist)
  //
  //    baseKey   : base key of source
  //    srcSubKey : sub key of source
  //    dstSubKey : sub key of destination
  //    return    : true if successful, false if unsuccessful
  //
  bool copySetting(const std::string& srcSubKey, const std::string& dstSubKey, const std::string& baseKey = "")
    { return copy_key_helper(srcSubKey, dstSubKey, SETTING_STUB, baseKey); }

  bool copyData(const std::string& srcSubKey, const std::string& dstSubKey, const std::string& baseKey = "")
    { return copy_key_helper(srcSubKey, dstSubKey, DATA_STUB, baseKey); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  renameSetting : rename a home setting key (dest key must not exist)
  //  renameData    : rename a home data key (dest key must not exist)
  //
  //    srcSubKey : sub key of source
  //    dstSubKey : sub key of destination
  //    return    : true if successful, false if unsuccessful
  //
  bool renameSetting(const std::string& subKeySrc, const std::string& subKeyDst)
    { return _redis->rename(build_key(SETTING_STUB, subKeySrc), build_key(SETTING_STUB, subKeyDst)); }

  bool renameData(const std::string& subKeySrc, const std::string& subKeyDst)
    { return _redis->rename(build_key(DATA_STUB, subKeySrc), build_key(DATA_STUB, subKeyDst)); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  deleteSetting : delete a home setting key
  //  deleteData    : delete a home data key
  //
  //    subKey : sub key to delete
  //    return : true if successful, false if unsuccessful
  //
  bool deleteSetting(const std::string& subKey) { return _redis->del(build_key(SETTING_STUB, subKey)) >= 0; }

  bool deleteData(const std::string& subKey) { return _redis->del(build_key(DATA_STUB, subKey)) >= 0; }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getServerTime : get time since epoch in nanoseconds from server
  //
  //    return : nanoseconds if successful, zero if unsuccessful
  //
  uint64_t getServerTime();

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Publish/Subscribe

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  ListenSubFn : callback function type for pub/sub notification
  //
  using ListenSubFn = std::function<void(const std::string& baseKey, const std::string& subKey, const std::string& message)>;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  publish : publish a message to a channel made up of base key and sub key
  //
  //    baseKey : the base key to construct the channel from
  //    subKey  : the sub key to construct the channel from
  //    message : the message to send
  //    return  : true on success, false on failure
  //
  bool publish(const std::string& subKey, const std::string& message, const std::string& baseKey = "")
    { return _redis->publish(build_key(CHANNEL_STUB, subKey, baseKey), message) >= 0; }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Stream Readers

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  ReaderSubFn : callback function type for stream reader notification
  //
  template<typename T>
  using ReaderSubFn = std::function<void(const std::string& baseKey, const std::string& subKey, const TimeValList<T>& data)>;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addStatusReader      : add a stream reader for a status key (type string)
  //  addLogReader         : add a stream reader for a log key (type string)
  //  addSettingReader     : add a stream reader for a setting key (trivial type or string)
  //  addSettingListReader : add a stream reader for a setting key (vector of trivial type)
  //  addDataReader        : add a stream reader for a data key (trivial type, string or Attr)
  //  addDataListReader    : add a stream reader for a data key (vector of trivial type)
  //
  //    baseKey : the base key to read from
  //    subKey  : the sub key to read from
  //    func    : the function to call when information is read on a key
  //    return  : true on success, false on failure
  //
  bool addStatusReader(const std::string& subKey, ReaderSubFn<std::string> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, STATUS_STUB, subKey, make_reader_callback(func)); }

  bool addLogReader(ReaderSubFn<std::string> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, LOG_STUB, "", make_reader_callback(func)); }

  template<typename T>
  bool addSettingReader(const std::string& subKey, ReaderSubFn<T> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, SETTING_STUB, subKey, make_reader_callback(func)); }

  template<typename T>
  bool addSettingListReader(const std::string& subKey, ReaderSubFn<std::vector<T>> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, SETTING_STUB, subKey, make_list_reader_callback(func)); }

  template<typename T>
  bool addDataReader(const std::string& subKey, ReaderSubFn<T> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, DATA_STUB, subKey, make_reader_callback(func)); }

  template<typename T>
  bool addDataListReader(const std::string& subKey, ReaderSubFn<std::vector<T>> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, DATA_STUB, subKey, make_list_reader_callback(func)); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addGenericReader : add a reader for a key that does NOT follow RedisAdapter schema
  //
  //    key     : the key to add (must NOT be a RedisAdapter schema key)
  //    func    : function to call when data is read - data will be Attrs
  //    return  : true if reader started, false if reader failed to start
  //
  bool addGenericReader(const std::string& key, ReaderSubFn<Attrs> func);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  removeStatusReader  : remove all readers for a status key
  //  removeLogReader     : remove all readers for the log key
  //  removeSettingReader : remove all readers for a setting key
  //  removeDataReader    : remove all readers for a data key
  //
  //    baseKey : the base key to remove
  //    subKey  : the sub key to remove
  //    return  : true on success, false on failure
  //
  bool removeStatusReader(const std::string& subKey, const std::string& baseKey = "")
    { return remove_reader_helper(baseKey, STATUS_STUB, subKey); }

  bool removeLogReader(const std::string& baseKey = "")
    { return remove_reader_helper(baseKey, LOG_STUB, ""); }

  bool removeSettingReader(const std::string& subKey, const std::string& baseKey = "")
    { return remove_reader_helper(baseKey, SETTING_STUB, subKey); }

  bool removeDataReader(const std::string& subKey, const std::string& baseKey = "")
    { return remove_reader_helper(baseKey, DATA_STUB, subKey); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  removeGenericReader : remove all readers for key that does NOT follow RedisAdapter schema
  //
  //    key     : the key to remove (must NOT be a RedisAdapter schema key)
  //    return  : true if reader started, false if reader failed to start
  //
  bool removeGenericReader(const std::string& key);

private:
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Containers for stream data suggested by the redis++ readme.md
  //    https://github.com/sewenew/redis-plus-plus#redis-stream
  //
  using Item = std::pair<std::string, Attrs>;
  using ItemStream = std::vector<Item>;
  using Streams = std::unordered_map<std::string, ItemStream>;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Redis key and field constants
  //
  const std::string DEFAULT_FIELD = "_";

  const std::string LOG_STUB      = "[*-LOG-*]";
  const std::string STATUS_STUB   = "[*-STATUS-*]";
  const std::string SETTING_STUB  = "[*-SETTING-*]";
  const std::string DATA_STUB     = "[*-DATA-*]";
  const std::string CHANNEL_STUB  = "[*-CHANNEL-*]";

  const std::string CONTROL_STUB  = "[*-CONTROL-*]";

  std::string build_key(const std::string& keyStub, const std::string& subKey = "", const std::string& baseKey = "") const;

  std::pair<std::string, std::string> split_key(const std::string& key) const;

  uint64_t id_to_time(const std::string& id) const { try { return std::stoull(id); } catch(...) {} return 0; }

  std::string time_to_id(uint64_t time = 0) const { return std::to_string(time ? time : get_host_time()) + "-0"; }

  std::string min_time_to_id(uint64_t time) const { return time ? time_to_id(time) : "-"; }

  std::string max_time_to_id(uint64_t time) const { return time ? time_to_id(time) : "+"; }

  uint64_t get_host_time() const;

  bool copy_key_helper(const std::string& srcSubKey, const std::string& dstSubKey, const std::string& keyStub, const std::string& baseKey);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions adding and removing stream readers
  //
  using reader_sub_fn = std::function<void(const std::string& baseKey, const std::string& subKey, const ItemStream& data)>;

  bool add_reader_helper(const std::string& baseKey, const std::string& keyStub, const std::string& subKey, reader_sub_fn func);

  template<typename T> reader_sub_fn make_reader_callback(ReaderSubFn<T> func) const;

  template<typename T> reader_sub_fn make_list_reader_callback(ReaderSubFn<std::vector<T>> func) const;

  bool remove_reader_helper(const std::string& baseKey, const std::string& keyStub, const std::string& subKey);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getting and setting DEFAULT_FIELD in Attrs
  //
  template<typename T> auto default_field_value(const Attrs& attrs) const;

  template<typename T> Attrs default_field_attrs(const T* data, size_t size) const;

  template<typename T> Attrs default_field_attrs(const T& data) const;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getting and adding data
  //
  template<typename T> TimeValList<T>
  get_forward_data_helper(const std::string& baseKey, const std::string& subKey, uint64_t minTime, uint64_t maxTime, uint32_t count);

  template<typename T> TimeValList<std::vector<T>>
  get_forward_data_list_helper(const std::string& baseKey, const std::string& subKey, uint64_t minTime, uint64_t maxTime, uint32_t count);

  template<typename T> TimeValList<T>
  get_reverse_data_helper(const std::string& baseKey, const std::string& subKey, uint64_t maxTime, uint32_t count);

  template<typename T> TimeValList<std::vector<T>>
  get_reverse_data_list_helper(const std::string& baseKey, const std::string& subKey, uint64_t maxTme, uint32_t count);

  template<typename T> uint64_t
  get_single_data_helper(const std::string& baseKey, const std::string& subKey, T& dest, uint64_t maxTime);

  template<typename T> uint64_t
  get_single_data_list_helper(const std::string& baseKey, const std::string& subKey, std::vector<T>& dest, uint64_t maxTime);

  template<typename T> uint64_t
  add_single_data_list_helper(const std::string& subKey, uint64_t time, const T* data, size_t size, uint32_t trim);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Redis stuff
  //
  std::unique_ptr<RedisConnection> _redis;

  std::string _base_key;

  uint32_t _timeout;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    std::string control;
    bool run = false;
  };
  std::unordered_map<uint16_t, reader_info> _reader;
};

#include "RedisAdapterTempl.hpp"
