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
  //
  std::string getStatus(const std::string& subKey, const std::string& baseKey = "");

  bool setStatus(const std::string& subKey, const std::string& value);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Log
  //
  TimeValList<std::string> getLog(uint64_t minTime, uint64_t maxTime = 0);
  TimeValList<std::string> getLogAfter(uint64_t minTime, uint32_t count = 100);
  TimeValList<std::string> getLogBefore(uint64_t maxTime = 0, uint32_t count = 100);
  TimeValList<std::string> getLogCountBefore(uint32_t count, uint64_t maxTime = 0)
    { return getLogBefore(maxTime, count); }

  bool addLog(const std::string& message, uint32_t trim = 1000);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Settings
  //
  template<typename T> auto getSetting(const std::string& subKey, const std::string& baseKey = "");
  template<typename T> std::vector<T> getSettingList(const std::string& subKey, const std::string& baseKey = "");

  template<typename T> bool setSetting(const std::string& subKey, const T& value);
  template<typename T> bool setSettingList(const std::string& subKey, const std::vector<T>& value);
  bool setSettingDouble(const std::string& subKey, double value);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Data (getting)
  //
  template<typename T> TimeValList<T>
  getData(const std::string& subKey, const uint64_t minTime, uint64_t maxTime, const std::string& baseKey = "")
    { return get_forward_data_helper<T>(baseKey, subKey, minTime, maxTime, 0); }

  template<typename T> TimeValList<std::vector<T>>
  getDataList(const std::string& subKey, uint64_t minTime, uint64_t maxTime, const std::string& baseKey = "")
    { return get_forward_data_list_helper<T>(baseKey, subKey, minTime, maxTime, 0); }

  //  GetDataArgs : structure for providing arguments to getDataXXXX functions
  //                each field can be overidden or left as the default value
  //  suggested usage:
  //    using GDA = RedisAdapter::GetDataArgs;
  //    redis.getDataBefore("my:subkey", GDA{ .baseKey="my:basekey", .count=10 });
  //
  struct GetDataArgs
  {
    std::string baseKey;
    uint64_t minTime = 0;
    uint64_t maxTime = 0;
    uint32_t count = 1;
  };
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

  template<typename T> uint64_t
  getDataSingle(const std::string& subKey, T& dest, const GetDataArgs& args = {})
    { return get_single_data_helper<T>(args.baseKey, subKey, dest, args.maxTime); }

  template<typename T> uint64_t
  getDataListSingle(const std::string& subKey, std::vector<T>& dest, const GetDataArgs& args = {})
    { return get_single_data_list_helper<T>(args.baseKey, subKey, dest, args.maxTime); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Data (adding)
  //
  template<typename T> std::vector<uint64_t>
  addData(const std::string& subKey, const TimeValList<T>& data, uint32_t trim = 1);

  template<typename T> std::vector<uint64_t>
  addDataList(const std::string& subKey, const TimeValList<std::vector<T>>& data, uint32_t trim = 1);

  template<typename T> uint64_t
  addDataSingle(const std::string& subKey, uint64_t time, const T& data, uint32_t trim = 1);

  template<typename T> uint64_t
  addDataSingle(const std::string& subKey, const T& data, uint32_t trim = 1)
    { return addDataSingle(subKey, 0, data, trim); }

  uint64_t addDataDouble(const std::string& subKey, uint64_t time, double data, uint32_t trim = 1);

  uint64_t addDataDouble(const std::string& subKey, double data, uint32_t trim = 1)
    { return addDataDouble(subKey, 0, data, trim); }

  //  addDataListSingle : Adds data from a provided container that has the signature:
  //                        template<typename T, typename Allocator = std::allocator<T>>
  //                      and implements the methods:
  //                        const T* data() const;
  //                        size_type size() const;
  //                      Currently only std::vector satisfies these criteria. This method effectively
  //                      performs a memcpy of the contiguous inner storage of the container.
  //
  template<template<typename T, typename A> class C, typename T, typename A> uint64_t
  addDataListSingle(const std::string& subKey, uint64_t time, const C<T, A>& data, uint32_t trim = 1)
  {
    static_assert(std::is_same<C<T, A>, std::vector<T>>(), "wrong type C");
    return add_single_data_list_helper(subKey, time, data.data(), data.size(), trim);
  }
  template<template<typename T, typename A> class C, typename T, typename A> uint64_t
  addDataListSingle(const std::string& subKey, const C<T, A>& data, uint32_t trim = 1)
  {
    static_assert(std::is_same<C<T, A>, std::vector<T>>(), "wrong type C");
    return add_single_data_list_helper(subKey, 0, data.data(), data.size(), trim);
  }

  //  addDataListSingle : Adds data from a provided container that has the signature:
  //                        template<typename T, std::size_t Extent>
  //                      and implements the methods:
  //                        const T* data() const;
  //                        size_type size() const;
  //                      Currently std::array and std::span satisfy these criteria. This method
  //                      effectively performs a memcpy of the contiguous inner storage of the container.
  //
  template<template<typename T, size_t S> class C, typename T, size_t S> uint64_t
  addDataListSingle(const std::string& subKey, uint64_t time, const C<T, S>& data, uint32_t trim = 1)
    { return add_single_data_list_helper(subKey, time, data.data(), data.size(), trim); }

  template<template<typename T, size_t S> class C, typename T, size_t S> uint64_t
  addDataListSingle(const std::string& subKey, const C<T, S>& data, uint32_t trim = 1)
    { return add_single_data_list_helper(subKey, 0, data.data(), data.size(), trim); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Utility
  //
  bool connected() { return _redis->ping(); }

  bool copyKey(const std::string& src, const std::string& dst) { return _redis->copy(src, dst) == 1; }

  bool deleteKey(const std::string& key) { return _redis->del(key) >= 0; }

  std::optional<timespec> getTimespec();

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Publish/Subscribe
  //
  using ListenSubFn = std::function<void(const std::string& baseKey, const std::string& subKey, const std::string& message)>;

  template<typename T>
  using ReaderSubFn = std::function<void(const std::string& baseKey, const std::string& subKey, const TimeValList<T>& data)>;

  bool publish(const std::string& subKey, const std::string& message, const std::string& baseKey = "")
    { return _redis->publish(build_key(baseKey, COMMANDS_STUB, subKey), message) >= 0; }

  bool psubscribe(const std::string& pattern, ListenSubFn func, const std::string& baseKey = "");

  bool subscribe(const std::string& command, ListenSubFn func, const std::string& baseKey = "");

  bool unsubscribe(const std::string& unsub, const std::string& baseKey = "");

  bool addStatusReader(const std::string& subKey, ReaderSubFn<std::string> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, STATUS_STUB, subKey, make_reader_callback(func)); }

  bool addLogReader(ReaderSubFn<std::string> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, LOG_STUB, "", make_reader_callback(func)); }

  template<typename T>
  bool addSettingReader(const std::string& subKey, ReaderSubFn<T> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, SETTINGS_STUB, subKey, make_reader_callback(func)); }

  template<typename T>
  bool addSettingListReader(const std::string& subKey, ReaderSubFn<std::vector<T>> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, SETTINGS_STUB, subKey, make_list_reader_callback(func)); }

  template<typename T>
  bool addDataReader(const std::string& subKey, ReaderSubFn<T> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, DATA_STUB, subKey, make_reader_callback(func)); }

  template<typename T>
  bool addDataListReader(const std::string& subKey, ReaderSubFn<std::vector<T>> func, const std::string& baseKey = "")
    { return add_reader_helper(baseKey, DATA_STUB, subKey, make_list_reader_callback(func)); }

  bool removeStatusReader(const std::string& subKey, const std::string& baseKey = "")
    { return remove_reader_helper(baseKey, STATUS_STUB, subKey); }

  bool removeLogReader(const std::string& baseKey = "")
    { return remove_reader_helper(baseKey, LOG_STUB, ""); }

  bool removeSettingReader(const std::string& subKey, const std::string& baseKey = "")
    { return remove_reader_helper(baseKey, SETTINGS_STUB, subKey); }

  bool removeDataReader(const std::string& subKey, const std::string& baseKey = "")
    { return remove_reader_helper(baseKey, DATA_STUB, subKey); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Containers for stream data suggested by the redis++ readme.md
  //    https://github.com/sewenew/redis-plus-plus#redis-stream
  //
  using Attrs = std::unordered_map<std::string, std::string>;

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

  const std::string LOG_STUB      = ":LOG";
  const std::string STATUS_STUB   = ":STATUS:";     //  trailing colon
  const std::string SETTINGS_STUB = ":SETTINGS:";   //  trailing colon
  const std::string DATA_STUB     = ":DATA:";       //  trailing colon
  const std::string COMMANDS_STUB = ":COMMANDS:";   //  trailing colon

  const std::string CONTROL_STUB  = ":[*-CTRL-*]";

  std::string build_key(const std::string& baseKey, const std::string& stub, const std::string& subKey);

  std::pair<std::string, std::string> split_key(const std::string& key);

  uint64_t id_to_time(const std::string& id) { return std::stoull(id); }

  std::string time_to_id(uint64_t time = 0) { return std::to_string(time ? time : get_host_time()) + "-0"; }

  std::string min_time_to_id(uint64_t time) { return time ? time_to_id(time) : "-"; }

  std::string max_time_to_id(uint64_t time) { return time ? time_to_id(time) : "+"; }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions adding and removing stream readers
  //
  using reader_sub_fn = std::function<void(const std::string& baseKey, const std::string& subKey, const ItemStream& data)>;

  bool add_reader_helper(const std::string& baseKey, const std::string& stub, const std::string& subKey, reader_sub_fn func);

  template<typename T> reader_sub_fn make_reader_callback(ReaderSubFn<T> func);

  template<typename T> reader_sub_fn make_list_reader_callback(ReaderSubFn<std::vector<T>> func);

  bool remove_reader_helper(const std::string& baseKey, const std::string& stub, const std::string& subKey);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getting and setting DEFAULT_FIELD in Attrs
  //
  template<typename T> auto default_field_value(const Attrs& attrs);

  template<typename T> Attrs default_field_attrs(const T* data, size_t size);

  template<typename T> Attrs default_field_attrs(const T& data);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getting and adding data
  //
  template<typename T> TimeValList<T>
  get_forward_data_helper(const std::string& baseKey, const std::string& subKey,
                          uint64_t minTime, uint64_t maxTime, uint32_t count);

  template<typename T> TimeValList<std::vector<T>>
  get_forward_data_list_helper(const std::string& baseKey, const std::string& subKey,
                               uint64_t minTime, uint64_t maxTime, uint32_t count);

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

  uint64_t get_host_time();

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
