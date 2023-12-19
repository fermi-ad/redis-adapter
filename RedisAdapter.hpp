/**
 * RedisAdapter.hpp
 *
 * This file contains the definition of the RedisAdapter class
 *
 * @author rsantucc
 */

#pragma once

#include "RedisConnection.hpp"
#include <thread>

namespace sw  //  https://github.com/sewenew/redis-plus-plus#redis-stream
{
  namespace redis
  {
    using Attrs = std::unordered_map<std::string, std::string>;
    template<typename T> using Item = std::pair<std::string, T>;
    template<typename T> using ItemStream = std::vector<Item<T>>;
    template<typename T> using Streams = std::unordered_map<std::string, ItemStream<T>>;
  }
}

namespace swr = sw::redis;

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
  swr::ItemStream<std::string> getLog(const std::string& minID, const std::string& maxID = "+");
  swr::ItemStream<std::string> getLogAfter(const std::string& minID, uint32_t count = 100);
  swr::ItemStream<std::string> getLogBefore(const std::string& maxID = "+", uint32_t count = 100);
  swr::ItemStream<std::string> getLogBefore(uint32_t count) { return getLogBefore("+", count); }

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
  template<typename T> swr::ItemStream<T>
  getData(const std::string& subKey, const std::string& minID, const std::string& maxID, const std::string& baseKey = "")
    { return get_forward_data_helper<T>(baseKey, subKey, minID, maxID, 0); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getDataList(const std::string& subKey, const std::string& minID, const std::string& maxID, const std::string& baseKey = "")
    { return get_forward_data_list_helper<T>(baseKey, subKey, minID, maxID, 0); }

  //  GetDataArgs : structure for providing arguments to getDataXXXX functions
  //                each field can be overidden or left as the default value
  //  suggested usage:
  //    using GDA = RedisAdapter::GetDataArgs;
  //    redis.getDataBefore("my:subkey", GDA{ .baseKey="my:basekey", .count=10 });
  //
  struct GetDataArgs
  {
    std::string minID = "-";
    std::string maxID = "+";
    std::string baseKey;
    uint32_t count = 1;
  };
  template<typename T> swr::ItemStream<T>
  getDataBefore(const std::string& subKey, const GetDataArgs& args = {})
    { return get_reverse_data_helper<T>(args.baseKey, subKey, args.maxID, args.count); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getDataListBefore(const std::string& subKey, const GetDataArgs& args = {})
    { return get_reverse_data_list_helper<T>(args.baseKey, subKey, args.maxID, args.count); }

  template<typename T> swr::ItemStream<T>
  getDataAfter(const std::string& subKey, const GetDataArgs& args = {})
    { return get_forward_data_helper<T>(args.baseKey, subKey, args.minID, "+", args.count); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getDataListAfter(const std::string& subKey, const GetDataArgs& args = {})
    { return get_forward_data_list_helper<T>(args.baseKey, subKey, args.minID, "+", args.count); }

  template<typename T> std::string
  getDataSingle(const std::string& subKey, T& dest, const GetDataArgs& args = {})
    { return get_single_data_helper<T>(args.baseKey, subKey, dest, args.maxID); }

  template<typename T> std::string
  getDataListSingle(const std::string& subKey, std::vector<T>& dest, const GetDataArgs& args = {})
    { return get_single_data_list_helper<T>(args.baseKey, subKey, dest, args.maxID); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Data (adding)
  //
  template<typename T> std::vector<std::string>
  addData(const std::string& subKey, const swr::ItemStream<T>& data, uint32_t trim = 1);

  template<typename T> std::vector<std::string>
  addDataList(const std::string& subKey, const swr::ItemStream<std::vector<T>>& data, uint32_t trim = 1);

  template<typename T> std::string
  addDataSingle(const std::string& subKey, const T& data, const std::string& id = "*", uint32_t trim = 1);

  template<typename T> std::string
  addDataSingle(const std::string& subKey, const T& data, uint32_t trim) { return addDataSingle(subKey, data, "*", trim); }

  std::string addDataDouble(const std::string& subKey, double data, const std::string& id = "*", uint32_t trim = 1);

  std::string addDataDouble(const std::string& subKey, double data, uint32_t trim) { return addDataDouble(subKey, data, "*", trim); }

  //  addDataListSingle : Adds data from a provided container that has the signature:
  //                        template<typename T, typename Allocator = std::allocator<T>>
  //                      and implements the methods:
  //                        const T* data() const;
  //                        size_type size() const;
  //                      Currently only std::vector satisfies these criteria. This method effectively
  //                      performs a memcpy of the contiguous inner storage of the container.
  //
  template<template<typename T, typename A> class C, typename T, typename A> std::string
  addDataListSingle(const std::string& subKey, const C<T, A>& data, const std::string& id = "*", uint32_t trim = 1)
    { return add_single_data_list_helper(subKey, data.data(), data.size(), id, trim); }

  template<template<typename T, typename A> class C, typename T, typename A> std::string
  addDataListSingle(const std::string& subKey, const C<T, A>& data, uint32_t trim)
    { return add_single_data_list_helper(subKey, data.data(), data.size(), "*", trim); }

  //  addDataListSingle : Adds data from a provided container that has the signature:
  //                        template<typename T, std::size_t Extent>
  //                      and implements the methods:
  //                        const T* data() const;
  //                        size_type size() const;
  //                      Currently std::array and std::span satisfy these criteria. This method
  //                      effectively performs a memcpy of the contiguous inner storage of the container.
  //
  template<template<typename T, size_t S> class C, typename T, size_t S> std::string
  addDataListSingle(const std::string& subKey, const C<T, S>& data, const std::string& id = "*", uint32_t trim = 1)
    { return add_single_data_list_helper(subKey, data.data(), data.size(), id, trim); }

  template<template<typename T, size_t S> class C, typename T, size_t S> std::string
  addDataListSingle(const std::string& subKey, const C<T, S>& data, uint32_t trim)
    { return add_single_data_list_helper(subKey, data.data(), data.size(), "*", trim); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Utility
  //
  bool connected() { return _redis->ping(); }

  bool copyKey(const std::string& src, const std::string& dst) { return _redis->copy(src, dst) == 1; }

  bool deleteKey(const std::string& key) { return _redis->del(key) >= 0; }

  swr::Optional<timespec> getTimespec();

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Publish/Subscribe
  //
  using ListenSubFn = std::function<void(const std::string& baseKey, const std::string& subKey, const std::string& message)>;

  template<typename T>
  using ReaderSubFn = std::function<void(const std::string& baseKey, const std::string& subKey, const swr::ItemStream<T>& data)>;

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

private:
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

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions adding and removing stream readers
  //
  bool add_reader_helper(const std::string& baseKey, const std::string& stub, const std::string& subKey, ReaderSubFn<swr::Attrs> func);

  template<typename T> ReaderSubFn<swr::Attrs> make_reader_callback(ReaderSubFn<T> func);

  template<typename T> ReaderSubFn<swr::Attrs> make_list_reader_callback(ReaderSubFn<std::vector<T>> func);

  bool remove_reader_helper(const std::string& baseKey, const std::string& stub, const std::string& subKey);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getting and setting DEFAULT_FIELD in swr::Attrs
  //
  template<typename T> auto default_field_value(const swr::Attrs& attrs);

  template<typename T> swr::Attrs default_field_attrs(const T* data, size_t size);

  template<typename T> swr::Attrs default_field_attrs(const T& data);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getting and adding data
  //
  template<typename T> swr::ItemStream<T>
  get_forward_data_helper(const std::string& baseKey, const std::string& subKey,
                          const std::string& minID, const std::string& maxID, uint32_t count);

  template<typename T> swr::ItemStream<std::vector<T>>
  get_forward_data_list_helper(const std::string& baseKey, const std::string& subKey,
                               const std::string& minID, const std::string& maxID, uint32_t count);

  template<typename T> swr::ItemStream<T>
  get_reverse_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& maxID, uint32_t count);

  template<typename T> swr::ItemStream<std::vector<T>>
  get_reverse_data_list_helper(const std::string& baseKey, const std::string& subKey, const std::string& maxID, uint32_t count);

  template<typename T> std::string
  get_single_data_helper(const std::string& baseKey, const std::string& subKey, T& dest, const std::string& maxID);

  template<typename T> std::string
  get_single_data_list_helper(const std::string& baseKey, const std::string& subKey, std::vector<T>& dest, const std::string& maxID);

  template<typename T> std::string
  add_single_data_list_helper(const std::string& subKey, const T* data, size_t size, const std::string& id, uint32_t trim);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Redis stuff
  //
  std::unique_ptr<RedisConnection> _redis;

  std::string _baseKey;

  uint32_t _timeout;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Pub/Sub Listener and Stream Reader
  //
  bool start_listener();
  std::thread _listener;
  bool _listenerRun;
  bool stop_listener();

  std::unordered_map<std::string, std::vector<ListenSubFn>> _patternSubs;
  std::unordered_map<std::string, std::vector<ListenSubFn>> _commandSubs;

  bool start_reader(uint16_t slot);
  bool stop_reader(uint16_t slot);

  struct ReaderInfo
  {
    std::thread thread;
    std::unordered_map<std::string, std::vector<ReaderSubFn<swr::Attrs>>> subs;
    std::unordered_map<std::string, std::string> keyids;
    std::string control;
    bool run = false;
  };
  std::unordered_map<uint16_t, ReaderInfo> _reader;
};

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  Helper functions for getting DEFAULT_FIELD in swr::Attrs
//
template<> inline auto RedisAdapter::default_field_value<std::string>(const swr::Attrs& attrs)
{
  std::string ret;
  if (attrs.count(DEFAULT_FIELD)) ret = attrs.at(DEFAULT_FIELD);
  return ret;
}
template<typename T> auto RedisAdapter::default_field_value(const swr::Attrs& attrs)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::Optional<T> ret;
  if (attrs.count(DEFAULT_FIELD)) ret = *(const T*)attrs.at(DEFAULT_FIELD).data();
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  Helper functions for setting DEFAULT_FIELD in swr::Attrs
//
template<> inline swr::Attrs RedisAdapter::default_field_attrs(const std::string& data)
{
  return {{ DEFAULT_FIELD, data }};
}
template<typename T> swr::Attrs RedisAdapter::default_field_attrs(const T* data, size_t size)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  return {{ DEFAULT_FIELD, data ? std::string((const char*)data, size * sizeof(T)) : "" }};
}
template<typename T> swr::Attrs RedisAdapter::default_field_attrs(const T& data)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  return {{ DEFAULT_FIELD, std::string((const char*)&data, sizeof(T)) }};
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getSetting : get setting for home device as type T (T is trivial, string)
//
//    subKey : sub key to get setting from
//    return : string or Optional with setting value if successful
//             empty string or Optional if unsuccessful
//
template<> inline auto RedisAdapter::getSetting<std::string>(const std::string& subKey, const std::string& baseKey)
{
  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, SETTINGS_STUB, subKey), "+", "-", 1, back_inserter(raw));

  return raw.size() ? default_field_value<std::string>(raw.front().second) : "";
}
template<typename T> auto RedisAdapter::getSetting(const std::string& subKey, const std::string& baseKey)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, SETTINGS_STUB, subKey), "+", "-", 1, back_inserter(raw));

  swr::Optional<T> ret;
  if (raw.size()) ret = default_field_value<T>(raw.front().second);
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getSettingList<T> : get setting for home device as type vector<T> (T is trivial)
//
//    subKey : sub key to get setting from
//    return : setting value as vector<T> or empty on failure
//
template<typename T> std::vector<T> RedisAdapter::getSettingList(const std::string& subKey, const std::string& baseKey)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, SETTINGS_STUB, subKey), "+", "-", 1, back_inserter(raw));

  std::vector<T> ret;
  if (raw.size())
  {
    std::string str = default_field_value<std::string>(raw.front().second);
    ret.assign((T*)str.data(), (T*)(str.data() + str.size()));
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  setSetting : set setting for home device as type T (T is trivial, string)
//
//    subKey : sub key to set setting on
//    value  : setting value to set
//    return : true on success, false on failure
//
template<typename T> bool RedisAdapter::setSetting(const std::string& subKey, const T& value)
{
  static_assert( ! std::is_same<T, double>(), "use setSettingDouble for double or 'f' suffix for float literal");
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  swr::Attrs attrs = default_field_attrs(value);

  return _redis->xaddTrim(_baseKey + SETTINGS_STUB + subKey, "*", attrs.begin(), attrs.end(), 1).size();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  setSettingList : set setting for home device as vector<T> (T is trivial)
//
//    subKey : sub key to set setting on
//    value  : vector of setting values to set
//    return : true on success, false on failure
//
template<typename T> bool RedisAdapter::setSettingList(const std::string& subKey, const std::vector<T>& value)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::Attrs attrs = default_field_attrs(value.data(), value.size());

  return _redis->xaddTrim(_baseKey + SETTINGS_STUB + subKey, "*", attrs.begin(), attrs.end(), 1).size();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_forward_data_helper<T> : get data as type T (T is trivial, string or Attrs)
//                               that occurred after a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    minID   : lowest time to get data for
//    maxID   : highest time to get data for
//    count   : max number of items to get
//    return  : ItemStream of Item<T>
//
template<> inline swr::ItemStream<swr::Attrs>
RedisAdapter::get_forward_data_helper(const std::string& baseKey, const std::string& subKey,
                                      const std::string& minID, const std::string& maxID, uint32_t count)
{
  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> ret;

  if (count) { _redis->xrange(key, minID, maxID, count, std::back_inserter(ret)); }
  else       { _redis->xrange(key, minID, maxID, std::back_inserter(ret)); }

  return ret;
}
template<typename T> swr::ItemStream<T>
RedisAdapter::get_forward_data_helper(const std::string& baseKey, const std::string& subKey,
                                      const std::string& minID, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> raw;

  if (count) { _redis->xrange(key, minID, maxID, count, std::back_inserter(raw)); }
  else       { _redis->xrange(key, minID, maxID, std::back_inserter(raw)); }

  swr::ItemStream<T> ret;
  swr::Item<T> retItem;
  for (const auto& rawItem : raw)
  {
    swr::Optional<T> maybe = default_field_value<T>(rawItem.second);
    if (maybe.has_value())
    {
      retItem.first = rawItem.first;
      retItem.second = maybe.value();
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_forward_data_list_helper<T> : get data as type vector<T> (T is trivial)
//                                    that occurred after a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    minID   : lowest time to get data for
//    maxID   : highest time to get data for
//    count   : max number of items to get
//    return  : ItemStream of Item<vector<T>>
//
template<typename T> swr::ItemStream<std::vector<T>>
RedisAdapter::get_forward_data_list_helper(const std::string& baseKey, const std::string& subKey,
                                           const std::string& minID, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> raw;

  if (count) { _redis->xrange(key, minID, maxID, count, std::back_inserter(raw)); }
  else       { _redis->xrange(key, minID, maxID, std::back_inserter(raw)); }

  swr::ItemStream<std::vector<T>> ret;
  swr::Item<std::vector<T>> retItem;
  for (const auto& rawItem : raw)
  {
    const std::string str = default_field_value<std::string>(rawItem.second);
    if (str.size())
    {
      retItem.first = rawItem.first;
      retItem.second.assign((T*)str.data(), (T*)(str.data() + str.size()));
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_reverse_data_helper<T> : get data as type T (T is trivial, string or Attrs)
//                               that occurred before a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    maxID   : highest time to get data for
//    count   : max number of items to get
//    return  : ItemStream of Item<Attrs>
//
template<> inline swr::ItemStream<swr::Attrs>
RedisAdapter::get_reverse_data_helper(const std::string& baseKey, const std::string& subKey,
                                      const std::string& maxID, uint32_t count)
{
  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> ret;

  if (count) { _redis->xrevrange(key, maxID, "-", count, std::back_inserter(ret)); }
  else       { _redis->xrevrange(key, maxID, "-", std::back_inserter(ret)); }

  std::reverse(ret.begin(), ret.end());   //  reverse in place
  return ret;
}
template<typename T> swr::ItemStream<T>
RedisAdapter::get_reverse_data_helper(const std::string& baseKey, const std::string& subKey,
                                      const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> raw;

  if (count) { _redis->xrevrange(key, maxID, "-", count, std::back_inserter(raw)); }
  else       { _redis->xrevrange(key, maxID, "-", std::back_inserter(raw)); }

  swr::ItemStream<T> ret;
  swr::Item<T> retItem;
  for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
  {
    swr::Optional<T> maybe = default_field_value<T>(rawItem->second);
    if (maybe.has_value())
    {
      retItem.first = rawItem->first;
      retItem.second = maybe.value();
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_reverse_data_list_helper<T> : get data as type vector<T> (T is trivial)
//                                    that occurred before a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    maxID   : highest time to get data for
//    count   : max number of items to get
//    return  : ItemStream of Item<vector<T>>
//
template<typename T> swr::ItemStream<std::vector<T>>
RedisAdapter::get_reverse_data_list_helper(const std::string& baseKey, const std::string& subKey,
                                           const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> raw;

  if (count) { _redis->xrevrange(key, maxID, "-", count, std::back_inserter(raw)); }
  else       { _redis->xrevrange(key, maxID, "-", std::back_inserter(raw)); }

  swr::ItemStream<std::vector<T>> ret;
  swr::Item<std::vector<T>> retItem;
  for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
  {
    const std::string str = default_field_value<std::string>(rawItem->second);
    if (str.size())
    {
      retItem.first = rawItem->first;
      retItem.second.assign((T*)str.data(), (T*)(str.data() + str.size()));
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_single_data_helper<T> : get a single data item of type T (T is trivial, string or Attrs)
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    dest    : destination to copy data to
//    maxID   : time that equals or exceeds the data to get
//    return  : id of the data item if successful
//              empty string on failure
//
template<> inline std::string
RedisAdapter::get_single_data_helper(const std::string& baseKey, const std::string& subKey,
                                     swr::Attrs& dest, const std::string& maxID)
{
  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, DATA_STUB, subKey), maxID, "-", 1, std::back_inserter(raw));

  std::string id;
  if (raw.size())
  {
    id = raw.front().first;
    dest = raw.front().second;
  }
  return id;
}
template<typename T> std::string
RedisAdapter::get_single_data_helper(const std::string& baseKey, const std::string& subKey,
                                     T& dest, const std::string& maxID)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, DATA_STUB, subKey), maxID, "-", 1, std::back_inserter(raw));

  std::string id;
  if (raw.size())
  {
    swr::Optional<T> maybe = default_field_value<T>(raw.front().second);
    if (maybe.has_value())
    {
      id = raw.front().first;
      dest = maybe.value();
    }
  }
  return id;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_single_data_list_helper<T> : get a single data item as vector<T> (T is trivial)
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    dest    : destination to copy data to
//    maxID   : time that equals or exceeds the data to get
//    return  : id of the data item if successful
//              empty string on failure
//
template<typename T> std::string
RedisAdapter::get_single_data_list_helper(const std::string& baseKey, const std::string& subKey,
                                          std::vector<T>& dest, const std::string& maxID)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, DATA_STUB, subKey), maxID, "-", 1, std::back_inserter(raw));

  std::string id;
  if (raw.size())
  {
    const std::string str = default_field_value<std::string>(raw.front().second);
    if (str.size())
    {
      id = raw.front().first;
      dest.assign((T*)str.data(), (T*)(str.data() + str.size()));
    }
  }
  return id;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addData<T> : add multiple data items of type T (T is trivial, string or Attrs)
//
//    subKey : sub key to add data to
//    data   : ids and data to add (empty ids treated as "*")
//    trim   : trim to greater of this value or number of data items
//    return : vector of ids of successfully added data items
//
template<> inline std::vector<std::string>
RedisAdapter::addData(const std::string& subKey, const swr::ItemStream<swr::Attrs>& data, uint32_t trim)
{
  std::vector<std::string> ret;
  std::string key = _baseKey + DATA_STUB + subKey;
  for (const auto& item : data)
  {
    std::string id = item.first.size() ? item.first : "*";
    id = _redis->xadd(key, id, item.second.begin(), item.second.end());
    if (id.size()) { ret.push_back(id); }
  }
  if (trim && ret.size()) { _redis->xtrim(key, std::max(trim, (uint32_t)ret.size())); }
  return ret;
}
template<typename T> std::vector<std::string>
RedisAdapter::addData(const std::string& subKey, const swr::ItemStream<T>& data, uint32_t trim)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::vector<std::string> ret;
  std::string key = _baseKey + DATA_STUB + subKey;
  for (const auto& item : data)
  {
    std::string id = item.first.size() ? item.first : "*";
    swr::Attrs attrs = default_field_attrs(item.second);
    id = _redis->xadd(key, id, attrs.begin(), attrs.end());
    if (id.size()) { ret.push_back(id); }
  }
  if (trim && ret.size()) { _redis->xtrim(key, std::max(trim, (uint32_t)ret.size())); }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addDataList<T> : add multiple vector<T> as data items (T is trivial)
//
//    subKey : sub key to add data to
//    data   : ids and data to add (empty ids treated as "*")
//    trim   : trim to greater of this value or number of data items
//    return : vector of ids of successfully added data items
//
template<typename T> std::vector<std::string>
RedisAdapter::addDataList(const std::string& subKey, const swr::ItemStream<std::vector<T>>& data, uint32_t trim)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::vector<std::string> ret;
  std::string key = _baseKey + DATA_STUB + subKey;
  for (const auto& item : data)
  {
    std::string id = item.first.size() ? item.first : "*";
    swr::Attrs attrs = default_field_attrs(item.second.data(), item.second.size());
    id = _redis->xadd(key, id, attrs.begin(), attrs.end());
    if (id.size()) { ret.push_back(id); }
  }
  if (trim && ret.size()) { _redis->xtrim(key, std::max(trim, (uint32_t)ret.size())); }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  make_reader_callback<T> : wrap user callback to convert data to desired type
//
//    func   : user callback that wants data as type T
//    base   : base key of desired data
//    sub    : sub key of desired data
//    raw    : raw data as type swr::Attrs
//    return : closure that reader thread can call upon data arrival
//
template<> inline RedisAdapter::ReaderSubFn<swr::Attrs>
RedisAdapter::make_reader_callback(ReaderSubFn<swr::Attrs> func) { return func; }

template<typename T> RedisAdapter::ReaderSubFn<swr::Attrs>
RedisAdapter::make_reader_callback(ReaderSubFn<T> func)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  return [&, func](const std::string& base, const std::string& sub, const swr::ItemStream<swr::Attrs>& raw)
  {
    swr::ItemStream<T> ret;
    swr::Item<T> retItem;
    for (const auto& rawItem : raw)
    {
      swr::Optional<T> maybe = default_field_value<T>(rawItem.second);
      if (maybe.has_value())
      {
        retItem.first = rawItem.first;
        retItem.second = maybe.value();
        ret.push_back(retItem);
      }
    }
    func(base, sub, ret);
  };
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  make_list_reader_callback<T> : wrap user callback to convert data to vector of T
//
//    func   : user callback that wants data as vector of T
//    base   : base key of desired data
//    sub    : sub key of desired data
//    raw    : raw data as type swr::Attrs
//    return : closure that reader thread can call upon data arrival
//
template<typename T> RedisAdapter::ReaderSubFn<swr::Attrs>
RedisAdapter::make_list_reader_callback(ReaderSubFn<std::vector<T>> func)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  return [&, func](const std::string& base, const std::string& sub, const swr::ItemStream<swr::Attrs>& raw)
  {
    swr::ItemStream<std::vector<T>> ret;
    swr::Item<std::vector<T>> retItem;
    for (const auto& rawItem : raw)
    {
      const std::string str = default_field_value<std::string>(rawItem.second);
      if (str.size())
      {
        retItem.first = rawItem.first;
        retItem.second.assign((T*)str.data(), (T*)(str.data() + str.size()));
        ret.push_back(retItem);
      }
    }
    func(base, sub, ret);
  };
}
