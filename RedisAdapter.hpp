/**
 * RedisAdapter.hpp
 *
 * This file contains the definition of the RedisAdapter class.
 *
 * @author rsantucc
 */

#pragma once

#include "RedisConnection.h"

namespace sw  //  https://github.com/sewenew/redis-plus-plus#redis-stream
{
  namespace redis
  {
    using Attrs = std::unordered_map<std::string, std::string>;
    template <typename T> using Item = std::pair<std::string, T>;
    template <typename T> using ItemStream = std::vector<Item<T>>;
    template <typename T> using Streams = std::unordered_map<std::string, ItemStream<T>>;
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
  //  Construction
  //
  RedisAdapter(const std::string& baseKey, const RedisConnection::Options& opts);
  RedisAdapter(const std::string& baseKey, const std::string& host = "", uint16_t port = 0);

  RedisAdapter(const RedisAdapter& ra) = delete;       //  copy construction not allowed
  RedisAdapter& operator=(const RedisAdapter& ra) = delete;   //  assignment not allowed

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Status
  //
  std::string getStatus(const std::string& subKey);
  std::string getForeignStatus(const std::string& foreignKey, const std::string& subKey);

  bool setStatus(const std::string& subKey, const std::string& value);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Log
  //
  swr::ItemStream<std::string> getLog(std::string minID, std::string maxID = "+");
  swr::ItemStream<std::string> getLogAfter(std::string minID, uint32_t count = 100);
  swr::ItemStream<std::string> getLogBefore(uint32_t count = 100, std::string maxID = "+");

  bool addLog(std::string message, uint32_t trim = 1000);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Settings
  //
  template<typename T> auto getSetting(const std::string& subKey);
  template<typename T> std::vector<T> getSettingList(const std::string& subKey);
  template<typename T> auto getForeignSetting(const std::string& foreignKey, const std::string& subKey);
  template<typename T> std::vector<T> getForeignSettingList(const std::string& foreignKey, const std::string& subKey);

  template<typename T> bool setSetting(const std::string& subKey, const T& value);
  template<typename T> bool setSettingList(const std::string& subKey, const std::vector<T>& value);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Data (getting local)
  //
  template<typename T> swr::ItemStream<T>
  getData(const std::string& subKey, const std::string& minID, const std::string& maxID)
  { return get_forward_data_helper<T>("", subKey, minID, maxID, 0); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getDataList(const std::string& subKey, const std::string& minID, const std::string& maxID)
  { return get_forward_data_list_helper<T>("", subKey, minID, maxID, 0); }

  template<typename T> swr::ItemStream<T>
  getDataBefore(const std::string& subKey, uint32_t count = 1, const std::string& maxID = "+")
  { return get_reverse_data_helper<T>("", subKey, maxID, count); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getDataListBefore(const std::string& subKey, uint32_t count = 1, const std::string& maxID = "+")
  { return get_reverse_data_list_helper<T>("", subKey, maxID, count); }

  template<typename T> swr::ItemStream<T>
  getDataAfter(const std::string& subKey, const std::string& minID = "-", uint32_t count = 1)
  { return get_forward_data_helper<T>("", subKey, minID, "+", count); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getDataListAfter(const std::string& subKey, const std::string& minID = "-", uint32_t count = 1)
  { return get_forward_data_list_helper<T>("", subKey, minID, "+", count); }

  template<typename T> std::string
  getDataSingle(const std::string& subKey, T& dest, const std::string& maxID = "+")
  { return get_single_data_helper<T>("", subKey, dest, maxID); }

  template<typename T> std::string
  getDataListSingle(const std::string& subKey, std::vector<T>& dest, const std::string& maxID = "+")
  { return get_single_data_list_helper<T>("", subKey, dest, maxID); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Data (getting foreign)
  //
  template<typename T> swr::ItemStream<T>
  getForeignData(const std::string& foreignKey, const std::string& subKey, const std::string& minID, const std::string& maxID)
  { return get_forward_data_helper<T>(foreignKey, subKey, minID, maxID, 0); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getForeignDataList(const std::string& foreignKey, const std::string& subKey, const std::string& minID, const std::string& maxID)
  { return get_forward_data_list_helper<T>(foreignKey, subKey, minID, maxID, 0); }

  template<typename T> swr::ItemStream<T>
  getForeignDataBefore(const std::string& foreignKey, const std::string& subKey, uint32_t count = 1, const std::string& maxID = "+")
  { return get_reverse_data_helper<T>(foreignKey, subKey, maxID, count); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getForeignDataListBefore(const std::string& foreignKey, const std::string& subKey, uint32_t count = 1, const std::string& maxID = "+")
  { return get_reverse_data_list_helper<T>(foreignKey, subKey, maxID, count); }

  template<typename T> swr::ItemStream<T>
  getForeignDataAfter(const std::string& foreignKey, const std::string& subKey, const std::string& minID = "-", uint32_t count = 1)
  { return get_forward_data_helper<T>(foreignKey, subKey, minID, "+", count); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getForeignDataListAfter(const std::string& foreignKey, const std::string& subKey, const std::string& minID = "-", uint32_t count = 1)
  { return get_forward_data_list_helper<T>(foreignKey, subKey, minID, "+", count); }

  template<typename T> std::string
  getForeignDataSingle(const std::string& foreignKey, const std::string& subKey, T& dest, const std::string& maxID = "+")
  { return get_single_data_helper<T>(foreignKey, subKey, dest, maxID); }

  template<typename T> std::string
  getForeignDataListSingle(const std::string& foreignKey, const std::string& subKey, std::vector<T>& dest, const std::string& maxID = "+")
  { return get_single_data_list_helper<T>(foreignKey, subKey, dest, maxID); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Data (adding)
  //
  template<typename T> std::string
  addDataSingle(const std::string& subKey, const T& data, const std::string& id = "*", uint32_t trim = 1);

  template<template<class> class C, typename T> std::string
  addDataListSingle(const std::string& subKey, const C<T>& data, const std::string& id = "*", uint32_t trim = 1);

  template<typename T> std::vector<std::string>
  addData(const std::string& subKey, const swr::ItemStream<T>& data, uint32_t trim = 1);

  template<typename T> std::vector<std::string>
  addDataList(const std::string& subKey, const swr::ItemStream<std::vector<T>>& data, uint32_t trim = 1);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Publish/Subscribe
  //
  void publish(std::string msg);
  void publish(std::string key, std::string msg);

  void psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> f);
  void subscribe(std::string channel, std::function<void(std::string,std::string)> f);

  void registerCommand(std::string command, std::function<void(std::string, std::string)> f);
  void addReader(std::string streamKey,  std::function<void(std::string, swr::ItemStream<swr::Attrs>)> f);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Utility
  //
  void copyKey(std::string src, std::string dst);
  void deleteKey(std::string key);

  virtual std::vector<std::string> getServerTime();
  virtual swr::Optional<timespec> getServerTimespec();

  void startListener();
  void startReader();

private:
  RedisConnection _redis;

  const std::string DEFAULT_FIELD = "_";

  const std::string LOG_STUB      = ":LOG";
  const std::string STATUS_STUB   = ":STATUS:";     //  trailing colon
  const std::string SETTINGS_STUB = ":SETTINGS:";   //  trailing colon
  const std::string DATA_STUB     = ":DATA:";       //  trailing colon
  const std::string COMMANDS_STUB = ":COMMANDS";

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getting and setting DEFAULT_FIELD in swr::Attrs
  //
  template<typename T> auto default_field_value(const swr::Attrs& attrs);

  template<template<class> class C, typename T> swr::Attrs default_field_attrs(const C<T>& data);

  template<typename T> swr::Attrs default_field_attrs(const T& data);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Helper functions for getData family of functions
  //
  template<typename T> swr::ItemStream<T>
  get_forward_data_helper(const std::string& foreignKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count);

  template<typename T> swr::ItemStream<std::vector<T>>
  get_forward_data_list_helper(const std::string& foreignKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count);

  template<typename T> swr::ItemStream<T>
  get_reverse_data_helper(const std::string& foreignKey, const std::string& subKey, const std::string& maxID, uint32_t count);

  template<typename T> swr::ItemStream<std::vector<T>>
  get_reverse_data_list_helper(const std::string& foreignKey, const std::string& subKey, const std::string& maxID, uint32_t count);

  template<typename T> std::string
  get_single_data_helper(const std::string& foreignKey, const std::string& subKey, T& dest, const std::string& maxID);

  template<typename T> std::string
  get_single_data_list_helper(const std::string& foreignKey, const std::string& subKey, std::vector<T>& dest, const std::string& maxID);

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Pub/Sub and Listener
  //
  void streamReadBlock(std::unordered_map<std::string, std::string>& keysID, swr::Streams<swr::Attrs>& result);

  std::thread _listener;
  void listener();

  std::thread _reader;
  void reader();

  struct patternFunctionPair
  {
    std::string pattern;
    std::function<void(std::string,std::string,std::string)> function;
  };
  std::vector<patternFunctionPair> _patternSubscriptions;

  struct keyFunctionPair
  {
      std::string key;
      std::function<void(std::string, std::string)> function;
  };
  std::vector<keyFunctionPair> _subscriptions;

  std::map<std::string, std::function<void(std::string,std::string)>> _commands;

  struct streamKeyFunctionPair
  {
      std::string streamKey;
      std::function<void(std::string, swr::ItemStream<swr::Attrs>)> function;
  };
  std::vector<streamKeyFunctionPair> _streamSubscriptions;

  std::unordered_map<std::string, std::string> _streamKeyID;

  std::string _baseKey;
  std::string _commandsKey;
};

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  Helper functions for getting and setting DEFAULT_FIELD in swr::Attrs
//
template<> inline auto RedisAdapter::default_field_value<std::string>(const swr::Attrs& attrs)
{
  return attrs.count(DEFAULT_FIELD) ? attrs.at(DEFAULT_FIELD) : "";
}
template<typename T> auto RedisAdapter::default_field_value(const swr::Attrs& attrs)
{
  swr::Optional<T> ret;
  if (attrs.count(DEFAULT_FIELD)) ret = *(const T*)attrs.at(DEFAULT_FIELD).data();
  return ret;
}

template<template<class> class C, typename T> swr::Attrs RedisAdapter::default_field_attrs(const C<T>& data)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {{ DEFAULT_FIELD, std::string((const char*)data.data(), data.size() * sizeof(data.front())) }};
}
template<> inline swr::Attrs RedisAdapter::default_field_attrs(const std::string& data)
{
  return {{ DEFAULT_FIELD, data }};
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
template<> inline auto RedisAdapter::getSetting<std::string>(const std::string& subKey)
{
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrevrange(_baseKey + SETTINGS_STUB + subKey, "+", "-", 1, back_inserter(raw));
  return raw.size() ? default_field_value<std::string>(raw.front().second) : "";
}
template<typename T> auto RedisAdapter::getSetting(const std::string& subKey)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  swr::Optional<T> ret;
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrevrange(_baseKey + SETTINGS_STUB + subKey, "+", "-", 1, back_inserter(raw));
  if (raw.size()) ret = default_field_value<T>(raw.front().second);
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getSettingList<T> : get setting for home device as type vector<T> (T is trivial)
//
//    subKey : sub key to get setting from
//    return : setting value as vector<T> or empty on failure
//
template<typename T> std::vector<T> RedisAdapter::getSettingList(const std::string& subKey)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  std::vector<T> ret;
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrevrange(_baseKey + SETTINGS_STUB + subKey, "+", "-", 1, back_inserter(raw));
  if (raw.size())
  {
    std::string str = default_field_value<std::string>(raw.front().second);
    ret.assign(str.data(), str.data() + str.size());
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getForeignSetting : get setting for foreign device as type T (T is trivial, string)
//
//    foreignKey : base key of foreign device
//    subKey     : sub key to get setting from
//    return     : string or Optional with setting value if successful
//                 string or empty Optional if unsuccessful
//
template<> inline auto RedisAdapter::getForeignSetting<std::string>(const std::string& foreignKey, const std::string& subKey)
{
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrevrange(foreignKey + SETTINGS_STUB + subKey, "+", "-", 1, back_inserter(raw));
  return raw.size() ? default_field_value<std::string>(raw.front().second) : "";
}
template<typename T> auto RedisAdapter::getForeignSetting(const std::string& foreignKey, const std::string& subKey)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  swr::Optional<T> ret;
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrevrange(foreignKey + SETTINGS_STUB + subKey, "+", "-", 1, back_inserter(raw));
  if (raw.size()) ret = default_field_value<T>(raw.front().second);
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getForeignSettingList<T> : get setting for foreign device as type vector<T> (T is trivial)
//
//    foreignKey : base key of foreign device
//    subKey     : sub key to get setting from
//    return     : setting value as vector<T> or empty on failure
//
template<typename T> std::vector<T> RedisAdapter::getForeignSettingList(const std::string& foreignKey, const std::string& subKey)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  std::vector<T> ret;
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrevrange(foreignKey + SETTINGS_STUB + subKey, "+", "-", 1, back_inserter(raw));
  if (raw.size())
  {
    std::string str = default_field_value<std::string>(raw.front().second);
    ret.assign(str.data(), str.data() + str.size());
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
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  std::string key = _baseKey + SETTINGS_STUB + subKey;
  swr::Attrs attrs = default_field_attrs(value);

  return _redis.xaddTrim(key, "*", attrs.begin(), attrs.end(), 1).size();
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
  std::string key = _baseKey + SETTINGS_STUB + subKey;
  swr::Attrs attrs = default_field_attrs(value);

  return _redis.xaddTrim(key, "*", attrs.begin(), attrs.end(), 1).size();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_forward_data_helper<T> : get data as type T (T is trivial, string or Attrs)
//                               that occurred after a specified time
//
//    foreignKey : base key of foreign device
//    subKey     : sub key to get data from
//    minID      : lowest time to get data for
//    maxID      : highest time to get data for
//    count      : max number of items to get
//    return     : ItemStream of Item<T>
//
template <> inline swr::ItemStream<swr::Attrs>
RedisAdapter::get_forward_data_helper(const std::string& foreignKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count)
{
  swr::ItemStream<swr::Attrs> ret;
  std::string key = (foreignKey.size() ? foreignKey : _baseKey) + DATA_STUB + subKey;

  if (count) { _redis.xrange(key, minID, maxID, count, std::back_inserter(ret)); }
  else       { _redis.xrange(key, minID, maxID, std::back_inserter(ret)); }

  return ret;
}
template<typename T> swr::ItemStream<T>
RedisAdapter::get_forward_data_helper(const std::string& foreignKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (foreignKey.size() ? foreignKey : _baseKey) + DATA_STUB + subKey;

  if (count) { _redis.xrange(key, minID, maxID, count, std::back_inserter(raw)); }
  else       { _redis.xrange(key, minID, maxID, std::back_inserter(raw)); }

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
//    foreignKey : base key of foreign device
//    subKey     : sub key to get data from
//    minID      : lowest time to get data for
//    maxID      : highest time to get data for
//    count      : max number of items to get
//    return     : ItemStream of Item<vector<T>>
//
template<typename T> swr::ItemStream<std::vector<T>>
RedisAdapter::get_forward_data_list_helper(const std::string& foreignKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (foreignKey.size() ? foreignKey : _baseKey) + DATA_STUB + subKey;

  if (count) { _redis.xrange(key, minID, maxID, count, std::back_inserter(raw)); }
  else       { _redis.xrange(key, minID, maxID, std::back_inserter(raw)); }

  swr::ItemStream<std::vector<T>> ret;
  swr::Item<std::vector<T>> retItem;
  for (const auto& rawItem : raw)
  {
    const std::string str = default_field_value<std::string>(rawItem.second);
    if (str.size())
    {
      retItem.first = rawItem.first;
      retItem.second.assign(str.data(), str.data() + str.size());
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_reverse_data_helper<T> : get data as type T (T is trivial, string or Attrs)
//                               that occurred before a specified time
//
//    foreignKey : base key of foreign device
//    subKey     : sub key to get data from
//    maxID      : highest time to get data for
//    count      : max number of items to get
//    return     : ItemStream of Item<Attrs>
//
template <> inline swr::ItemStream<swr::Attrs>
RedisAdapter::get_reverse_data_helper(const std::string& foreignKey, const std::string& subKey, const std::string& maxID, uint32_t count)
{
  swr::ItemStream<swr::Attrs> ret;
  std::string key = (foreignKey.size() ? foreignKey : _baseKey) + DATA_STUB + subKey;

  if (count) { _redis.xrevrange(key, maxID, "-", count, std::back_inserter(ret)); }
  else       { _redis.xrevrange(key, maxID, "-", std::back_inserter(ret)); }

  std::reverse(ret.begin(), ret.end());   //  reverse in place
  return ret;
}
template<typename T> swr::ItemStream<T>
RedisAdapter::get_reverse_data_helper(const std::string& foreignKey, const std::string& subKey, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (foreignKey.size() ? foreignKey : _baseKey) + DATA_STUB + subKey;

  if (count) { _redis.xrevrange(key, maxID, "-", count, std::back_inserter(raw)); }
  else       { _redis.xrevrange(key, maxID, "-", std::back_inserter(raw)); }

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
//    foreignKey : base key of foreign device
//    subKey     : sub key to get data from
//    maxID      : highest time to get data for
//    count      : max number of items to get
//    return     : ItemStream of Item<vector<T>>
//
template<typename T> swr::ItemStream<std::vector<T>>
RedisAdapter::get_reverse_data_list_helper(const std::string& foreignKey, const std::string& subKey, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (foreignKey.size() ? foreignKey : _baseKey) + DATA_STUB + subKey;

  if (count) { _redis.xrevrange(key, maxID, "-", count, std::back_inserter(raw)); }
  else       { _redis.xrevrange(key, maxID, "-", std::back_inserter(raw)); }

  swr::ItemStream<std::vector<T>> ret;
  swr::Item<std::vector<T>> retItem;
  for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
  {
    const std::string str = default_field_value<std::string>(rawItem->second);
    if (str.size())
    {
      retItem.first = rawItem->first;
      retItem.second.assign(str.data(), str.data() + str.size());
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_single_data_helper<T> : get a single data item of type T (T is trivial, string or Attrs)
//
//    foreignKey : base key of foreign device
//    subKey : sub key to get data from
//    dest   : destination to copy data to
//    maxID  : time that equals or exceeds the data to get
//    return : id of the data item if successful
//             empty string on failure
//
template<> inline std::string
RedisAdapter::get_single_data_helper(const std::string& foreignKey, const std::string& subKey, swr::Attrs& dest, const std::string& maxID)
{
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (foreignKey.size() ? foreignKey : _baseKey) + DATA_STUB + subKey;
  _redis.xrevrange(key, maxID, "-", 1, std::back_inserter(raw));
  std::string id;
  if (raw.size())
  {
    id = raw.front().first;
    dest = raw.front().second;
  }
  return id;
}
template<typename T> std::string
RedisAdapter::get_single_data_helper(const std::string& foreignKey, const std::string& subKey, T& dest, const std::string& maxID)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (foreignKey.size() ? foreignKey : _baseKey) + DATA_STUB + subKey;
  _redis.xrevrange(key, maxID, "-", 1, std::back_inserter(raw));
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
//    foreignKey : base key of foreign device
//    subKey : sub key to get data from
//    dest   : destination to copy data to
//    maxID  : time that equals or exceeds the data to get
//    return : id of the data item if successful
//             empty string on failure
//
template<typename T> std::string
RedisAdapter::get_single_data_list_helper(const std::string& foreignKey, const std::string& subKey, std::vector<T>& dest, const std::string& maxID)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (foreignKey.size() ? foreignKey : _baseKey) + DATA_STUB + subKey;
  _redis.xrevrange(key, maxID, "-", 1, std::back_inserter(raw));
  std::string id;
  if (raw.size())
  {
    const std::string str = default_field_value<std::string>(raw.front().second);
    if (str.size())
    {
      id = raw.front().first;
      dest.assign(str.data(), str.data() + str.size());
    }
  }
  return id;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addDataSingle<T> : add a data item of type T (T is trivial, string or Attrs)
//
//    subKey : sub key to add data to
//    data   : data to add
//    id     : time to add the data at ("*" is current redis time)
//    trim   : number of items to trim the stream to
//    return : id of the added data item if successful
//             empty string on failure
//
template<> inline std::string
RedisAdapter::addDataSingle(const std::string& subKey, const swr::Attrs& data, const std::string& id, uint32_t trim)
{
  std::string key = _baseKey + DATA_STUB + subKey;

  return trim ? _redis.xaddTrim(key, id, data.begin(), data.end(), trim)
              : _redis.xadd(key, id, data.begin(), data.end());
}
template<typename T> std::string
RedisAdapter::addDataSingle(const std::string& subKey, const T& data, const std::string& id, uint32_t trim)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  std::string key = _baseKey + DATA_STUB + subKey;
  swr::Attrs attrs = default_field_attrs(data);

  return trim ? _redis.xaddTrim(key, id, attrs.begin(), attrs.end(), trim)
              : _redis.xadd(key, id, attrs.begin(), attrs.end());
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addDataListSingle<C<T>> : add a vector<T> or span<T> as a data item (T is trivial)
//
//    subKey : sub key to add data to
//    data   : data to add
//    id     : time to add the data at ("*" is current redis time)
//    trim   : number of items to trim the stream to
//    return : id of the added data item if successful
//             empty string on failure
//
template<template<class> class C, typename T> inline std::string
RedisAdapter::addDataListSingle(const std::string& subKey, const C<T>& data, const std::string& id, uint32_t trim)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  std::string key = _baseKey + DATA_STUB + subKey;
  swr::Attrs attrs = default_field_attrs(data);

  return trim ? _redis.xaddTrim(key, id, attrs.begin(), attrs.end(), trim)
              : _redis.xadd(key, id, attrs.begin(), attrs.end());
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
    id = _redis.xadd(key, id, item.second.begin(), item.second.end());
    if (id.size()) { ret.push_back(id); }
  }
  if (trim && ret.size()) { _redis.xtrim(key, std::max(trim, (uint32_t)ret.size())); }
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
    id = _redis.xadd(key, id, attrs.begin(), attrs.end());
    if (id.size()) { ret.push_back(id); }
  }
  if (trim && ret.size()) { _redis.xtrim(key, std::max(trim, (uint32_t)ret.size())); }
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
    swr::Attrs attrs = default_field_attrs(item.second);
    id = _redis.xadd(key, id, attrs.begin(), attrs.end());
    if (id.size()) { ret.push_back(id); }
  }
  if (trim && ret.size()) { _redis.xtrim(key, std::max(trim, (uint32_t)ret.size())); }
  return ret;
}