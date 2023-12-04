/**
 * RedisAdapter.hpp
 *
 * This file contains the definition of the redis Adapter class.
 *
 * @author rsantucc
 */

#pragma once

#include "RedisConnection.h"
#include <algorithm>

namespace sw
{
  namespace redis
  {
    using Attrs = std::unordered_map<std::string, std::string>;
    template <typename T> using Item = std::pair<std::string, T>;
    template <typename T> using ItemStream = std::vector<Item<T>>;
    template <typename T> using Streams = std::unordered_map<std::string, ItemStream<T>>;
  }
}

/**
 * RedisAdapter
 */
class RedisAdapter
{
public:
  //  Constructors
  RedisAdapter(const std::string& baseKey, const std::string& host = "", uint16_t port = 0);
  RedisAdapter(const std::string& baseKey, const RedisConnection::Options& opts);

  RedisAdapter(const RedisAdapter& ra) = delete;
  RedisAdapter& operator=(const RedisAdapter& ra) = delete;

  //  Status
  std::string getStatus(const std::string& subKey);
  std::string getForeignStatus(const std::string& baseKey, const std::string& subKey);

  bool setStatus(const std::string& subKey, const std::string& value);

  //  Log
  swr::ItemStream<std::string> getLog(std::string minID, std::string maxID = "+");
  swr::ItemStream<std::string> getLogAfter(std::string minID, uint32_t count);
  swr::ItemStream<std::string> getLogBefore(std::string maxID, uint32_t count = 1);

  bool addLog(std::string message, uint32_t trim = 1000);

  //  Settings
  template<typename T> T getSetting(const std::string& subKey);
  template<typename T> std::vector<T> getSettingList(const std::string& subKey);

  template<typename T> T getForeignSetting(const std::string& baseKey, const std::string& subKey);
  template<typename T> std::vector<T> getForeignSettingList(const std::string& baseKey, const std::string& subKey);

  template<typename T> bool setSetting(const std::string& subKey, const T& value);
  template<typename T> bool setSettingList(const std::string& subKey, const std::vector<T>& value);

  //  Data
  template<typename T> swr::ItemStream<T>
  getData(const std::string& subKey, const std::string& minID, const std::string& maxID)
  { return get_fwd_data_helper<T>("", subKey, minID, maxID, 0); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getDataList(const std::string& subKey, const std::string& minID, const std::string& maxID)
  { return get_fwd_data_list_helper<T>("", subKey, minID, maxID, 0); }

  template<typename T> swr::ItemStream<T>
  getForeignData(const std::string& baseKey, const std::string& subKey, const std::string& minID, const std::string& maxID)
  { return get_fwd_data_helper<T>(baseKey, subKey, minID, maxID, 0); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getForeignDataList(const std::string& baseKey, const std::string& subKey, const std::string& minID, const std::string& maxID)
  { return get_fwd_data_list_helper<T>(baseKey, subKey, minID, maxID, 0); }

  template<typename T> swr::ItemStream<T>
  getDataBefore(const std::string& subKey, const std::string& maxID = "+", uint32_t count = 1)
  { return get_rev_data_helper<T>("", subKey, maxID, count); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getDataListBefore(const std::string& subKey, const std::string& maxID = "+", uint32_t count = 1)
  { return get_rev_data_list_helper<T>("", subKey, maxID, count); }

  template<typename T> swr::ItemStream<T>
  getForeignDataBefore(const std::string& baseKey, const std::string& subKey, const std::string& maxID = "+", uint32_t count = 1)
  { return get_rev_data_helper<T>(baseKey, subKey, maxID, count); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getForeignDataListBefore(const std::string& baseKey, const std::string& subKey, const std::string& maxID = "+", uint32_t count = 1)
  { return get_rev_data_list_helper<T>(baseKey, subKey, maxID, count); }

  template<typename T> swr::ItemStream<T>
  getDataAfter(const std::string& subKey, const std::string& minID = "-", uint32_t count = 1)
  { return get_fwd_data_helper<T>("", subKey, minID, "+", count); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getDataListAfter(const std::string& subKey, const std::string& minID = "-", uint32_t count = 1)
  { return get_fwd_data_list_helper<T>("", subKey, minID, "+", count); }

  template<typename T> swr::ItemStream<T>
  getForeignDataAfter(const std::string& baseKey, const std::string& subKey, const std::string& minID = "-", uint32_t count = 1)
  { return get_fwd_data_helper<T>(baseKey, subKey, minID, "+", count); }

  template<typename T> swr::ItemStream<std::vector<T>>
  getForeignDataListAfter(const std::string& baseKey, const std::string& subKey, const std::string& minID = "-", uint32_t count = 1)
  { return get_fwd_data_list_helper<T>(baseKey, subKey, minID, "+", count); }

  template<typename T> std::string
  addData(const std::string& subKey, const T& data, const std::string& id = "*", uint32_t trim = 1)
  { return add_data_helper<T>(subKey, data, id, trim); }

  template<typename T> std::vector<std::string>
  addDataList(const std::string& subKey, const swr::ItemStream<T>& data, uint32_t trim = 1)
  { return add_data_list_helper<T>(subKey, data, trim); }

  //  Publish / Subscribe Functions
  void publish(std::string msg);
  void publish(std::string key, std::string msg);

  void psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> f);
  void subscribe(std::string channel, std::function<void(std::string,std::string)> f);

  void registerCommand(std::string command, std::function<void(std::string, std::string)> f);
  void addReader(std::string streamKey,  std::function<void(std::string, swr::ItemStream<swr::Attrs>)> f);

  //  Copy & Delete Functions
  void copyKey(std::string src, std::string dst);
  void deleteKey(std::string key);

  //  Time
  virtual std::vector<std::string> getServerTime();
  virtual swr::Optional<timespec> getServerTimespec();

  void startListener();
  void startReader();

private:
  RedisConnection _redis;

  const std::string DEFAULT_FIELD = "_";

  template<typename T> T default_field_value(const swr::Item<swr::Attrs> item);

  template<typename T> swr::Attrs default_field_attrs(const T& data);

  template<typename T> swr::ItemStream<T>
  get_fwd_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count);

  template<typename T> swr::ItemStream<std::vector<T>>
  get_fwd_data_list_helper(const std::string& baseKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count);

  template<typename T> swr::ItemStream<T>
  get_rev_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& maxID, uint32_t count);

  template<typename T> swr::ItemStream<std::vector<T>>
  get_rev_data_list_helper(const std::string& baseKey, const std::string& subKey, const std::string& maxID, uint32_t count);

  template<typename T> std::string
  add_data_helper(const std::string& subKey, const T& data, const std::string& id, uint32_t trim);

  template<typename T> std::vector<std::string>
  add_data_list_helper(const std::string& subKey, const swr::ItemStream<T>& data, uint32_t trim);

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
  std::string _settingsKey;
  std::string _logKey;
  std::string _commandsKey;
  std::string _statusKey;
  std::string _dataKey;
};

using RedisAdapterSingle = RedisAdapter;
using RedisAdapterCluster = RedisAdapter;

template<> inline std::string RedisAdapter::default_field_value(const swr::Item<swr::Attrs> item) { return item.second.at(DEFAULT_FIELD); }

template<typename T> T RedisAdapter::default_field_value(const swr::Item<swr::Attrs> item) { return *(T*)item.second.at(DEFAULT_FIELD).data(); }

template<> inline swr::Attrs RedisAdapter::default_field_attrs(const std::string& data) { return {{ DEFAULT_FIELD, data }}; }

template<typename T> swr::Attrs RedisAdapter::default_field_attrs(const T& data) { return {{ DEFAULT_FIELD, std::string((const char*)&data, sizeof(T)) }}; }

template<typename T> T RedisAdapter::getSetting(const std::string& subKey)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  return {};
}

template<typename T> std::vector<T> RedisAdapter::getSettingList(const std::string& subKey)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}

template<typename T> T RedisAdapter::getForeignSetting(const std::string& baseKey, const std::string& subKey)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  return {};
}

template<typename T> std::vector<T> RedisAdapter::getForeignSettingList(const std::string& baseKey, const std::string& subKey)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}

template<typename T> bool RedisAdapter::setSetting(const std::string& subKey, const T& value)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  return {};
}

template<typename T> bool RedisAdapter::setSettingList(const std::string& subKey, const std::vector<T>& value)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}

template <> inline swr::ItemStream<swr::Attrs>
RedisAdapter::get_fwd_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count)
{
  swr::ItemStream<swr::Attrs> ret;
  std::string key = (baseKey.size() ? baseKey : _baseKey) + ":DATA:" + subKey;
  if (count) { _redis.xrange(key, minID, maxID, count, std::back_inserter(ret)); }
  else       { _redis.xrange(key, minID, maxID, std::back_inserter(ret)); }
  return ret;
}

template<typename T> swr::ItemStream<T>
RedisAdapter::get_fwd_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (baseKey.size() ? baseKey : _baseKey) + ":DATA:" + subKey;
  bool ok = count ? _redis.xrange(key, minID, maxID, count, std::back_inserter(raw))
                  : _redis.xrange(key, minID, maxID, std::back_inserter(raw));
  if (ok)
  {
    swr::ItemStream<T> ret;
    swr::Item<T> retItem;
    for (const auto& rawItem : raw)
    {
      if (rawItem.second.count(DEFAULT_FIELD))
      {
        retItem.first = rawItem.first;
        retItem.second = default_field_value<T>(rawItem);
        ret.push_back(retItem);
      }
    }
    return ret;
  }
  return {};
}

template<typename T> swr::ItemStream<std::vector<T>>
RedisAdapter::get_fwd_data_list_helper(const std::string& baseKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (baseKey.size() ? baseKey : _baseKey) + ":DATA:" + subKey;
  bool ok = count ? _redis.xrange(key, minID, maxID, count, std::back_inserter(raw))
                  : _redis.xrange(key, minID, maxID, std::back_inserter(raw));
  if (ok)
  {
    swr::ItemStream<std::vector<T>> ret;
    swr::Item<std::vector<T>> retItem;
    for (const auto& rawItem : raw)
    {
      if (rawItem.second.count(DEFAULT_FIELD))
      {
        retItem.first = rawItem.first;
        const std::string& str = rawItem.second.at(DEFAULT_FIELD);
        retItem.second.assign(str.data(), str.data() + str.size());
        ret.push_back(retItem);
      }
    }
    return ret;
  }
  return {};
}

template <> inline swr::ItemStream<swr::Attrs>
RedisAdapter::get_rev_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& maxID, uint32_t count)
{
  swr::ItemStream<swr::Attrs> ret;
  std::string key = (baseKey.size() ? baseKey : _baseKey) + ":DATA:" + subKey;
  if (count) { _redis.xrevrange(key, maxID, "-", count, std::back_inserter(ret)); }
  else       { _redis.xrevrange(key, maxID, "-", std::back_inserter(ret)); }
  std::reverse(ret.begin(), ret.end());   //  reverse in place
  return ret;
}

template<typename T> swr::ItemStream<T>
RedisAdapter::get_rev_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (baseKey.size() ? baseKey : _baseKey) + ":DATA:" + subKey;
  bool ok = count ? _redis.xrevrange(key, maxID, "-", count, std::back_inserter(raw))
                  : _redis.xrevrange(key, maxID, "-", count, std::back_inserter(raw));
  if (ok)
  {
    swr::ItemStream<T> ret;
    swr::Item<T> retItem;
    for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
    {
      if (rawItem->second.count(DEFAULT_FIELD))
      {
        retItem.first = rawItem->first;
        retItem.second = default_field_value<T>(*rawItem);
        ret.push_back(retItem);
      }
    }
    return ret;
  }
  return {};
}

template<typename T> swr::ItemStream<std::vector<T>>
RedisAdapter::get_rev_data_list_helper(const std::string& baseKey, const std::string& subKey, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (baseKey.size() ? baseKey : _baseKey) + ":DATA:" + subKey;
  bool ok = count ? _redis.xrevrange(key, maxID, "-", count, std::back_inserter(raw))
                  : _redis.xrevrange(key, maxID, "-", std::back_inserter(raw));
  if (ok)
  {
    swr::ItemStream<std::vector<T>> ret;
    swr::Item<std::vector<T>> retItem;
    for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
    {
      if (rawItem->second.count(DEFAULT_FIELD))
      {
        retItem.first = rawItem->first;
        const std::string& str = rawItem->second.at(DEFAULT_FIELD);
        retItem.second.assign(str.data(), str.data() + str.size());
        ret.push_back(retItem);
      }
    }
    return ret;
  }  return {};
}

template<> inline std::string
RedisAdapter::add_data_helper(const std::string& subKey, const swr::Attrs& attrs, const std::string& id, uint32_t trim)
{
  std::string key = _baseKey + ":DATA:" + subKey;
  if (trim) { return _redis.xaddTrim(key, id, attrs.begin(), attrs.end(), trim); }
  else      { return _redis.xadd(key, id, attrs.begin(), attrs.end()); }
  return {};
}

template<typename T> std::string
RedisAdapter::add_data_helper(const std::string& subKey, const T& data, const std::string& id, uint32_t trim)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");
  std::string key = _baseKey + ":DATA:" + subKey;
  swr::Attrs attrs = default_field_attrs<T>(data);
  if (trim) { return _redis.xaddTrim(key, id, attrs.begin(), attrs.end(), trim); }
  else      { return _redis.xadd(key, id, attrs.begin(), attrs.end()); }
  return {};
}

template<typename T> std::vector<std::string>
RedisAdapter::add_data_list_helper(const std::string& subKey, const swr::ItemStream<T>& data, uint32_t trim)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  std::vector<std::string> ret;
  std::string key = _baseKey + ":DATA:" + subKey;
  for (const auto& item : data)
  {
    std::string id = item.first.size() ? item.first : "*";
    swr::Attrs attrs = {{ DEFAULT_FIELD, std::string((const char*)item.second.data(), item.second.size() * sizeof(T)) }};
    if (trim) { ret.push_back(_redis.xaddTrim(key, id, attrs.begin(), attrs.end(), trim)); }
    else      { ret.push_back(_redis.xadd(key, id, attrs.begin(), attrs.end())); }
  }
  return ret;
}

