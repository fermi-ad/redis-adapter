/**
 * RedisAdapter.hpp
 *
 * This file contains the definition of the redis Adapter class.
 *
 * @author rsantucc
 */

#pragma once

#include "RedisConnection.h"

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

namespace swr = sw::redis;

/**
 * RedisAdapter
 */
class RedisAdapter
{
public:
  /* Constructors */
  RedisAdapter(const std::string& baseKey, const std::string& host = "", uint16_t port = 0);
  RedisAdapter(const std::string& baseKey, const RedisConnection::Options& opts);

  RedisAdapter(const RedisAdapter& ra) = delete;
  RedisAdapter& operator=(const RedisAdapter& ra) = delete;

  std::string getStatus(const std::string& subKey);
  std::string getForeignStatus(const std::string& baseKey, const std::string& subKey);

  bool setStatus(const std::string& subKey, const std::string& value);

  swr::ItemStream<std::string> getLog(std::string minID, std::string maxID = "+");
  swr::ItemStream<std::string> getLogAfter(std::string minID, uint32_t count);
  swr::ItemStream<std::string> getLogBefore(std::string maxID, uint32_t count = 1);

  bool addLog(std::string message, uint32_t trim = 1000);

  template<typename T> T getSetting(const std::string& subKey);
  template<typename T> std::vector<T> getSettingList(const std::string& subKey);

  template<typename T> T getForeignSetting(const std::string& baseKey, const std::string& subKey);
  template<typename T> std::vector<T> getForeignSettingList(const std::string& baseKey, const std::string& subKey);

  template<typename T> bool setSetting(const std::string& subKey, const T& value);
  template<typename T> bool setSettingList(const std::string& subKey, const std::vector<T>& value);

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

  /*
   * Publish / Subscribe Functions
   */
  void publish(std::string msg);
  void publish(std::string key, std::string msg);

  void psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> f);
  void subscribe(std::string channel, std::function<void(std::string,std::string)> f);

  void registerCommand(std::string command, std::function<void(std::string, std::string)> f);
  void addReader(std::string streamKey,  std::function<void(std::string, swr::ItemStream<swr::Attrs>)> f);

  /*
   * Copy & Delete Functions
   */
  void copyKey(std::string src, std::string dst);
  void deleteKey(std::string key);

  /*
   * Time
   */
  virtual std::vector<std::string> getServerTime();
  virtual swr::Optional<timespec> getServerTimespec();

  void startListener();
  void startReader();

private:
  const std::string DEFAULT_FIELD = "_";

  RedisConnection _redis;

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

  static const uint defaultTrimSize = 1;
};

using RedisAdapterSingle = RedisAdapter;
using RedisAdapterCluster = RedisAdapter;

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

template <> inline swr::ItemStream<std::string>
RedisAdapter::get_fwd_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count)
{
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (baseKey.size() ? baseKey : _baseKey) + ":DATA:" + subKey;
  if (_redis.xrange(key, minID, maxID, count, std::back_inserter(raw)))
  {
    swr::ItemStream<std::string> ret;
    swr::Item<std::string> retItem;
    for (const auto& rawItem : raw)
    {
      if (rawItem.second.count(DEFAULT_FIELD))
      {
        retItem.first = rawItem.first;
        retItem.second = rawItem.second.at(DEFAULT_FIELD);
        ret.push_back(retItem);
      }
    }
    return ret;
  }
  return {};
}

template <> inline swr::ItemStream<swr::Attrs>
RedisAdapter::get_fwd_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count)
{
  swr::ItemStream<swr::Attrs> ret;
  std::string key = (baseKey.size() ? baseKey : _baseKey) + ":DATA:" + subKey;
  _redis.xrange(key, minID, maxID, count, std::back_inserter(ret));
  return ret;
}

template<typename T> swr::ItemStream<T>
RedisAdapter::get_fwd_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& minID, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  swr::ItemStream<swr::Attrs> raw;
  std::string key = (baseKey.size() ? baseKey : _baseKey) + ":DATA:" + subKey;
  if (_redis.xrange(key, minID, maxID, count, std::back_inserter(raw)))
  {
    swr::ItemStream<T> ret;
    swr::Item<T> retItem;
    for (const auto& rawItem : raw)
    {
      if (rawItem.second.count(DEFAULT_FIELD))
      {
        retItem.first = rawItem.first;
        retItem.second = *(T*)rawItem.second.at(DEFAULT_FIELD).data();
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
  return {};
}

template<typename T> swr::ItemStream<T>
RedisAdapter::get_rev_data_helper(const std::string& baseKey, const std::string& subKey, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>() || std::is_same<T, swr::Attrs>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStream<std::vector<T>>
RedisAdapter::get_rev_data_list_helper(const std::string& baseKey, const std::string& subKey, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}

template<typename T> std::string
RedisAdapter::add_data_helper(const std::string& subKey, const T& data, const std::string& id, uint32_t trim)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>() || std::is_same<T, swr::Attrs>(), "wrong type T");
  return {};
}

template<typename T> std::vector<std::string>
RedisAdapter::add_data_list_helper(const std::string& subKey, const swr::ItemStream<T>& data, uint32_t trim)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}
