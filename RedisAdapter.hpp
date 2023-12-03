/**
 * RedisAdapter.hpp
 *
 * This file contains the definition of the redis Adapter class.
 *
 * @author rsantucc
 */
#pragma once

#if __cplusplus >= 202002L
#define CPLUSPLUS20_SUPPORTED
#endif

#include "RedisConnection.h"

#if defined(CPLUSPLUS20_SUPPORTED)
#include <ranges>
#endif

namespace sw::redis
{
  using Attrs = std::unordered_map<std::string, std::string>;
  using Item = std::pair<std::string, Attrs>;
  using ItemStream = std::vector<Item>;
  using Streams = std::unordered_map<std::string, ItemStream>;

  template <typename T> using ItemT = std::pair<std::string, T>;
  template <typename T> using ItemStreamT = std::vector<ItemT<T>>;
  template <typename T> using StreamsT = std::unordered_map<std::string, ItemStreamT<T>>;
};

namespace swr = sw::redis;

/**
 * RedisAdapter
 */
class RedisAdapter
{
public:
  /* Constructor / Destructor */
  RedisAdapter(const std::string& baseKey, const std::string& host = "", uint16_t port = 0, uint16_t size = 0);

  RedisAdapter(const RedisAdapter& ra) = delete;
  RedisAdapter& operator=(const RedisAdapter& ra) = delete;


  std::string getStatus(const std::string& subKey);
  std::string getForeignStatus(const std::string& baseKey, const std::string& subKey);

  bool setStatus(const std::string& subkey, const std::string& value);


  swr::ItemStreamT<std::string> getLog(std::string minID, std::string maxID = "+");
  swr::ItemStreamT<std::string> getLogAfter(std::string minID, uint32_t count);
  swr::ItemStreamT<std::string> getLogBefore(std::string maxID, uint32_t count = 1);

  bool addLog(std::string message, uint32_t trim = 1000);


  template<typename T> T getSetting(const std::string& subKey);
  template<typename T> std::vector<T> getSettingList(const std::string& subKey);

  template<typename T> T getForeignSetting(const std::string& baseKey, const std::string& subKey);
  template<typename T> std::vector<T> getForeignSettingList(const std::string& baseKey, const std::string& subKey);

  template<typename T> bool setSetting(const std::string& subKey, const T& value);
  template<typename T> bool setSettingList(const std::string& subKey, const std::vector<T>& value);


  template<typename T> swr::ItemStreamT<T> getData(const std::string& subkey, const std::string& minID, const std::string& maxID);
  template<typename T> swr::ItemStreamT<std::vector<T>> getDataList(const std::string& subkey, const std::string& minID, const std::string& maxID);

  template<typename T> swr::ItemStreamT<T> getForeignData(const std::string& baseKey, const std::string& subkey, const std::string& minID, const std::string& maxID);
  template<typename T> swr::ItemStreamT<std::vector<T>> getForeignDataList(const std::string& baseKey, const std::string& subkey, const std::string& minID, const std::string& maxID);

  template<typename T> swr::ItemStreamT<T> getDataBefore(const std::string& subkey, const std::string& maxID = "+", uint32_t count = 1);
  template<typename T> swr::ItemStreamT<std::vector<T>> getDataListBefore(const std::string& subkey, const std::string& maxID = "+", uint32_t count = 1);

  template<typename T> swr::ItemStreamT<T> getForeignDataBefore(const std::string& baseKey, const std::string& subkey, const std::string& maxID = "+", uint32_t count = 1);
  template<typename T> swr::ItemStreamT<std::vector<T>> getForeignDataListBefore(const std::string& baseKey, const std::string& subkey, const std::string& maxID = "+", uint32_t count = 1);

  template<typename T> swr::ItemStreamT<T> getDataAfter(const std::string& subkey, const std::string& minID = "-", uint32_t count = 1);
  template<typename T> swr::ItemStreamT<std::vector<T>> getDataListAfter(const std::string& subkey, const std::string& minID = "-", uint32_t count = 1);

  template<typename T> swr::ItemStreamT<T> getForeignDataAfter(const std::string& baseKey, const std::string& subkey, const std::string& minID = "-", uint32_t count = 1);
  template<typename T> swr::ItemStreamT<std::vector<T>> getForeignDataListAfter(const std::string& baseKey, const std::string& subkey, const std::string& minID = "-", uint32_t count = 1);

  template<typename T> std::string addData(const std::string& subKey, const T& data, const std::string& id = "*", uint32_t trim = 1);
  template<typename T> std::vector<std::string> addDataList(const std::string& subKey, const swr::ItemStreamT<T>& data, uint32_t trim = 1);


  /*
   * Stream Functions
   * Note: All stream functions use the cluster connection.
   */
  void streamWrite(std::vector<std::pair<std::string,std::string>> data, std::string timeID, std::string key, uint trim = defaultTrimSize);

  void streamWriteOneField(const std::string& data, const std::string& timeID, const std::string& key, const std::string& field, uint trim = defaultTrimSize);

  #if defined(CPLUSPLUS20_SUPPORTED)
  // Simplified version of streamWrite when you only have one element in the item you want to add to the stream, and you have binary data.
  // When this is called an element is appended to the stream named 'key' that has one field named 'field' with the value data in binary form.
  // This is in the header to make it compile, if you move this to the source file, then it causes really wierd linker errors.
  // @todo Consider performing host to network conversion for data compatibility.
  static_assert(BYTE_ORDER == __LITTLE_ENDIAN); // Arm and x86 use the same byte order. If this ever fails we should look into this problem.
  template <std::ranges::input_range T_RANGE>
  void streamWriteOneFieldRange(T_RANGE&& data, const std::string& timeID, const std::string& key, const std::string& field, uint trim = defaultTrimSize)
  {
    // Copy data from the caller to a string so that it can be used by the redis++ API
    std::string_view view((char *)data.data(), data.size() * sizeof(*data.begin()));
    std::string temp(view);
    streamWriteOneField(temp, timeID, key, field, trim);
  }
  #endif

  void streamReadBlock(std::unordered_map<std::string,std::string>& keysID, swr::Streams& result);

  void streamRead(std::string key, std::string time, int count, std::vector<float>& result);
  void streamRead(std::string key, std::string time, int count, swr::ItemStream& dest);
  void streamRead(std::string key, int count, swr::ItemStream& dest);
  void streamRead(std::string key, std::string timeA, std::string timeB, swr::ItemStream& dest);
  void streamTrim(std::string key, int size);
  swr::ItemStream logRead(uint count);
  void logWrite(std::string key, std::string msg, std::string source);

  // Read a single field from the element at desiredTime and return the actual time.
  // If this fails then return an empty optional
  template <typename T_VAL>
  swr::Optional<std::string> streamReadOneField(std::string key, std::string desiredTime, std::string field, std::vector<T_VAL>& dest)
  {
    swr::ItemStream result;
    //streamRead(key,desiredTime, 1, result);
    streamRead(key, 1, result);
    if (0 == result.size()) { return std::nullopt; }
    swr::Optional<std::string> time = result.at(0).first;
    swr::Attrs attributes = result.at(0).second;
    // Find the field named field or return an empty optional
    auto fieldPointer = attributes.find(field);
    if (fieldPointer == attributes.end()) // if the field isn't in the item in the stream
    {
      time.reset();
      return time;
    }
    std::string& str = fieldPointer->second;
    dest.resize(str.length() / sizeof(T_VAL));
    memcpy(dest.data(), str.c_str(), str.length());
    return time;
  }

  /*
   * Publish / Subscribe Functions
   */
  void publish(std::string msg);
  void publish(std::string key, std::string msg);
  void psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> f);
  void subscribe(std::string channel, std::function<void(std::string,std::string)> f);
  void registerCommand(std::string command, std::function<void(std::string, std::string)> f);
  void addReader(std::string streamKey,  std::function<void(std::string, swr::ItemStream)> f);

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
  RedisConnection _redis;

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
      std::function<void(std::string, swr::ItemStream)> function;
  };
  std::vector<streamKeyFunctionPair> _streamSubscriptions;

  std::unordered_map<std::string, std::string> _streamKeyID;

  std::string _settingsKey;
  std::string _logKey;
  std::string _commandsKey;
  std::string _statusKey;
  std::string _dataKey;

  static const uint defaultTrimSize = 1;
};


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

template<typename T> swr::ItemStreamT<T> RedisAdapter::getData(const std::string& subkey, const std::string& minID, const std::string& maxID)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>() || std::is_same<T, swr::Attrs>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<std::vector<T>> RedisAdapter::getDataList(const std::string& subkey, const std::string& minID, const std::string& maxID)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<T> RedisAdapter::getForeignData(const std::string& basekey, const std::string& subkey, const std::string& minID, const std::string& maxID)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>() || std::is_same<T, swr::Attrs>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<std::vector<T>> RedisAdapter::getForeignDataList(const std::string& basekey, const std::string& subkey, const std::string& minID, const std::string& maxID)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<T> getDataBefore(const std::string& subkey, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>() || std::is_same<T, swr::Attrs>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<std::vector<T>> getDataListBefore(const std::string& subkey, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<T> getForeignDataBefore(const std::string& baseKey, const std::string& subkey, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>() || std::is_same<T, swr::Attrs>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<std::vector<T>> getForeignDataListBefore(const std::string& baseKey, const std::string& subkey, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<T> getDataAfter(const std::string& subkey, const std::string& minID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>() || std::is_same<T, swr::Attrs>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<std::vector<T>> getDataListAfter(const std::string& subkey, const std::string& minID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<T> getForeignDataAfter(const std::string& baseKey, const std::string& subkey, const std::string& minID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>() || std::is_same<T, swr::Attrs>(), "wrong type T");
  return {};
}

template<typename T> swr::ItemStreamT<std::vector<T>> getForeignDataListAfter(const std::string& baseKey, const std::string& subkey, const std::string& minID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");
  return {};
}

template<typename T> std::string addData(const std::string& subKey, const T& data, const std::string& id, uint32_t trim)
{
  return {};
}

template<typename T> std::vector<std::string> addDataList(const std::string& subKey, const swr::ItemStreamT<T>& data, uint32_t trim)
{
  return {};
}
