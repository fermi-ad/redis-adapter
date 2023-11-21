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

#include <sw/redis++/redis++.h>

#if defined(CPLUSPLUS20_SUPPORTED)
#include <ranges>
#endif

namespace sw::redis
{
  using Attrs = std::unordered_map<std::string, std::string>;
  using Item = std::pair<std::string, Attrs>;
  using ItemStream = std::vector<Item>;
  using Streams =  std::unordered_map<std::string, ItemStream>;
};

namespace swr = sw::redis;

/**
 * RedisAdapter
 *
 * Methods are marked 'virtual' to force linker to include them?
 * I don't understand why this is happening, but the test.cpp
 * app fails to link any of the methods not marked 'virtual'.
 * Any other solution would be welcome.
 *
 */
template <typename T_REDIS>
class RedisAdapter
{
public:
  /* Constructor / Destructor */
  RedisAdapter(std::string key, std::string host = "127.0.0.1", int port = 6379, size_t size = 5);

  RedisAdapter(const RedisAdapter& ra) = delete;
  RedisAdapter& operator=(const RedisAdapter& ra) = delete;

  /* Wrapper Functions */
  virtual void setDeviceConfig(std::unordered_map<std::string, std::string> map);
  virtual std::unordered_map<std::string, std::string> getDeviceConfig();

  /*
   * Single Value Functions
   * Note: These use the config connection
   */
  virtual swr::Optional<std::string> getValue(std::string key);
  virtual void setValue(std::string key, std::string val);
  virtual int getUniqueValue(std::string key);
  virtual std::unordered_map<std::string, std::string> getHash(std::string key);
  virtual void setHash(std::string key, std::unordered_map<std::string, std::string> m);
  virtual std::unordered_set<std::string> getSet(std::string key);
  virtual void setSet(std::string key, std::string val);

  /*
   * Stream Functions
   * Note: All stream functions use the cluster connection.
   *       logRead and logWrite are stream functions, but use the config connection
   */
  virtual void streamWrite(std::vector<std::pair<std::string,std::string>> data, std::string timeID, std::string key, uint trim = defaultTrimSize);

  virtual void streamWriteOneField(const std::string& data, const std::string& timeID, const std::string& key, const std::string& field, uint trim = defaultTrimSize);

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

  virtual void streamReadBlock(std::unordered_map<std::string,std::string>& keysID, swr::Streams& result);

  virtual void streamRead(std::string key, std::string time, int count, std::vector<float>& result);
  virtual void streamRead(std::string key, std::string time, int count, swr::ItemStream& dest);
  virtual void streamRead(std::string key, int count, swr::ItemStream& dest);
  virtual void streamRead(std::string key, std::string timeA, std::string timeB, swr::ItemStream& dest);
  virtual void streamTrim(std::string key, int size);
  virtual swr::ItemStream logRead(uint count);
  virtual void logWrite(std::string key, std::string msg, std::string source);

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
   * Note: All publish / subscribe functions use the config connection
   */
  virtual void publish(std::string msg);
  virtual void publish(std::string key, std::string msg);
  virtual void psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> f);
  virtual void subscribe(std::string channel, std::function<void(std::string,std::string)> f);
  virtual void registerCommand(std::string command, std::function<void(std::string, std::string)> f);
  virtual void addReader(std::string streamKey,  std::function<void(std::string, swr::ItemStream)> f);

  /*
   * Copy & Delete Functions
   */
  virtual void copyKey(std::string src, std::string dest, bool data = false);
  virtual void deleteKey(std::string key);

  /*
   *  Abort Flag
   */
  virtual void setAbortFlag(bool flag = false);
  virtual bool getAbortFlag();

  /*
   * Time
   */
  virtual std::vector<std::string> getServerTime();
  virtual swr::Optional<timespec> getServerTimespec();

  /*
   * Device Status
   */
  virtual bool getDeviceStatus();
  virtual void setDeviceStatus(bool status = true);

  virtual void startListener();
  virtual void startReader();

  T_REDIS _redis;

private:
  void initKeys(std::string baseKey);

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

using RedisAdapterSingle = RedisAdapter<swr::Redis>;
using RedisAdapterCluster = RedisAdapter<swr::RedisCluster>;
