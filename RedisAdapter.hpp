/**
 * RedisAdapter.hpp
 *
 * This file contains the definition of the redis Adapter class.
 *
 * @author rsantucc
 */

#ifndef RedisAdapter_HPP
#define RedisAdapter_HPP

#if __cplusplus >= 202002L
#define CPLUSPLUS20_SUPPORTED
#endif

#include "IRedisAdapter.hpp"

#if defined(CPLUSPLUS20_SUPPORTED)
#include <ranges>
#endif

#include <thread>

/**
 * RedisAdapter
 */
template <typename T_REDIS>
class RedisAdapter: public IRedisAdapter
{
public:
  /* Constructor / Destructor */
  RedisAdapter(std::string key, std::string = "tcp://127.0.0.1:6379");
  RedisAdapter(const RedisAdapter& ra);

  /* Wrapper Functions */
  virtual std::vector<std::string> getDevices();
  virtual void clearDevices(std::string devicelist);
  virtual void setDeviceConfig(std::unordered_map<std::string, std::string> map);
  virtual std::unordered_map<std::string, std::string> getDeviceConfig();
  virtual void setDevice(std::string name);

  /*
   * Single Value Functions
   * Note: These use the config connection
   */
  virtual sw::redis::Optional<std::string> getValue(std::string key);
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
  virtual void streamWrite(std::vector<std::pair<std::string,std::string>> data, std::string timeID, std::string key, uint trim = 0);

  void streamWriteOneField(const std::string& data, const std::string& timeID, const std::string& key, const std::string& field)
  {
    // Single element vector formated the way that streamWrite wants it.
    std::vector<std::pair<std::string, std::string>> wrapperVector = { {field, data }};
    // When you give * as your time in redis the server generates the timestamp for you. Here we do the same if timeID is empty.
    if (0 == timeID.length()) { streamWrite(wrapperVector,    "*", key, false); }
    else                      { streamWrite(wrapperVector, timeID, key, false); }
  }

  #if defined(CPLUSPLUS20_SUPPORTED)
  // Simplified version of streamWrite when you only have one element in the item you want to add to the stream, and you have binary data.
  // When this is called an element is appended to the stream named 'key' that has one field named 'field' with the value data in binary form.
  // This is in the header to make it compile, if you move this to the source file, then it causes really wierd linker errors.
  // @todo Consider performing host to network conversion for data compatibility.
  static_assert(BYTE_ORDER == __LITTLE_ENDIAN); // Arm and x86 use the same byte order. If this ever fails we should look into this problem.
  template <std::ranges::input_range T_RANGE>
  void streamWriteOneFieldRange(T_RANGE&& data, const std::string& timeID, const std::string& key, const std::string& field)
  {
    // Copy data from the caller to a string so that it can be used by the redis++ API
    std::string_view view((char *)data.data(), data.size() * sizeof(*data.begin()));
    std::string temp(view);
    streamWriteOneField(temp, timeID, key, field);
  }
  #endif

  void streamReadBlock(T_REDIS& redisConnection, std::unordered_map<std::string,std::string>& keysID, Streams& result);

  virtual void streamRead(std::string key, std::string time, int count, std::vector<float>& result);
  virtual void streamRead(std::string key, std::string time, int count, ItemStream& dest);
  virtual void streamRead(std::string key, int count, ItemStream& dest);
  virtual void streamRead(std::string key, std::string timeA, std::string timeB, ItemStream& dest);
  virtual void streamTrim(std::string key, int size);
  virtual IRedisAdapter::ItemStream logRead(uint count);
  virtual void logWrite(std::string key, std::string msg, std::string source);

  // Read a single field from the element at desiredTime and return the actual time.
  // If this fails then return an empty optional
  template <typename T_VAL>
  sw::redis::Optional<std::string> streamReadOneField(std::string key, std::string desiredTime, std::string field, std::vector<T_VAL>& dest)
  {
    ItemStream result;
    //streamRead(key,desiredTime, 1, result);
    streamRead(key, 1, result);
    if (0 == result.size()) { return std::nullopt; }
    sw::redis::Optional<std::string> time = result.at(0).first;
    Attrs attributes = result.at(0).second;
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
  virtual void addReader(std::string streamKey,  std::function<void(ItemStream)> f);

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
  virtual sw::redis::Optional<timespec> getServerTimespec();

  /*
   * Device Status
   */
  virtual bool getDeviceStatus();
  virtual void setDeviceStatus(bool status = true);

  /* Key getters and setters */
  virtual std::string getBaseKey() const { return _baseKey; }
  virtual void setBaseKey(std::string baseKey) { _baseKey = baseKey; }

  virtual std::string getChannelKey() const { return _channelKey; }
  virtual void setChannelKey(std::string channelKey) { _channelKey = channelKey; }

  virtual  std::string getConfigKey() const { return _configKey; }
  virtual void setConfigKey(std::string configKey) { _configKey = configKey; }

  virtual std::string getLogKey() const { return _logKey; }
  virtual void setLogKey(std::string logKey) { _logKey = logKey; }

  virtual std::string getStatusKey() const { return _statusKey; }
  virtual void setStatusKey(std::string statusKey) { _statusKey = statusKey; }

  virtual std::string getTimeKey()  const  { return _timeKey; }
  virtual void setTimeKey(std::string timeKey) { _timeKey = timeKey; }

  virtual std::string getDeviceKey()  const {return _deviceKey; }
  virtual void setDeviceKey(std::string deviceKey) { _deviceKey = deviceKey; }

  virtual std::string getDataKey() const { return _dataKey; }
  virtual void setDataKey(std::string dataKey) { _dataKey = dataKey; }
  virtual std::string getDataSubKey(std::string subKey) const { return _dataKey + ":" + subKey; }

  virtual std::string getAbortKey() const { return _abortKey; }
  virtual void setAbortKey(std::string abortKey) { _abortKey = abortKey; }

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
      std::function<void(ItemStream)> function;
  };
  std::vector<streamKeyFunctionPair> _streamSubscriptions;

  std::unordered_map<std::string, std::string> _streamKeyID;

  std::string _connection;

  std::string _baseKey;
  std::string _configKey;
  std::string _logKey;
  std::string _channelKey;
  std::string _statusKey;
  std::string _timeKey;
  std::string _deviceKey;
  std::string _dataKey;
  std::string _abortKey;
};

using RedisAdapterSingle = RedisAdapter<sw::redis::Redis>;
using RedisAdapterCluster = RedisAdapter<sw::redis::RedisCluster>;

#endif
