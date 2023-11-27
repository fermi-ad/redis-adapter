/**
 * RedisAdapter.cpp
 *
 * This file contains the implementation of the RedisAdapter class.
 *
 * @author rsantucc
 */

#include "RedisAdapter.hpp"

#include <syslog.h>

using namespace sw::redis;
using namespace std;

//  Needed to add constructor with args for Redis/RedisCluster
//  initializations in RedisAdapter constructors below
struct RAConnOptions : public ConnectionOptions
{
  RAConnOptions(string host, int port) { this->host = host; this->port = port; }
};

//  Needed to add constructor with args for Redis/RedisCluster
//  initializations in RedisAdapter constructors below
struct RAConnPoolOptions : public ConnectionPoolOptions
{
  RAConnPoolOptions(size_t size) { this->size = size; }
};

template <>
RedisAdapter<Redis>::RedisAdapter(string key, string host, int port, size_t size)
: _redis(RAConnOptions(host, port), RAConnPoolOptions(size))
{
  initKeys(key);
}

template <>
RedisAdapter<RedisCluster>::RedisAdapter(string key, string host, int port, size_t size)
: _redis(RAConnOptions(host, port), RAConnPoolOptions(size))
{
  initKeys(key);
}

template <typename T>
void RedisAdapter<T>::initKeys(std::string baseKey)
{
  baseKey = "{" + baseKey + "}";  //  for cluster hashing

  _settingsKey  = baseKey + ":SETTINGS";
  _logKey       = baseKey + ":LOG";
  _commandsKey  = baseKey + ":COMMANDS";
  _statusKey    = baseKey + ":STATUS";
  _dataKey      = baseKey + ":DATA";
}

/*
* Get Device Config
*/
template <typename T>
void RedisAdapter<T>::setSetting(unordered_map<string, string> map)
{
  setHash(_settingsKey, map);
}

template <typename T>
unordered_map<string, string> RedisAdapter<T>::getSetting()
{
  return getHash(_settingsKey);
}

template <typename T>
Optional<string> RedisAdapter<T>::getValue(string key)
{
  return _redis.get(key);
}

template <typename T>
void RedisAdapter<T>::setValue(string key, string val)
{
  _redis.set(key,val);
}

template <typename T>
int RedisAdapter<T>::getUniqueValue(string key)
{
  return _redis.incr(key);
}

/*
* Hash get and set
*/
template <typename T>
unordered_map<string, string> RedisAdapter<T>::getHash(string key)
{
  unordered_map<string, string> m;
  _redis.hgetall(key, inserter(m, m.begin()));
  return m;
}

template <typename T>
void RedisAdapter<T>::setHash(string key, unordered_map<string, string> m)
{
  return _redis.hmset(key, m.begin(), m.end());
}

/*
* Set get and add member
*/
template <typename T>
unordered_set<string> RedisAdapter<T>::getSet(string key)
{
  unordered_set<string> set;
  try
  {
    _redis.smembers(key, inserter(set, set.begin()));
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
  return set;
}

template <typename T>
void RedisAdapter<T>::setSet(string key, string val)
{
  _redis.sadd(key,val);
}

/*
* Stream Functions
*/
// Redis Stream structure:
// element 1:
//   timestamp
//   fieldname1 fielddata1
//   fieldname2 fielddata2
// element 2:
//   timestamp
//   fieldname1 fielddata1

// Adds data to a redis stream at the key key
// timeID is the time that should be used as the time in the stream
// data is formated as a pair of strings the first is the element name and the second is the data at that element
template <typename T>
void RedisAdapter<T>::streamWrite(vector<pair<string,string>> data, string timeID , string key, uint trim )
{
  try
  {
    auto replies = _redis.xadd(key, timeID, data.begin(), data.end());
    if (trim)
    {
      streamTrim(key, trim);
    }
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
}

template <typename T>
void RedisAdapter<T>::streamWriteOneField(const std::string& data, const std::string& timeID, const std::string& key, const std::string& field, uint trim)
{
  // Single element vector formated the way that streamWrite wants it.
  std::vector<std::pair<std::string, std::string>> wrapperVector = {{ field, data }};
  // When you give * as your time in redis the server generates the timestamp for you. Here we do the same if timeID is empty.
  if (0 == timeID.length()) { streamWrite(wrapperVector,    "*", key, trim); }
  else                      { streamWrite(wrapperVector, timeID, key, trim); }
}

template <typename T>
void RedisAdapter<T>::streamReadBlock(unordered_map<string,string>& keysID, Streams& dest)
{
  try
  {
    _redis.xread(keysID.begin(), keysID.end(), chrono::seconds(0), 10, inserter(dest, dest.end()));
    // Update the time of last message, multiple different streams can return at once, default times use "$"
    // this is equivalent to saying start from where the stream is now. Listen for only new input.
    // We will get the newest time from the last element in an ItemStream
    // Messages are returned simlar to tail -f, newest message is last. Update last time
    for (auto val : dest)
    {
      if (!val.second.empty())
      {
        keysID[val.first] = val.second.back().first;
      }
    }
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
}

template <typename T>
void RedisAdapter<T>::streamRead(string key, int count, ItemStream& dest)
{
  try
  {
    _redis.xrevrange(key, "+", "-", count, back_inserter(dest));
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
}

template <typename T>
void RedisAdapter<T>::streamRead(string key, string time, int count, ItemStream& dest)
{
  try
  {
    _redis.xrevrange(key, "+", time, count, back_inserter(dest));
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
}

template <typename T>
void RedisAdapter<T>::streamRead(string key, string timeA, string timeB, ItemStream& dest)
{
  try
  {
    _redis.xrevrange(key, timeB, timeA, back_inserter(dest));
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
}

template <typename T>
void RedisAdapter<T>::streamRead(string key, string time, int count, vector<float>& dest)
{
  try
  {
    ItemStream result;
    streamRead(key, time, 1, result);
    for (Item data : result)
    {
      string timeID = data.first;
      for (auto val : data.second)
      {
        // If we have an element named data
        if (val.first.compare("DATA") == 0)
        {
          dest.resize(val.second.length() / sizeof(float));
          memcpy(dest.data(), val.second.data(), val.second.length());
        }
      }
    }
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
}

template <typename T>
void RedisAdapter<T>::logWrite(string key, string msg, string source)
{
  vector<pair<string,string>> data;
  data.emplace_back(make_pair(source,msg));
  streamWrite(data, "*", key, 1000);
}

template <typename T>
ItemStream RedisAdapter<T>::logRead(uint count)
{
  ItemStream is;
  try
  {
    _redis.xrevrange(_logKey, "+", "-", count, back_inserter(is));
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
  return is;
}

template <typename T>
void RedisAdapter<T>::streamTrim(string key, int size)
{
  try
  {
    _redis.xtrim(key, size, false);
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
}

template <typename T>
void RedisAdapter<T>::publish(string msg)
{
  try
  {
    _redis.publish(_commandsKey, msg);
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
}

template <typename T>
void RedisAdapter<T>::publish(string key, string msg)
{
  try
  {
    _redis.publish(_commandsKey + ":" + key, msg);
  }
  catch (const exception &e)
  {
    //  TODO: handle exceptions
  }
}

inline bool const StringToBool(string const& s)
{
  return s != "0";
}

template <typename T>
bool RedisAdapter<T>::getDeviceStatus()
{
  return StringToBool(getValue(_statusKey).value());
}

template <typename T>
void RedisAdapter<T>::setDeviceStatus(bool status)
{
  setValue(_statusKey, to_string((int)status));
}

template <>
void RedisAdapter<Redis>::copyKey( string src, string dest, bool data)
{
  _redis.command<void>("copy", src, dest);
}

template <>
void RedisAdapter<RedisCluster>::copyKey( string src, string dest, bool data)
{
  _redis.command<void>("copy", src, dest);
}

template <typename T>
void RedisAdapter<T>::deleteKey( string key )
{
  _redis.del(key);
}

template <>
vector<string> RedisAdapter<Redis>::getServerTime()
{
  vector<string> result;
  _redis.command("time", back_inserter(result));
  return result;
}

template <>
vector<string> RedisAdapter<RedisCluster>::getServerTime()
{
  vector<string> result;
  _redis.redis("hash-tag", false).command("time", back_inserter(result));
  return result;
}

template <typename T>
void RedisAdapter<T>::psubscribe(string pattern, function<void(string, string, string)> func)
{
  _patternSubscriptions.push_back({ .pattern = pattern, .function = func });
}

template <typename T>
void RedisAdapter<T>::subscribe(string channel, function<void(string, string)> func)
{
  _subscriptions.push_back({ .key = channel, .function = func });
}

template <typename T>
void RedisAdapter<T>::startListener()
{
  _listener = thread(&RedisAdapter::listener, this);
}
template <typename T>
void RedisAdapter<T>::startReader()
{
  _reader = thread(&RedisAdapter::reader, this);
}

template <typename T>
void RedisAdapter<T>::registerCommand(string command, function<void(string, string)> func)
{
  _commands.emplace(_commandsKey + ":" + command, func);
}

template <typename T>
void RedisAdapter<T>::listener()
{
  // Consume messages in a loop.
  bool flag = false;
  Subscriber _sub = _redis.subscriber();
  while (true)
  {
    try
    {
      if (flag)
      {
        _sub.consume();
      }
      else
      {
        flag = true;
        _sub.on_pmessage([&](string pattern, string key, string msg)
        {
          auto search = _commands.find(key);
          if (search != _commands.end())
          {
            search->second(key, msg);
          }
          else
          {
            vector<patternFunctionPair> matchingPatterns;
            for (patternFunctionPair patternSubscription : _patternSubscriptions)
            {
              if (patternSubscription.pattern == pattern)
              {
                matchingPatterns.push_back(patternSubscription);
              }
            }
            // Loop over the members of _patternSubscriptions
            // that have the same pattern as this event
            for (patternFunctionPair patternFunction : matchingPatterns)
            {
              patternFunction.function(pattern, key, msg);
            }
          }
        });
        _sub.on_message([&](string key, string msg)
        {
          vector<keyFunctionPair> matchingSubscriptions;
          for (keyFunctionPair subscription : _subscriptions)
          {
            if (subscription.key == key)
            {
              matchingSubscriptions.push_back(subscription);
            }
          }
          // Loop over the members of _subscriptions
          // that have the same key as this event
          for (keyFunctionPair keyFunction : matchingSubscriptions)
          {
            keyFunction.function(key, msg);
          }
        });
        // The default is everything published on ChannelKey
        _sub.psubscribe(_commandsKey + "*");
        // Subscribe to the pattens in _patternSubscriptions
        for (auto element : _patternSubscriptions)
        {
          _sub.psubscribe(element.pattern);
        }
        // Subscribe to the keys in _subscriptions
        for (auto element : _subscriptions)
        {
          _sub.subscribe(element.key);
        }
      }
    }
    catch (const TimeoutError &e)
    {
      continue;
    }
    catch (const exception &e)
    {
      // Handle unrecoverable exceptions. Need to re create redis connection
      syslog(LOG_ERR, "ERROR %s occured, trying to recover", e.what());
      flag = false;
      _sub = _redis.subscriber();
      continue;
    }
  }
}

template <typename T>
void RedisAdapter<T>::addReader(string streamKey,  function<void(std::string, ItemStream)> func)
{
  _streamKeyID.emplace(streamKey, "$");
  _streamSubscriptions.push_back({ .streamKey = streamKey, .function = func});
}

template <typename T>
void RedisAdapter<T>::reader()
{
  // Create a new redis connection only used for streams
  Streams streamsBuffer;
  while (true)
  {
    try
    {
      streamsBuffer.clear();
      streamReadBlock(_streamKeyID, streamsBuffer);
      // iterate thorugh the buffer and pass onto the correct handler
      for (auto is : streamsBuffer)
      {
        for (streamKeyFunctionPair streamSubscription : _streamSubscriptions)
        {
          if (streamSubscription.streamKey == is.first)
          {
            streamSubscription.function(streamSubscription.streamKey, is.second);
          }
        }
      }
    }
    catch (const exception &e)
    {
      // Handle unrecoverable exceptions. Need to re create redis connection
      syslog(LOG_ERR, "ERROR %s occured, trying to recover", e.what());
      continue;
    }
  }
}

template <typename T>
Optional<timespec> RedisAdapter<T>::getServerTimespec()
{
  vector<string> result = getServerTime();
  // The redis command time is returns an array with the first element being
  // the time in seconds and the second being the microseconds within that second
  if (result.size() != 2) { return nullopt; }
  timespec ts;
  ts.tv_sec  = stoll(result.at(0));        // first element contains unix time
  ts.tv_nsec = stoll(result.at(1)) * 1000; // second element contains microseconds in the second
  return ts;
}
