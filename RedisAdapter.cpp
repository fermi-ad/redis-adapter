/**
 * RedisAdapter.cpp
 *
 * This file contains the implementation of the RedisAdapter class.
 *
 * @author rsantucc
 */

#include "RedisAdapter.hpp"

using namespace sw::redis;
using namespace std;

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RedisAdapter : constructor
//
//    baseKey : base key of home device
//    opts    : struct of default values, override using per-field initializer list
//              e.g. RedisConnection::Options{ .user = "adinst", .password = "adinst" }
//    return  : RedisAdapter
//
RedisAdapter::RedisAdapter(const string& baseKey, const RedisConnection::Options& opts)
: _redis(opts), _baseKey(baseKey)
{
  _commandsKey  = baseKey + COMMANDS_STUB;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RedisAdapter : constructor
//
//    baseKey : base key of home device
//    host    : IP address of server "w.x.y.z"
//    port    : port server is listening on
//    return  : RedisAdapter
//
RedisAdapter::RedisAdapter(const string& baseKey, const string& host, uint16_t port)
: RedisAdapter(baseKey, RedisConnection::Options{ .host = host, .port = port }) {}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getStatus : get status for home device as string
//
//    subKey : sub key to get status from
//    return : string with status if successful
//             empty string if failure
//
string RedisAdapter::getStatus(const string& subKey)
{
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrevrange(_baseKey + STATUS_STUB + subKey, "+", "-", 1, back_inserter(raw));
  if (raw.size()) { return default_field_value<string>(raw.front().second); }
  return {};
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getForeignStatus : get status for foreign device as string
//
//    foreignKey : base key of foreign device
//    subKey     : sub key to get status from
//    return     : string with status if successful
//                 empty string if failure
//
string RedisAdapter::getForeignStatus(const string& foreignKey, const string& subKey)
{
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrevrange(foreignKey + STATUS_STUB + subKey, "+", "-", 1, back_inserter(raw));
  if (raw.size()) { return default_field_value<string>(raw.front().second); }
  return {};
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  setStatus : set status for home device as string
//
//    subKey : sub key to set status on
//    value  : status value to set
//    return : true on success, false on failure
//
bool RedisAdapter::setStatus(const string& subKey, const string& value)
{
  swr::Attrs attrs = default_field_attrs(value);
  return _redis.xaddTrim(_baseKey + STATUS_STUB + subKey, "*", attrs.begin(), attrs.end(), 1).size();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getLog : get log for home device between specified times
//
//    minID  : lowest time to get log for
//    maxID  : highest time to get log for
//    return : ItemStream of Item<string>
//
ItemStream<string> RedisAdapter::getLog(string minID, string maxID)
{
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrange(_baseKey + LOG_STUB, minID, maxID, back_inserter(raw));
  swr::ItemStream<string> ret;
  swr::Item<string> retItem;
  for (const auto& rawItem : raw)
  {
    retItem.second = default_field_value<string>(rawItem.second);
    if (retItem.second.size())
    {
      retItem.first = rawItem.first;
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getLogAfter : get log for home device after specified time
//
//    minID  : lowest time to get log for
//    count  : greatest number of log items to get
//    return : ItemStream of Item<string>
//
ItemStream<string> RedisAdapter::getLogAfter(string minID, uint32_t count)
{
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrange(_baseKey + LOG_STUB, minID, "+", count, back_inserter(raw));
  swr::ItemStream<string> ret;
  swr::Item<string> retItem;
  for (const auto& rawItem : raw)
  {
    retItem.second = default_field_value<string>(rawItem.second);
    if (retItem.second.size())
    {
      retItem.first = rawItem.first;
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getLogBefore : get log for home device before specified time
//
//    count  : greatest number of log items to get
//    maxID  : highest time to get log for
//    return : ItemStream of Item<string>
//
ItemStream<string> RedisAdapter::getLogBefore(uint32_t count, string maxID)
{
  swr::ItemStream<swr::Attrs> raw;
  _redis.xrevrange(_baseKey + LOG_STUB, maxID, "-", count, back_inserter(raw));
  swr::ItemStream<string> ret;
  swr::Item<string> retItem;
  for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
  {
    retItem.second = default_field_value<string>(rawItem->second);
    if (retItem.second.size())
    {
      retItem.first = rawItem->first;
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addLog : add a log message
//
//    message : log message to add
//    trim    : number of items to trim log stream to
//    return  : true for success, false for failure
//
bool RedisAdapter::addLog(string message, uint32_t trim)
{
  swr::Attrs attrs = default_field_attrs(message);
  return _redis.xaddTrim(_baseKey + LOG_STUB, "*", attrs.begin(), attrs.end(), trim).size();
}

void RedisAdapter::streamReadBlock(unordered_map<string,string>& keysID, Streams<Attrs>& dest)
{
  try
  {
    _redis.xreadMultiBlock(keysID.begin(), keysID.end(), 0, inserter(dest, dest.end()));
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

void RedisAdapter::publish(string msg)
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

void RedisAdapter::publish(string key, string msg)
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

void RedisAdapter::copyKey(string src, string dst)
{
  _redis.copy(src, dst);
}

void RedisAdapter::deleteKey(string key)
{
  _redis.del(key);
}

vector<string> RedisAdapter::getServerTime()
{
  return _redis.time();
}

void RedisAdapter::psubscribe(string pattern, function<void(string, string, string)> func)
{
  _patternSubscriptions.push_back({ .pattern = pattern, .function = func });
}

void RedisAdapter::subscribe(string channel, function<void(string, string)> func)
{
  _subscriptions.push_back({ .key = channel, .function = func });
}

void RedisAdapter::startListener()
{
  _listener = thread(&RedisAdapter::listener, this);
}

void RedisAdapter::startReader()
{
  _reader = thread(&RedisAdapter::reader, this);
}

void RedisAdapter::registerCommand(string command, function<void(string, string)> func)
{
  _commands.emplace(_commandsKey + ":" + command, func);
}

void RedisAdapter::listener()
{
  auto maybe = _redis.subscriber();
  if ( ! maybe.has_value())
  {
    syslog(LOG_ERR, "Can't get initial subscriber");
    return;
  }
  Subscriber& sub = maybe.value();

  // Consume messages in a loop.
  bool flag = false;
  while (true)
  {
    try
    {
      if (flag)
      {
        sub.consume();
      }
      else
      {
        flag = true;
        sub.on_pmessage([&](string pattern, string key, string msg)
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
        sub.on_message([&](string key, string msg)
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
        sub.psubscribe(_commandsKey + "*");
        // Subscribe to the pattens in _patternSubscriptions
        for (auto element : _patternSubscriptions)
        {
          sub.psubscribe(element.pattern);
        }
        // Subscribe to the keys in _subscriptions
        for (auto element : _subscriptions)
        {
          sub.subscribe(element.key);
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
      auto maybe = _redis.subscriber();
      if ( ! maybe.has_value())
      {
        syslog(LOG_ERR, "Can't replace subscriber");
        return;
      }
      sub = move(maybe.value());
    }
  }
}

void RedisAdapter::addReader(string streamKey,  function<void(string, ItemStream<Attrs>)> func)
{
  _streamKeyID.emplace(streamKey, "$");
  _streamSubscriptions.push_back({ .streamKey = streamKey, .function = func});
}

void RedisAdapter::reader()
{
  // Create a new redis connection only used for streams
  Streams<Attrs> streamsBuffer;
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

Optional<timespec> RedisAdapter::getServerTimespec()
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
