/**
 * RedisAdapter.cpp
 *
 * This file contains the implementation of the RedisAdapter class
 *
 * @author rsantucc
 */

#include "RedisAdapter.hpp"

using namespace std;
using namespace chrono;
using namespace sw::redis;

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RedisAdapter : constructor
//
//    baseKey : base key of home device
//    options : struct of default values, override using per-field initializer list
//              e.g. RedisConnection::Options{ .user = "adinst", .password = "adinst" }
//    timeout : timeout for pubsub consume and stream listener read
//    return  : RedisAdapter
//
RedisAdapter::RedisAdapter(const string& baseKey, const RedisConnection::Options& options, uint32_t timeout)
: _baseKey(baseKey), _timeout(timeout)
{
  RedisConnection::Options opts = options;
  opts.timeout = timeout;
  _redis = make_unique<RedisConnection>(opts);
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RedisAdapter : constructor
//
//    baseKey : base key of home device
//    host    : IP address of server "w.x.y.z"
//    port    : port server is listening on
//    timeout : timeout for pubsub consume and stream listener read
//    return  : RedisAdapter
//
RedisAdapter::RedisAdapter(const string& baseKey, const string& host, uint16_t port, uint32_t timeout)
: RedisAdapter(baseKey, RedisConnection::Options{ .host = host, .port = port }, timeout) {}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  ~RedisAdapter : destructor
//
RedisAdapter::~RedisAdapter()
{
  stop_listener();
  stop_reader();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getStatus : get status for home device as string
//
//    subKey : sub key to get status from
//    return : string with status if successful
//             empty string if failure
//
string RedisAdapter::getStatus(const string& subKey, const string& baseKey)
{
  string key = (baseKey.size() ? baseKey : _baseKey) + STATUS_STUB + subKey;
  ItemStream<Attrs> raw;

  _redis->xrevrange(key, "+", "-", 1, back_inserter(raw));

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
  Attrs attrs = default_field_attrs(value);

  return _redis->xaddTrim(_baseKey + STATUS_STUB + subKey, "*", attrs.begin(), attrs.end(), 1).size();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getLog : get log for home device between specified times
//
//    minID  : lowest time to get log for
//    maxID  : highest time to get log for
//    return : ItemStream of Item<string>
//
ItemStream<string> RedisAdapter::getLog(const string& minID, const string& maxID)
{
  ItemStream<Attrs> raw;

  _redis->xrange(_baseKey + LOG_STUB, minID, maxID, back_inserter(raw));

  ItemStream<string> ret;
  Item<string> retItem;
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
ItemStream<string> RedisAdapter::getLogAfter(const string& minID, uint32_t count)
{
  ItemStream<Attrs> raw;

  _redis->xrange(_baseKey + LOG_STUB, minID, "+", count, back_inserter(raw));

  ItemStream<string> ret;
  Item<string> retItem;
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
ItemStream<string> RedisAdapter::getLogBefore(uint32_t count, const string& maxID)
{
  ItemStream<Attrs> raw;

  _redis->xrevrange(_baseKey + LOG_STUB, maxID, "-", count, back_inserter(raw));

  ItemStream<string> ret;
  Item<string> retItem;
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
bool RedisAdapter::addLog(const string& message, uint32_t trim)
{
  Attrs attrs = default_field_attrs(message);

  return _redis->xaddTrim(_baseKey + LOG_STUB, "*", attrs.begin(), attrs.end(), trim).size();
}

bool RedisAdapter::copyKey(const string& src, const string& dst)
{
  return _redis->copy(src, dst) == 1;
}

bool RedisAdapter::deleteKey(const string& key)
{
  return _redis->del(key) == 1;
}

vector<string> RedisAdapter::getServerTime()
{
  return _redis->time();
}

Optional<timespec> RedisAdapter::getServerTimespec()
{
  Optional<timespec> ret;
  vector<string> result = _redis->time();
  // The redis command time is returns an array with the first element being
  // the time in seconds and the second being the microseconds within that second
  if (result.size() == 2)
  {
    ret = { .tv_sec  = stoll(result[0]),            // unix time in seconds
            .tv_nsec = stoll(result[1]) * 1000 };   // microseconds in the second
  }
  return ret;
}

bool RedisAdapter::publish(const string& msg, const string& key)
{
  return _redis->publish(_baseKey + COMMANDS_STUB + key, msg) >= 0;
}

bool RedisAdapter::psubscribe(const string& pat, ListenSubFn func)
{
  _patternSubs[_baseKey + COMMANDS_STUB + pat].push_back(func);
  return true;
}

bool RedisAdapter::subscribe(const string& key, ListenSubFn func)
{
  _commandSubs[_baseKey + COMMANDS_STUB + key].push_back(func);
  return true;
}

bool RedisAdapter::start_listener()
{
  if (_listener.joinable()) return false;

  //  use condition_variable to signal when thread is about to enter consume loop
  mutex mx;
  condition_variable cv;
  unique_lock<mutex> lk(mx, defer_lock);

  bool ret = true;

  _listener = thread([&]()
    {
      auto maybe = _redis->subscriber();
      if ( ! maybe.has_value())
      {
        syslog(LOG_ERR, "failed to get subscriber");
        ret = false;      //  start_listener return false
        cv.notify_all();  //  notify cv
        return;           //  return from lambda
      }
      Subscriber& sub = maybe.value();

      sub.on_pmessage([&](string pat, string key, string msg)
        {
          //  figure out pattern matching and do the right thing
        }
      );

      sub.on_message([&](string key, string msg)
        {
          if (_commandSubs.count(key))
          {
            auto split = split_fully_qualified_key(key);
            for (auto& func : _commandSubs.at(key))
            {
              func(split.first, split.second, msg);
            }
          }
        }
      );

      //  subscribe foreign only?
      for (const auto& cs : _commandSubs) sub.subscribe(cs.first);

      //  psubscribe foreign only?
      for (const auto& ps : _patternSubs) sub.psubscribe(ps.first);

      sub.psubscribe(_baseKey + COMMANDS_STUB + "*");   //  does this catch everything local?

      _runListener = true;

      cv.notify_all();  //  notify about to enter loop (NOT in loop)

      while (_runListener)
      {
        try { sub.consume(); }
        catch (const TimeoutError&) {}
        catch (const Error& e)
        {
          syslog(LOG_ERR, "consume in listener: %s", e.what());
          _runListener = false;
        }
      }
    }
  );
  //  wait until notified that thread is running (or timeout)
  bool nto = cv.wait_for(lk, milliseconds(10)) == cv_status::no_timeout;
  if ( ! nto) syslog(LOG_ERR, "start_listener timeout waiting for thread start");
  return nto && ret;
}

bool RedisAdapter::stop_listener()
{
  if (_listener.joinable())
  {
    _runListener = false;
    _listener.join();
    return true;
  }
  return false;
}

bool RedisAdapter::addReader(const string& key, ReaderSubFn func)
{
  _readerKeyID.emplace(key, "$");
  _readerSubs[key].push_back(func);
  return true;
}

bool RedisAdapter::start_reader()
{
  if (_reader.joinable()) return false;

  //  use condition_variable to signal when thread is about to enter xreadMultiBlock loop
  mutex mx;
  condition_variable cv;
  unique_lock<mutex> lk(mx, defer_lock);

  _reader = thread([&]()
    {
      _runReader = true;

      cv.notify_all();  //  notify about to enter loop (NOT in loop)

      for (Streams<Attrs> out; _runReader; out.clear())
      {
        if (_redis->xreadMultiBlock(_readerKeyID.begin(), _readerKeyID.end(), _timeout, inserter(out, out.end())))
        {
          for (auto& is : out)
          {
            if (is.second.size()) { _readerKeyID[is.first] = is.second.back().first; }

            if (_readerSubs.count(is.first))
            {
              auto split = split_fully_qualified_key(is.first);
              for (auto& func : _readerSubs.at(is.first))
              {
                func(split.first, split.second, is.second);
              }
            }
          }
        }
        else
        {
          syslog(LOG_ERR, "xreadMultiBlock returned false in reader");
          return;
        }
      }
    }
  );
  //  wait until notified that thread is running (or timeout)
   bool nto = cv.wait_for(lk, milliseconds(10)) == cv_status::no_timeout;
   if ( ! nto) syslog(LOG_ERR, "start_reader timeout waiting for thread start");
   return nto;
}

bool RedisAdapter::stop_reader()
{
  if (_reader.joinable())
  {
    _runReader = false;
    _reader.join();
    return true;
  }
  return false;
}

pair<string, string> RedisAdapter::split_fully_qualified_key(const string& key)
{
  size_t idx = key.find(COMMANDS_STUB);
  if (idx != string::npos)
    { return make_pair(key.substr(0, idx), key.substr(idx + COMMANDS_STUB.size())); }

  idx = key.find(DATA_STUB);
  if (idx != string::npos)
    { return make_pair(key.substr(0, idx), key.substr(idx + DATA_STUB.size())); }

  idx = key.find(SETTINGS_STUB);
  if (idx != string::npos)
    { return make_pair(key.substr(0, idx), key.substr(idx + SETTINGS_STUB.size())); }

  idx = key.find(STATUS_STUB);
  if (idx != string::npos)
    { return make_pair(key.substr(0, idx), key.substr(idx + STATUS_STUB.size())); }

  idx = key.find(LOG_STUB);
  if (idx != string::npos)
    { return make_pair(key.substr(0, idx), ""); }

  return {};
}
