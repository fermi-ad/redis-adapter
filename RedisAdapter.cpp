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
  RedisConnection::Options opts;

  //  handle possible "" in options.host by preserving opts.host default
  string host = options.host.size() ? options.host : opts.host;

  opts = options;
  opts.host = host;
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
  for (auto& item : _reader) {  stop_reader(item.first); }
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getStatus : get status as string
//
//    subKey : sub key to get status from
//    return : string with status if successful
//             empty string if failure
//
string RedisAdapter::getStatus(const string& subKey, const string& baseKey)
{
  ItemStream<Attrs> raw;

  _redis->xrevrange(build_key(baseKey, STATUS_STUB, subKey), "+", "-", 1, back_inserter(raw));

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

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getTimespec : get server time as a timespec
//
//    return  : Optional with timespec on success
//              Optional empty on failure
//
Optional<timespec> RedisAdapter::getTimespec()
{
  Optional<timespec> ret;
  vector<string> result = _redis->time();
  // The redis command time is returns an array with the first element being
  // the time in seconds and the second being the microseconds within that second
  if (result.size() == 2)
  {
    ret = { .tv_sec  = stoll(result[0]),    // unix time in seconds
            .tv_nsec = stoll(result[1]) };  // nanoseconds in the second
  }
  return ret;
}

bool RedisAdapter::publish(const string& subKey, const string& message, const string& baseKey)
{
  return _redis->publish(build_key(baseKey, COMMANDS_STUB, subKey), message) >= 0;
}

bool RedisAdapter::psubscribe(const string& pattern, ListenSubFn func, const string& baseKey)
{
  stop_listener();
  _patternSubs[build_key(baseKey, COMMANDS_STUB, pattern)].push_back(func);
  return start_listener();
}

bool RedisAdapter::subscribe(const string& subKey, ListenSubFn func, const string& baseKey)
{
  stop_listener();
  _commandSubs[build_key(baseKey, COMMANDS_STUB, subKey)].push_back(func);
  return start_listener();
}

bool RedisAdapter::unsubscribe(const string& unsub, const string& baseKey)
{
  stop_listener();
  string key = build_key(unsub, COMMANDS_STUB, baseKey);
  if (_patternSubs.count(key)) _patternSubs.erase(key);
  if (_commandSubs.count(key)) _commandSubs.erase(key);
  return (_patternSubs.size() || _commandSubs.size()) ? start_listener() : false;
}

bool RedisAdapter::start_listener()
{
  if (_listener.joinable()) return false;

  mutex mx;                   //  use condition_variable to signal when
  condition_variable cv;      //  thread is about to enter consume loop
  unique_lock<mutex> lk(mx);

  bool ret = true;

  //  begin lambda  //////////////////////////////////////////////////
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

      //  begin lambda in lambda ///////////////////////////
      sub.on_pmessage([&](string pat, string key, string msg)
        {
          if (_patternSubs.count(pat))
          {
            auto split = split_key(key);
            for (auto& func : _patternSubs.at(pat))
              { func(split.first, split.second, msg); }
          }
        }
      );  //  end lambda in lambda /////////////////////////

      //  begin lambda in lambda ///////////////////////////
      sub.on_message([&](string key, string msg)
        {
          if (_commandSubs.count(key))
          {
            auto split = split_key(key);
            for (auto& func : _commandSubs.at(key))
              { func(split.first, split.second, msg); }
          }
        }
      );  //  end lambda in lambda /////////////////////////

      for (const auto& cs : _commandSubs) { sub.subscribe(cs.first); }

      for (const auto& ps : _patternSubs) { sub.psubscribe(ps.first); }

      sub.subscribe(_baseKey + CONTROL_STUB);

      _listenerRun = true;

      cv.notify_all();  //  notify about to enter loop (NOT in loop)

      while (_listenerRun)
      {
        try { sub.consume(); }
        catch (const TimeoutError&) {}
        catch (const Error& e)
        {
          syslog(LOG_ERR, "consume in listener: %s", e.what());
          _listenerRun = false;
        }
      }
    }
  );  //  end lambda  ////////////////////////////////////////////////

  //  wait until notified that thread is running (or timeout)
  bool nto = cv.wait_for(lk, milliseconds(10)) == cv_status::no_timeout;
  if ( ! nto) syslog(LOG_ERR, "start_listener timeout waiting for thread start");
  return nto && ret;
}

bool RedisAdapter::stop_listener()
{
  if ( ! _listener.joinable()) return false;
  _listenerRun = false;
  _redis->publish(_baseKey + CONTROL_STUB, "");
  _listener.join();
  return true;
}

bool RedisAdapter::add_reader_helper(const string& baseKey, const string& stub, const string& subKey, ReaderSubFn func)
{
  string key = build_key(baseKey, stub, subKey);
  int32_t slot = _redis->keyslot(key);
  if (slot < 0) return false;
  stop_reader(slot);
  ReaderInfo& info = _reader[slot];
  if (info.control.empty())
  {
    //  use key in {} to ensure control hashes to this slot
    info.control = "{" + key + "}" + CONTROL_STUB;
    info.keyids[info.control] = "$";
  }
  info.subs[key].push_back(func);
  info.keyids[key] = "$";
  return start_reader(slot);
}

bool RedisAdapter::remove_reader_helper(const string& baseKey, const string& stub, const string& subKey)
{
  string key = build_key(baseKey, stub, subKey);
  int32_t slot = _redis->keyslot(key);
  if (slot < 0 || _reader.count(slot) == 0) return false;
  stop_reader(slot);
  ReaderInfo& info = _reader.at(slot);
  info.subs.erase(key);
  info.keyids.erase(key);
  if (info.subs.empty())
  {
    _reader.erase(slot);
    return true;
  }
  return start_reader(slot);
}

bool RedisAdapter::start_reader(uint16_t slot)
{
  if (_reader.count(slot) == 0) return false;

  ReaderInfo& info = _reader.at(slot);

  if (info.thread.joinable()) return false;

  mutex mx;                   //  use condition_variable to signal when
  condition_variable cv;      //  thread is about to enter read loop
  unique_lock<mutex> lk(mx);

  //  begin lambda  //////////////////////////////////////////////////
  info.thread = thread([&]()
    {
      info.run = true;

      cv.notify_all();  //  notify about to enter loop (NOT in loop)

      for (Streams<Attrs> out; info.run; out.clear())
      {
        if (_redis->xreadMultiBlock(info.keyids.begin(), info.keyids.end(), _timeout, inserter(out, out.end())))
        {
          for (auto& is : out)
          {
            if (is.second.size())
              { info.keyids[is.first] = is.second.back().first; }

            if (info.subs.count(is.first))
            {
              auto split = split_key(is.first);
              for (ReaderSubFn& func : info.subs.at(is.first))
                { func(split.first, split.second, is.second); }
            }
          }
        }
        else
        {
          syslog(LOG_ERR, "xreadMultiBlock returned false in reader");
          info.run = false;
        }
      }
    }
  );  //  end lambda  ////////////////////////////////////////////////

  //  wait until notified that thread is running (or timeout)
   bool nto = cv.wait_for(lk, milliseconds(10)) == cv_status::no_timeout;
   if ( ! nto) syslog(LOG_ERR, "start_reader timeout waiting for thread start");
   return nto;
}

bool RedisAdapter::stop_reader(uint16_t slot)
{
  if (_reader.count(slot) == 0) return false;
  ReaderInfo& info = _reader.at(slot);
  if ( ! info.thread.joinable()) return false;
  info.run = false;
  Attrs attrs = default_field_attrs("");
  _redis->xaddTrim(info.control, "*", attrs.begin(), attrs.end(), 1);
  info.thread.join();
  return true;
}

string RedisAdapter::build_key(const string& baseKey, const string& stub, const string& subKey)
{
  return (baseKey.size() ? baseKey : _baseKey) + stub + subKey;
}

pair<string, string> RedisAdapter::split_key(const string& key)
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
