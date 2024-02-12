//
//  RedisAdapter.cpp
//
//  This file contains the implementation of the RedisAdapter class

#include "RedisAdapter.hpp"

using namespace std;
using namespace chrono;
using namespace sw::redis;

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RedisAdapter : constructor
//
//    baseKey : base key of home device
//    options : struct of default values, override using per-field initializer list
//              e.g. RedisConnection::Options{ .user = "adinst", .password = "adinst" }
//    timeout : timeout for pubsub consume and stream listener read
//    return  : RedisAdapter
//
RedisAdapter::RedisAdapter(const string& baseKey, const RedisConnection::Options& options, uint32_t timeout)
: _base_key(baseKey), _timeout(timeout)
{
  RedisConnection::Options opts;

  //  handle possible "" in options.host by preserving opts.host default
  string host = options.host.size() ? options.host : opts.host;

  opts = options;
  opts.host = host;
  opts.timeout = timeout;

  _redis = make_unique<RedisConnection>(opts);
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  ~RedisAdapter : destructor
//
RedisAdapter::~RedisAdapter()
{
  stop_listener();
  for (auto& item : _reader) { stop_reader(item.first); }
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getStatus : get status as string
//
//    subKey  : sub key to get status from
//    baseKey : base key to get status from
//    return  : string with status on success, empty string on failure
//
string RedisAdapter::getStatus(const RA_StatusArgs& args)
{
  ItemStream raw;

  _redis->xrevrange(build_key(STATUS_STUB, args.subKey, args.baseKey), "+", "-", 1, back_inserter(raw));

  if (raw.size()) { return default_field_value<string>(raw.front().second); }
  return {};
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  setStatus : set status for home device as string
//
//    subKey : sub key to set status on
//    value  : status value to set
//    return : true on success, false on failure
//
bool RedisAdapter::setStatus(const string& value, const string& subKey)
{
  Attrs attrs = default_field_attrs(value);

  return _redis->xaddTrim(build_key(STATUS_STUB, subKey), time_to_id(), attrs.begin(), attrs.end(), 1).size();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getLog : get log for home device between specified times
//
//    subKey : sub key to get log from
//    minID  : lowest time to get log for
//    maxID  : highest time to get log for
//    return : ItemStream of Item<string>
//
RedisAdapter::TimeValList<string> RedisAdapter::getLog(const RA_GetLogArgs& args)
{
  ItemStream raw;

  _redis->xrange(build_key(LOG_STUB, args.subKey), min_time_to_id(args.minTime), max_time_to_id(args.maxTime), back_inserter(raw));

  TimeValList<string> ret;
  TimeVal<string> retItem;
  for (const auto& rawItem : raw)
  {
    retItem.second = default_field_value<string>(rawItem.second);
    if (retItem.second.size())
    {
      retItem.first = id_to_time(rawItem.first);
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getLogAfter : get log for home device after specified time
//
//    subKey : sub key to get log from
//    minID  : lowest time to get log for
//    count  : greatest number of log items to get
//    return : ItemStream of Item<string>
//
RedisAdapter::TimeValList<string> RedisAdapter::getLogAfter(const RA_GetLogArgs& args)
{
  ItemStream raw;

  _redis->xrange(build_key(LOG_STUB, args.subKey), min_time_to_id(args.minTime), "+", args.count, back_inserter(raw));

  TimeValList<string> ret;
  TimeVal<string> retItem;
  for (const auto& rawItem : raw)
  {
    retItem.second = default_field_value<string>(rawItem.second);
    if (retItem.second.size())
    {
      retItem.first = id_to_time(rawItem.first);
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getLogBefore : get log for home device before specified time
//
//    subKey : sub key to get log from
//    maxID  : highest time to get log for
//    count  : greatest number of log items to get
//    return : ItemStream of Item<string>
//
RedisAdapter::TimeValList<string> RedisAdapter::getLogBefore(const RA_GetLogArgs& args)
{
  ItemStream raw;

  _redis->xrevrange(build_key(LOG_STUB, args.subKey), max_time_to_id(args.maxTime), "-", args.count, back_inserter(raw));

  TimeValList<string> ret;
  TimeVal<string> retItem;
  for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
  {
    retItem.second = default_field_value<string>(rawItem->second);
    if (retItem.second.size())
    {
      retItem.first = id_to_time(rawItem->first);
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addLog : add a log message
//
//    message : log message to add
//    subKey  : sub key to add log to
//    trim    : number of items to trim log stream to
//    return  : true for success, false for failure
//
bool RedisAdapter::addLog(const string& message, const RA_AddLogArgs& args)
{
  Attrs attrs = default_field_attrs(message);

  return _redis->xaddTrim(build_key(LOG_STUB, args.subKey), time_to_id(), attrs.begin(), attrs.end(), args.trim).size();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addStreamSingleDouble : add a single data item of type double
//
//    subKey : sub key to add data to
//    time   : time to add the data at
//    data   : data to add
//    trim   : number of items to trim the stream to
//    return : time of the added data item if successful, zero on failure
//
uint64_t RedisAdapter::addStreamSingleDouble(const string& subKey, double data, const RA_AddStreamArgs& args)
{
  string key = build_key(STREAM_STUB, subKey);
  Attrs attrs = default_field_attrs(data);

  string id = args.trim ? _redis->xaddTrim(key, time_to_id(args.time), attrs.begin(), attrs.end(), args.trim)
                        : _redis->xadd(key, time_to_id(args.time), attrs.begin(), attrs.end());

  return id_to_time(id);
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getServerTime : get Redis server time as nanoseconds
//
//    return  : zero on failure, nanoseconds on success
//
uint64_t RedisAdapter::getServerTime()
{
  uint64_t nanos = 0;
  vector<string> time = _redis->time();
  // The redis command time is returns an array with the first element being
  // the time in seconds and the second being the microseconds within that second
  if (time.size() == 2)
  {
    try
    {
      nanos = stoull(time[0]) * 1000 * 1000 * 1000  //  seconds to nanoseconds
            + stoull(time[1]) * 1000;               //  microseconds to nanoseconds
    }
    catch (...) {}
  }
  return nanos;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  subscribe : subscribe to a channel
//
//    baseKey : the base key to construct the channel from
//    subKey  : the sub key to construct the channel from
//    func    : the function to call when message received on this channel
//    return  : true if listener started, false if listener failed to start
//
bool RedisAdapter::subscribe(const string& subKey, ListenSubFn func, const string& baseKey)
{
  stop_listener();
  _command_subs[build_key(CHANNEL_STUB, subKey, baseKey)].push_back(func);
  return start_listener();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  psubscribe : subscribe to a pattern
//
//    baseKey : the base key to construct the channel from
//    subKey  : the sub key pattern to construct the channel from
//    func    : the function to call when message received on this channel
//    return  : true if listener started, false if listener failed to start
//
bool RedisAdapter::psubscribe(const string& subKey, ListenSubFn func, const string& baseKey)
{
  //  don't allow psubscribe if wildcards in the base key
  if (baseKey.size())
    { if (baseKey.find_first_of("*?[]") != string::npos) { return false; } }
  else
    { if (_base_key.find_first_of("*?[]") != string::npos) { return false; } }

  stop_listener();
  _pattern_subs[build_key(CHANNEL_STUB, subKey, baseKey)].push_back(func);
  return start_listener();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  unsubscribe : unsubscribe from a command and/or pattern
//
//    subKey  : device subKey/pattern to unsubscribe from
//    baseKey : device basekey to unsubscribe from
//    return  : true if listener started or no more commands/patterns
//              false if listener failed to start
//
bool RedisAdapter::unsubscribe(const string& subKey, const string& baseKey)
{
  stop_listener();
  string key = build_key(CHANNEL_STUB, subKey, baseKey);
  if (_pattern_subs.count(key)) _pattern_subs.erase(key);
  if (_command_subs.count(key)) _command_subs.erase(key);
  return (_pattern_subs.size() || _command_subs.size()) ? start_listener() : true;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addGenericReader : add a reader for a key that does NOT follow RedisAdapter schema
//
//    key     : the key to add (must NOT be a RedisAdapter schema key)
//    func    : function to call when data is read - data will be RedisAdapter::Attrs
//    return  : true if reader started, false if reader failed to start
//
bool RedisAdapter::addGenericReader(const string& key, ReaderSubFn<Attrs> func)
{
  if (split_key(key).first.size()) return false;  //  reject if RedisAdapter key stub found

  int32_t slot = _redis->keyslot(key);
  if (slot < 0) return false;

  stop_reader(slot);
  reader_info& info = _reader[slot];

  if (info.stop.empty())
  {
    info.stop = build_key(STOP_STUB, key);
    info.keyids[info.stop] = "$";
  }
  info.subs[key].push_back(make_reader_callback(func));
  info.keyids[key] = "$";
  return start_reader(slot);
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  removeGenericReader : remove all readers for a key that does NOT follow RedisAdapter schema
//
//    key     : the key to remove (must NOT be a RedisAdapter schema key)
//    return  : true if reader started, false if reader failed to start
//
bool RedisAdapter::removeGenericReader(const string& key)
{
  if (split_key(key).first.size()) return false;  //  reject if RedisAdapter key stub found

  int32_t slot = _redis->keyslot(key);
  if (slot < 0 || _reader.count(slot) == 0) return false;

  stop_reader(slot);
  reader_info& info = _reader.at(slot);
  info.subs.erase(key);
  info.keyids.erase(key);

  if (info.subs.empty())
  {
    _reader.erase(slot);
    return true;
  }
  return start_reader(slot);
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  Private methods
//
string RedisAdapter::build_key(const string& keyStub, const string& subKey, const string& baseKey) const
{
  //  surround base key with {} to locate keys with same base key in same cluster slot
  //  this mitgates CROSSSLOT errors for copyKey and renameKey but also puts all keys
  //  for a base key onto the same reader thread (this could be mitigate with an additional
  //  load balancing strategy of mutiple threads per slot if necessary)
  //  NOTE - none of this has ANY effect for single instance (non-cluster) Redis servers
  return "{" + (baseKey.size() ? baseKey : _base_key) + "}:" + keyStub + (subKey.size() ? ":" + subKey : "");
}

pair<string, string> RedisAdapter::split_key(const string& key) const
{
  size_t idx = key.find(CHANNEL_STUB), len = CHANNEL_STUB.size();

  if (idx == string::npos) { idx = key.find(STREAM_STUB); len = STREAM_STUB.size(); }

  if (idx == string::npos) { idx = key.find(STATUS_STUB); len = STATUS_STUB.size(); }

  if (idx == string::npos) { idx = key.find(LOG_STUB);    len = LOG_STUB.size(); }

  if (idx == string::npos) { idx = key.find(STOP_STUB);   len = STOP_STUB.size(); }

  if (idx == string::npos) return {};

  //  make sure to omit the {} from base key (see build_key) and correctly handle empty sub key
  return make_pair(key.substr(1, idx - 3), key.size() > idx + len ? key.substr(idx + len + 1) : "");
}

uint64_t RedisAdapter::get_host_time() const
{
  return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

bool RedisAdapter::copyStream(const string& srcSubKey, const string& dstSubKey, const string& baseKey)
{
  string srcKey = build_key(STREAM_STUB, srcSubKey, baseKey);
  string dstKey = build_key(STREAM_STUB, dstSubKey);

  int32_t ret = _redis->copy(srcKey, dstKey);

  //  WARNING - this cross-slot copy brings ALL the data from srcKey to the client computer for
  //            manual re-add to dstKey - this is potentially network, memory and cpu intensive!
  if (ret == -2 && _redis->exists(dstKey) == 0)
  {
    ItemStream raw;
    if (_redis->xrange(srcKey, "-", "+", back_inserter(raw)))
    {
      string id;
      for (auto& it : raw) { id = _redis->xadd(dstKey, it.first, it.second.begin(), it.second.end()); }
      ret = id.size();
    }
  }
  return ret > 0;
}

bool RedisAdapter::start_listener()
{
  if (_listener.joinable()) return false;

  mutex mx; condition_variable cv;        //  use condition_variable to signal when
  unique_lock<mutex> lk(mx, defer_lock);  //  thread is about to enter consume loop

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
          if (_pattern_subs.count(pat))
          {
            auto split = split_key(key);
            for (auto& func : _pattern_subs.at(pat))
              { func(split.first, split.second, msg); }
          }
        }
      );  //  end lambda in lambda /////////////////////////

      //  begin lambda in lambda ///////////////////////////
      sub.on_message([&](string key, string msg)
        {
          if (_command_subs.count(key))
          {
            auto split = split_key(key);
            for (auto& func : _command_subs.at(key))
              { func(split.first, split.second, msg); }
          }
        }
      );  //  end lambda in lambda /////////////////////////

      for (const auto& cs : _command_subs) { sub.subscribe(cs.first); }

      for (const auto& ps : _pattern_subs) { sub.psubscribe(ps.first); }

      sub.subscribe(build_key(STOP_STUB));

      _listener_run = true;

      cv.notify_all();  //  notify about to enter loop (NOT in loop)

      while (_listener_run)
      {
        try { sub.consume(); }
        catch (const TimeoutError&) {}
        catch (const Error& e)
        {
          syslog(LOG_ERR, "consume in listener: %s", e.what());
          _listener_run = false;
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
  _listener_run = false;
  _redis->publish(build_key(STOP_STUB), "");
  _listener.join();
  return true;
}

bool RedisAdapter::add_reader_helper(const string& baseKey, const string& keyStub, const string& subKey, reader_sub_fn func)
{
  string key = build_key(keyStub, subKey, baseKey);
  int32_t slot = _redis->keyslot(key);
  if (slot < 0) return false;

  stop_reader(slot);
  reader_info& info = _reader[slot];

  if (info.stop.empty())
  {
    info.stop = build_key(STOP_STUB, "", baseKey);
    info.keyids[info.stop] = "$";
  }
  info.subs[key].push_back(func);
  info.keyids[key] = "$";
  return start_reader(slot);
}

bool RedisAdapter::remove_reader_helper(const string& baseKey, const string& keyStub, const string& subKey)
{
  string key = build_key(keyStub, subKey, baseKey);
  int32_t slot = _redis->keyslot(key);
  if (slot < 0 || _reader.count(slot) == 0) return false;

  stop_reader(slot);
  reader_info& info = _reader.at(slot);
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

  reader_info& info = _reader.at(slot);

  if (info.thread.joinable()) return false;

  mutex mx; condition_variable cv;        //  use condition_variable to signal when
  unique_lock<mutex> lk(mx, defer_lock);  //  thread is about to enter read loop

  //  begin lambda  //////////////////////////////////////////////////
  info.thread = thread([&]()
    {
      info.run = true;

      cv.notify_all();  //  notify about to enter loop (NOT in loop)

      for (Streams out; info.run; out.clear())
      {
        if (_redis->xreadMultiBlock(info.keyids.begin(), info.keyids.end(), _timeout, inserter(out, out.end())))
        {
          for (auto& item : out)
          {
            if (item.second.size())
              { info.keyids[item.first] = item.second.back().first; }

            if (info.subs.count(item.first))
            {
              auto split = split_key(item.first);
              for (auto& func : info.subs.at(item.first))
              {
                if (split.first.size()) { func(split.first, split.second, item.second); }
                else { func(item.first, item.first, item.second); }
              }
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

  reader_info& info = _reader.at(slot);
  if ( ! info.thread.joinable()) return false;

  info.run = false;
  Attrs attrs = default_field_attrs("");
  _redis->xaddTrim(info.stop, "*", attrs.begin(), attrs.end(), 1);
  info.thread.join();
  return true;
}
