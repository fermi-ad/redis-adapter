//
//  RedisAdapter.cpp
//
//  This file contains the implementation of the RedisAdapter class

#include "RedisAdapter.hpp"

using namespace std;
using namespace chrono;
using namespace sw::redis;

const uint32_t NANOS_PER_MILLI = 1'000'000;

const auto THREAD_START_CONFIRM = milliseconds(20);

const uint32_t NO_TOKEN = -1;

static uint64_t nanoseconds_since_epoch()
{
  return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RA_Time : constructor that converts an id string to nanoseconds and sequence number
//
//    id     : Redis ID string e.g. "12345-67089" where the first number is milliseconds since
//             epoch and the second number is the nanoseconds remainder
//    return : RA_Time
//
RA_Time::RA_Time(const string& id)
{
  try
  {
    value = stoll(id) * NANOS_PER_MILLI;
    size_t pos = id.find('-');
    if (pos != string::npos) { value += stoll(id.substr(pos + 1)); }
  }
  catch (...) { value = 0; }
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RA_Time::id : return Redis ID string
//
string RA_Time::id() const
{
  //  place the whole milliseconds on the left-hand side of the ID
  //  and the remainder nanoseconds on the right-hand side of the ID
  return ok() ? to_string(value / NANOS_PER_MILLI) + "-" + to_string(value % NANOS_PER_MILLI) : "0-0";
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RA_Time::id_or_now : return RA_Time or current time as Redis ID string
//
string RA_Time::id_or_now() const
{
  return ok() ? id() : RA_Time(nanoseconds_since_epoch()).id();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RedisAdapter : constructor
//
//    baseKey : base key of home device
//    options : struct of default values, override using per-field initializer list
//              e.g. { .user = "adinst", .password = "adinst" }
//    return  : RedisAdapter
//
RedisAdapter::RedisAdapter(const string& baseKey, const RA_Options& options) :
  _options(options), _redis(options.cxn), _base_key(baseKey), _connecting(false),
  _watchdog_run(false), _readers_defer(false), _replier_pool(options.workers)
{
  _watchdog_key = build_key("watchdog");

  if (_options.dogname.size())
  {
    _watchdog_thd = thread([&]()
      {
        mutex mx; unique_lock lk(mx);   //  dummies for _watchdog_cv

        addWatchdog(_options.dogname, 1);

        for (_watchdog_run = true;      //  every 900ms set expire for 1000ms
             _watchdog_run && _watchdog_cv.wait_for(lk, milliseconds(900)) == cv_status::timeout;
             petWatchdog(_options.dogname, 1)) {}
      }
    );
  }
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  ~RedisAdapter : destructor
//
RedisAdapter::~RedisAdapter()
{
  if (_watchdog_run)
  {
    _watchdog_run = false;
    _watchdog_cv.notify_all();
    _watchdog_thd.join();
  }
  for (auto& item : _reader) { stop_reader(item.first); }
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addSingleDouble : add a single data item of type double
//
//    subKey : sub key to add data to
//    time   : time to add the data at
//    data   : data to add
//    trim   : number of items to trim the stream to
//    return : time of the added data item if successful, zero on failure
//
RA_Time RedisAdapter::addSingleDouble(const string& subKey, double data, const RA_ArgsAdd& args)
{
  string key = build_key(subKey);
  Attrs attrs = default_field_attrs(data);

  string id = args.trim ? _redis.xaddTrim(key, args.time.id_or_now(), attrs.begin(), attrs.end(), args.trim)
                        : _redis.xadd(key, args.time.id_or_now(), attrs.begin(), attrs.end());

  if (reconnect(id.size()) == 0) { return RA_NOT_CONNECTED; }

  return RA_Time(id);
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  setDeferReaders : defer or un-defer addition and removal of readers
//                    - deferring cancels all reads and stops all reader threads until un-defer
//                    - un-deferring starts all reader threads
//                    this prevents redundant thread destruction/creation and is
//                    the preferred way to add/remove multiple readers at one time
//
//    defer   : whether to defer or un-defer addition and removal of readers
//    return  : true on success, false on failure
//
bool RedisAdapter::setDeferReaders(bool defer)
{
  if (defer && ! _readers_defer)
  {
    _readers_defer = defer;
    for (auto& item : _reader) { stop_reader(item.first); }
  }
  else if ( ! defer && _readers_defer)
  {
    _readers_defer = defer;
    for (auto& item : _reader) { start_reader(item.first); }
  }
  return true;
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
  if (split_key(key).first.size()) return false;  //  reject if basekey found

  uint32_t token = reader_token(key);
  reader_info& info = _reader[token];

  info.subs[key].push_back(make_reader_callback(func));
  info.keyids[key] = "$";

  if (token == NO_TOKEN) return false;

  stop_reader(token);

  if (info.stop.empty())
  {
    info.stop = build_key(STOP_STUB, key);
    info.keyids[info.stop] = "$";
  }
  return start_reader(token);
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  removeGenericReader : remove all readers for a key that does NOT follow RedisAdapter schema
//
//    key     : the key to remove (must NOT be a RedisAdapter schema key)
//    return  : true if reader started, false if reader failed to start
//
bool RedisAdapter::removeGenericReader(const string& key)
{
  if (split_key(key).first.size()) return false;  //  reject if basekey found

  uint32_t token = reader_token(key);
  if (token == NO_TOKEN || _reader.count(token) == 0) return false;

  //  TODO: this is flawed - if NO_TOKEN (not connected) we need to search all buckets
  //  for the key and remove it, also the NO_TOKEN bucket should be checked for every
  //  remove to see if the key is in there - HOWEVER removing readers is very rare
  //  (pretty much unheard of) so this is not a huge priority

  stop_reader(token);
  reader_info& info = _reader.at(token);
  info.subs.erase(key);
  info.keyids.erase(key);

  if (info.subs.empty())
  {
    _reader.erase(token);
    return true;
  }
  return start_reader(token);
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  Private methods
//
string RedisAdapter::build_key(const string& subKey, const string& baseKey) const
{
  //  surround base key with {} to locate keys with same base key in same cluster slot
  //  this mitgates CROSSSLOT errors for copyKey and renameKey but also puts all keys
  //  for a base key onto the same reader thread (this could be mitigated with an additional
  //  load balancing strategy of mutiple threads per slot if necessary)
  //  NOTE - none of this has ANY effect for single instance (non-cluster) Redis servers
  return "{" + (baseKey.size() ? baseKey : _base_key) + "}" + (subKey.size() ? ":" + subKey : "");
}

pair<string, string> RedisAdapter::split_key(const string& key) const
{
  size_t idx = key.find(_base_key), len = _base_key.size();

  if (idx == string::npos) return {};

  return make_pair(key.substr(idx, len),  //  look past the {} and :
                   key.size() > idx + len + 1 ? key.substr(idx + len + 2) : "");
}

bool RedisAdapter::copy(const string& srcSubKey, const string& dstSubKey, const string& baseKey)
{
  string srcKey = build_key(srcSubKey, baseKey);
  string dstKey = build_key(dstSubKey);

  int32_t ret = _redis.copy(srcKey, dstKey);

  //  WARNING - this cross-slot copy brings ALL the data from srcKey to the client computer for
  //            manual re-add to dstKey - this is potentially network, memory and cpu intensive!
  if (ret == -2 && _redis.exists(dstKey) == 0)
  {
    ItemStream raw;
    if (_redis.xrange(srcKey, "-", "+", back_inserter(raw)))
    {
      string id;
      for (auto& it : raw) { id = _redis.xadd(dstKey, it.first, it.second.begin(), it.second.end()); }
      ret = id.size();
    }
  }
  reconnect(ret != -1);   //  if ret == -1, pass 0 to reconnect
  return ret > 0;
}

uint32_t RedisAdapter::reader_token(const std::string& key)
{
  static hash<string> hasher;

  int32_t slot = _redis.keyslot(key);
  if (slot < 0) return NO_TOKEN;

  uint32_t token = slot << 16;
  if (_options.readers > 1)
    { token += hasher(key) % _options.readers; }

  return token;
}

bool RedisAdapter::add_reader_helper(const string& baseKey, const string& subKey, reader_sub_fn func)
{
  string key = build_key(subKey, baseKey);

  uint32_t token = reader_token(key);
  reader_info& info = _reader[token];

  info.subs[key].push_back(func);
  info.keyids[key] = "$";

  if (token == NO_TOKEN) return false;

  stop_reader(token);

  if (info.stop.empty())
  {
    info.stop = build_key(subKey + ":" + STOP_STUB, baseKey);
    info.keyids[info.stop] = "$";
  }
  return start_reader(token);
}

bool RedisAdapter::remove_reader_helper(const string& baseKey, const string& subKey)
{
  string key = build_key(subKey, baseKey);

  uint32_t token = reader_token(key);
  if (token == NO_TOKEN || _reader.count(token) == 0) return false;

  //  TODO: this is flawed - if NO_TOKEN (not connected) we need to search all buckets
  //  for the key and remove it, also the NO_TOKEN bucket should be checked for every
  //  remove to see if the key is in there - HOWEVER removing readers is very rare
  //  (pretty much unheard of) so this is not a huge priority

  stop_reader(token);
  reader_info& info = _reader.at(token);
  info.subs.erase(key);
  info.keyids.erase(key);

  if (info.subs.empty())
  {
    _reader.erase(token);
    return true;
  }
  return start_reader(token);
}

bool RedisAdapter::start_reader(uint32_t token)
{
  if (_readers_defer) return true;

  if (token == NO_TOKEN || _reader.count(token) == 0) return false;

  reader_info& info = _reader.at(token);

  if (info.thread.joinable()) return false;

  mutex mx; condition_variable cv;        //  use condition_variable to signal when
  unique_lock<mutex> lk(mx, defer_lock);  //  thread is about to enter read loop

  //  begin lambda  //////////////////////////////////////////////////
  info.thread = thread([&]()
    {
      bool check_for_dollars = true;

      info.run = true;

      cv.notify_all();  //  notify about to enter loop (NOT in loop)

      for (Streams out; info.run; out.clear())
      {
        if (_redis.xreadMultiBlock(info.keyids.begin(), info.keyids.end(), _options.cxn.timeout, inserter(out, out.end())))
        {
          for (auto& item : out)
          {
            if (item.second.size())
            {
              info.keyids[item.first] = item.second.back().first;

              //  when the first result with an id comes back set all '$' to that id
              //  this prevents missing other results on '$' while processing this one
              if (check_for_dollars)
              {
                const string& newid = item.second.back().first;
                for (auto& ki : info.keyids)
                {
                  if (ki.second[0] == '$') { ki.second = newid; }
                }
                check_for_dollars = false;
              }
            }

            if (info.subs.count(item.first))
            {
              auto split = split_key(item.first);
              for (auto& func : info.subs.at(item.first))
              {
                if (split.first.size())
                {
                  _replier_pool.job(item.first, [func, split = std::move(split), item = std::move(item)]()
                    { func(split.first, split.second, item.second); }
                  );
                }
                else
                {
                  _replier_pool.job(item.first, [func, item = std::move(item)]()
                    { func(item.first, item.first, item.second); }
                  );
                }
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
   bool nto = cv.wait_for(lk, THREAD_START_CONFIRM) == cv_status::no_timeout;
   if ( ! nto) syslog(LOG_WARNING, "start_reader timeout waiting for thread start");
   return nto;
}

bool RedisAdapter::stop_reader(uint32_t token)
{
  if (token == NO_TOKEN || _reader.count(token) == 0) return false;

  reader_info& info = _reader.at(token);
  if ( ! info.thread.joinable()) return false;

  info.run = false;
  Attrs attrs = default_field_attrs("");
  reconnect(_redis.xaddTrim(info.stop, "*", attrs.begin(), attrs.end(), 1).size());
  info.thread.join();
  return true;
}

//  lazy reconnect - any _redis operation that passes zero into this function
//    triggers a reconnect thread to launch (unless thread is already active)
//    on failure thread lingers for 100ms to throttle network connection requests
int32_t RedisAdapter::reconnect(int32_t result)
{
  if (result == 0 && _connecting.exchange(true) == false)
  {
    thread([&]()
      {
        if (_redis.connect(_options.cxn))
        {
          //  stop any waiting readers
          for (const auto& rdr : _reader) { stop_reader(rdr.first); }
          //  if any NO_TOKEN readers exist move them to valid tokens
          if (_reader.count(NO_TOKEN))
          {
            reader_info& tmp = _reader.at(NO_TOKEN);
            for (const auto& subs : tmp.subs)
            {
              string key = subs.first;
              uint32_t token = reader_token(key);
              reader_info& info = _reader[token];
              for (const auto& func : subs.second) { info.subs[key].push_back(func); }
              info.keyids[key] = "$";
              if (info.stop.empty())
              {
                auto part = split_key(key);
                info.stop = build_key(part.second + ":" + STOP_STUB, part.first);
                info.keyids[info.stop] = "$";
              }
              // syslog(LOG_INFO, "RedisAdapter::reconnect create %s token %u funcs %zu", key.c_str(), token, subs.second.size());
            }
            _reader.erase(NO_TOKEN);
          }
          //  restart all readers
          for (const auto& rdr : _reader) { start_reader(rdr.first); }
        }
        else
        {
          this_thread::sleep_for(milliseconds(100));  //  throttle failures
        }
        _connecting = false;  //  thread is done
      }
    ).detach();   //  cast new thread into the void
  }
  return result;
}
