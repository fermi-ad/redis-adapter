//
//  RedisAdapter.cpp
//
//  This file contains the implementation of the RedisAdapter class

#include "RedisAdapter.hpp"

using namespace std;
using namespace chrono;
using namespace sw::redis;

const uint32_t NANOS_PER_MILLI = 1'000'000;
const uint64_t REMAINDER_SCALE = 10'000'000'000;

const auto THREAD_START_CONFIRM = milliseconds(20);

static uint64_t nanoseconds_since_epoch()
{
  return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RA_Time : constructor that converts an id string to nanoseconds and sequence number
//
//    id     : Redis ID string e.g. "12345-67089" where the first number is milliseconds since
//             epoch and the second number has nanoseconds remainder scaled by 1E10 added to a
//             sequence number, the scale factor is chosen as a power of ten to make the remainder
//             and sequence number human readable in the id string
//    return : RA_Time
//
RA_Time::RA_Time(const string& id)
{
  try
  {
    //  do the reverse of id() below
    uint64_t mixed = stoull(id.substr(id.find('-') + 1));
    nanos = stoull(id) * NANOS_PER_MILLI + mixed / REMAINDER_SCALE;
    seqnum = mixed % REMAINDER_SCALE;
  }
  catch (...) { nanos = seqnum = 0; }
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RA_Time::id : return Redis ID string
//
string RA_Time::id() const
{
  //  find the remainder nanoseconds (i.e. remove all the whole milliseconds) and
  //  place that at the REMAINDER_SCALE position in the base-ten representation
  //  of the number, then place the seqnum at the ones position in the base-ten
  //  representation of the number - this way the values will be human-readable
  uint64_t mixed = (nanos % NANOS_PER_MILLI) * REMAINDER_SCALE + seqnum;

  //  place the whole milliseconds on the left-hand side of the ID and the number
  //  from above on the right-hand side of the ID
  return to_string(nanos / NANOS_PER_MILLI) + "-" + to_string(mixed);
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
RedisAdapter::RedisAdapter(const string& baseKey, const RedisConnection::Options& options, const uint workerThreadCount)
: _options(options), _redis(options), _base_key(baseKey),
  _connecting(false), _listener_run(false), _readers_defer(false), workerThreads(workerThreadCount) {}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  ~RedisAdapter : destructor
//
RedisAdapter::~RedisAdapter()
{
  stop_listener();
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
  connect(id.size());
  return RA_Time(id);
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
  _command_subs[build_key(subKey, baseKey)].push_back(func);
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
  _pattern_subs[build_key(subKey, baseKey)].push_back(func);
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
  string key = build_key(subKey, baseKey);
  if (_pattern_subs.count(key)) _pattern_subs.erase(key);
  if (_command_subs.count(key)) _command_subs.erase(key);
  return (_pattern_subs.size() || _command_subs.size()) ? start_listener() : true;
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
  if (split_key(key).first.size()) return false;  //  reject if RedisAdapter key found

  int32_t slot = _redis.keyslot(key);
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
  if (split_key(key).first.size()) return false;  //  reject if RedisAdapter key found

  int32_t slot = _redis.keyslot(key);
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
  connect(ret != -1);
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
      Subscriber* psub = _redis.subscriber();
      if ( ! psub)
      {
        syslog(LOG_ERR, "failed to get subscriber");
        ret = false;      //  start_listener return false
        cv.notify_all();  //  notify cv
        return;           //  return from lambda
      }

      //  begin lambda in lambda ///////////////////////////
      psub->on_pmessage([&](string pat, string key, string msg)
        {
          if (_pattern_subs.count(pat))
          {
            auto split = split_key(key);
            for (auto& func : _pattern_subs.at(pat))
              { 
	        workerThreads.doJob([func, splitFirst = std::move(split.first), splitSecond = std::move(split.second), msg = std::move(msg)]() {
			         func(splitFirst, splitSecond, msg);}); 
	      }
          }
        }
      );  //  end lambda in lambda /////////////////////////

      //  begin lambda in lambda ///////////////////////////
      psub->on_message([&](string key, string msg)
        {
          if (_command_subs.count(key))
          {
            auto split = split_key(key);
            for (auto& func : _command_subs.at(key))
              { 
	        workerThreads.doJob([func, splitFirst = std::move(split.first), splitSecond = std::move(split.second), msg = std::move(msg)]() {
			         func(splitFirst, splitSecond, msg);}); 
	      }
          }
        }
      );  //  end lambda in lambda /////////////////////////

      for (const auto& cs : _command_subs) { psub->subscribe(cs.first); }

      for (const auto& ps : _pattern_subs) { psub->psubscribe(ps.first); }

      psub->subscribe(build_key(STOP_STUB));

      _listener_run = true;

      cv.notify_all();  //  notify about to enter loop (NOT in loop)

      while (_listener_run)
      {
        try { psub->consume(); }
        catch (const TimeoutError&) {}
        catch (const Error& e)
        {
          syslog(LOG_ERR, "consume in listener: %s", e.what());
          _listener_run = false;
        }
      }
      delete psub;
    }
  );  //  end lambda  ////////////////////////////////////////////////

  //  wait until notified that thread is running (or timeout)
  bool nto = cv.wait_for(lk, THREAD_START_CONFIRM) == cv_status::no_timeout;
  if ( ! nto) syslog(LOG_ERR, "start_listener timeout waiting for thread start");
  return nto && ret;
}

bool RedisAdapter::stop_listener()
{
  if ( ! _listener.joinable()) return false;
  _listener_run = false;
  connect(_redis.publish(build_key(STOP_STUB), "") != -1);
  _listener.join();
  return true;
}

bool RedisAdapter::add_reader_helper(const string& baseKey, const string& subKey, reader_sub_fn func)
{
  string key = build_key(subKey, baseKey);
  int32_t slot = _redis.keyslot(key);
  if (slot < 0) return false;

  stop_reader(slot);
  reader_info& info = _reader[slot];

  if (info.stop.empty())
  {
    info.stop = build_key(subKey + ":" + STOP_STUB, baseKey);
    info.keyids[info.stop] = "$";
  }
  info.subs[key].push_back(func);
  info.keyids[key] = "$";
  return start_reader(slot);
}

bool RedisAdapter::remove_reader_helper(const string& baseKey, const string& subKey)
{
  string key = build_key(subKey, baseKey);
  int32_t slot = _redis.keyslot(key);
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
  if (_readers_defer) return true;

  if (_reader.count(slot) == 0) return false;

  reader_info& info = _reader.at(slot);

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
        if (_redis.xreadMultiBlock(info.keyids.begin(), info.keyids.end(), _options.timeout, inserter(out, out.end())))
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
		  workerThreads.doJob([func, splitFirst = std::move(split.first), splitSecond = std::move(split.second), itemSecond = std::move(item.second)]() 
				  {func(splitFirst, splitSecond, itemSecond);});
	        }
		else 
		{  
		  workerThreads.doJob([func, itemFirst = std::move(item.first), itemSecond = std::move(item.second)]() 
				  {func(itemFirst, itemFirst, itemSecond);});
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
  connect(_redis.xaddTrim(info.stop, "*", attrs.begin(), attrs.end(), 1).size());
  info.thread.join();
  return true;
}

//  lazy reconnect - any _redis operation that passes zero into this function
//    triggers a reconnect thread to launch (unless thread is already active)
int32_t RedisAdapter::connect(int32_t result)
{
  if (result == 0 && _connecting.exchange(true) == false)
  {
    thread([&]()
      {
        if (_redis.connect(_options))
        {
          //  restart all the readers
          for (auto& rdr : _reader)
          {
            stop_reader(rdr.first);
            start_reader(rdr.first);
          }
          //  restart the listener
          stop_listener();
          start_listener();
        }
        _connecting = false;  //  thread is done
      }
    ).detach();   //  cast new thread into the void
  }
  return result;
}
