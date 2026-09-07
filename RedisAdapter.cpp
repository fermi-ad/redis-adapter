//
//  RedisAdapter.cpp
//
//  This file contains the implementation of the RedisAdapter class

#include "RedisAdapter.hpp"
#include <algorithm>
#include <charconv>
#include <stdexcept>

using namespace std;
using namespace chrono;
using namespace sw::redis;

const uint32_t NANOS_PER_MILLI = 1'000'000;

const auto THREAD_START_CONFIRM = milliseconds(20);

const uint32_t NO_TOKEN = -1;

namespace {
// Preserve the actual Redis hash tag. Untagged keys containing braces cannot
// always be represented by a hash tag; those readers use their bounded timeout
// for shutdown instead of adding a control stream in a different cluster slot.
std::string stopStreamKey(const std::string& key) {
  const auto open = key.find('{');
  const auto close = open == std::string::npos ? std::string::npos : key.find('}', open + 1);
  if (close != std::string::npos && close > open + 1) return key + ":<$-STOP-$>";
  if (!key.empty() && key.find_first_of("{}") == std::string::npos)
    return "{" + key + "}:<$-STOP-$>";
  return {};
}
}


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
  value = 0;
  const auto dash = id.find('-');
  uint64_t millis = 0, remainder = 0;
  const auto end = id.data() + (dash == string::npos ? id.size() : dash);
  const auto first = from_chars(id.data(), end, millis);
  if (first.ec != errc{} || first.ptr != end) return;
  if (dash != string::npos) {
    const auto second = from_chars(id.data() + dash + 1, id.data() + id.size(), remainder);
    if (second.ec != errc{} || second.ptr != id.data() + id.size()) return;
  }
  const uint64_t maximum = numeric_limits<int64_t>::max();
  if (remainder > maximum || millis > (maximum - remainder) / NANOS_PER_MILLI) return;
  value = static_cast<int64_t>(millis * NANOS_PER_MILLI + remainder);
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
  _reader_owner->adapter = this;
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
  _shutdown = true;
  {
    lock_guard<mutex> lock(_reader_owner->mutex);
    _reader_owner->adapter = nullptr;
  }

  if (_watchdog_run.load())
  {
    _watchdog_run = false;
    _watchdog_cv.notify_all();
    _watchdog_thd.join();
  }

  if (_reconnect_thd.joinable()) _reconnect_thd.join();

  std::lock_guard<std::mutex> lk(_reader_mtx);
  for (auto& item : _reader) {
    stop_reader(item.first);
    for (auto& key : item.second.subs)
      for (auto& registration : key.second) registration->active = false;
  }
}

RedisAdapter::ReaderHandle::ReaderHandle(weak_ptr<ReaderOwner> owner,
                                         shared_ptr<ReaderRegistration> registration)
    : owner_(std::move(owner)), registration_(std::move(registration)) {}

RedisAdapter::ReaderHandle::~ReaderHandle() { reset(); }
RedisAdapter::ReaderHandle::ReaderHandle(ReaderHandle&& other) noexcept
    : owner_(std::move(other.owner_)), registration_(std::move(other.registration_)) {}
RedisAdapter::ReaderHandle& RedisAdapter::ReaderHandle::operator=(ReaderHandle&& other) noexcept {
  if (this != &other) {
    reset();
    owner_ = std::move(other.owner_);
    registration_ = std::move(other.registration_);
  }
  return *this;
}
RedisAdapter::ReaderHandle::operator bool() const {
  return registration_ && registration_->active.load();
}
void RedisAdapter::ReaderHandle::reset() noexcept {
  auto registration = std::move(registration_);
  if (!registration) return;
  registration->active = false;
  try {
    if (auto owner = owner_.lock()) {
      lock_guard<mutex> lock(owner->mutex);
      if (owner->adapter) owner->adapter->remove_registration(registration->id);
    }
  } catch (const exception& ex) {
    syslog(LOG_ERR, "remove stream subscription: %s", ex.what());
  }
  owner_.reset();
}

int RedisAdapter::compareStreamIds(const string& lhs, const string& rhs) {
  const auto parse = [](const string& id) {
    const auto dash = id.find('-');
    if (dash == string::npos) throw invalid_argument("invalid Redis stream ID: " + id);
    pair<uint64_t, uint64_t> parts{};
    const auto first = from_chars(id.data(), id.data() + dash, parts.first);
    const auto second = from_chars(id.data() + dash + 1, id.data() + id.size(), parts.second);
    if (first.ec != errc{} || first.ptr != id.data() + dash ||
        second.ec != errc{} || second.ptr != id.data() + id.size())
      throw invalid_argument("invalid Redis stream ID: " + id);
    return parts;
  };
  const auto a = parse(lhs), b = parse(rhs);
  return a < b ? -1 : b < a ? 1 : 0;
}

RedisAdapter::StreamSnapshot RedisAdapter::getStreamSnapshot(const string& subKey, const string& baseKey) {
  StreamSnapshot result;
  ItemStream items;
  result.connected = _redis.xrevrange(build_key(subKey, baseKey), "+", "-", 1, back_inserter(items));
  reconnect(result.connected);
  if (result.connected && !items.empty()) {
    result.id = std::move(items.front().first);
    result.fields = std::move(items.front().second);
  }
  return result;
}

RedisAdapter::ReaderHandle RedisAdapter::subscribeStream(const string& subKey, StreamCallback callback,
                                                         const string& afterId, const string& baseKey) {
  if (!callback) throw invalid_argument("empty stream callback");
  const auto base = baseKey.empty() ? _base_key : baseKey;
  return ReaderHandle(_reader_owner, register_reader(build_key(subKey, baseKey),
      [base, subKey, callback = std::move(callback)](const auto&, const auto&, const auto& batch) {
        callback(base, subKey, batch);
      }, afterId));
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

  string id = args.trim ? _redis.xaddTrim(key, args.time.id_or_now(), attrs.begin(), attrs.end(),
                                         args.trim, args.approximateTrim)
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
  std::lock_guard<std::mutex> lk(_reader_mtx);
  if (defer && ! _readers_defer.load())
  {
    _readers_defer = defer;
    for (auto& item : _reader) { stop_reader(item.first); }
  }
  else if ( ! defer && _readers_defer.load())
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
  if (split_key(key).first.size()) return false;
  register_reader(key, make_reader_callback(func), "$", false);
  return reader_token(key) != NO_TOKEN;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  removeGenericReader : remove all readers for a key that does NOT follow RedisAdapter schema
//
//    key     : the key to remove (must NOT be a RedisAdapter schema key)
//    return  : true if reader started, false if reader failed to start
//
bool RedisAdapter::removeGenericReader(const string& key)
{
  if (split_key(key).first.size()) return false;
  lock_guard<mutex> lock(_reader_mtx);
  bool found = false;
  for (auto it = _reader.begin(); it != _reader.end();) {
    auto& info = it->second;
    auto entry = info.subs.find(key);
    if (entry == info.subs.end()) { ++it; continue; }
    stop_reader(it->first);
    for (auto& registration : entry->second) registration->active = false;
    info.subs.erase(entry);
    info.keyids.erase(key);
    found = true;
    if (info.subs.empty()) it = _reader.erase(it);
    else { start_reader(it->first); ++it; }
  }
  return found;
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
  const auto key = build_key(subKey, baseKey);
  const auto base = baseKey.empty() ? _base_key : baseKey;
  register_reader(key, [base, subKey, func = std::move(func)](const auto&, const auto&, const auto& batch) {
    func(base, subKey, batch);
  }, "$", false);
  return reader_token(key) != NO_TOKEN;
}

shared_ptr<RedisAdapter::ReaderRegistration>
RedisAdapter::register_reader(const string& key, reader_sub_fn func, const string& afterId, bool resolveTail)
{
  if (!func) throw invalid_argument("empty stream callback");
  if (afterId != "$") compareStreamIds(afterId, "0-0");
  lock_guard<mutex> lock(_reader_mtx);
  if (_shutdown) throw runtime_error("adapter is shutting down");
  const auto token = reader_token(key);
  auto& info = _reader[token];
  stop_reader(token);
  auto registration = make_shared<ReaderRegistration>();
  registration->id = ++_next_reader_id;
  registration->cursor = afterId;
  registration->callback = std::move(func);
  if (resolveTail && registration->cursor == "$") {
    ItemStream latest;
    if (_redis.xrevrange(key, "+", "-", 1, back_inserter(latest)))
      registration->cursor = latest.empty() ? "0-0" : latest.front().first;
  }
  auto cursor = info.keyids.find(key);
  if (cursor == info.keyids.end() || cursor->second == "$" ||
      (registration->cursor != "$" && compareStreamIds(registration->cursor, cursor->second) < 0))
    info.keyids[key] = registration->cursor;
  info.subs[key].push_back(registration);
  if (info.stop.empty()) {
    info.stop = stopStreamKey(key);
    if (!info.stop.empty()) info.keyids[info.stop] = "$";
  }
  start_reader(token);
  return registration;
}

void RedisAdapter::remove_registration(uint64_t id)
{
  lock_guard<mutex> lock(_reader_mtx);
  for (auto bucket = _reader.begin(); bucket != _reader.end(); ++bucket) {
    auto& info = bucket->second;
    for (auto key = info.subs.begin(); key != info.subs.end(); ++key) {
      auto& registrations = key->second;
      auto registration = find_if(registrations.begin(), registrations.end(),
                                  [id](const auto& item) { return item->id == id; });
      if (registration == registrations.end()) continue;
      (*registration)->active = false;
      stop_reader(bucket->first);
      registrations.erase(registration);
      if (registrations.empty()) {
        info.keyids.erase(key->first);
        info.subs.erase(key);
      }
      if (info.subs.empty()) _reader.erase(bucket);
      else start_reader(bucket->first);
      return;
    }
  }
}

bool RedisAdapter::remove_reader_helper(const string& baseKey, const string& subKey)
{
  const auto key = build_key(subKey, baseKey);
  lock_guard<mutex> lock(_reader_mtx);
  bool found = false;
  for (auto bucket = _reader.begin(); bucket != _reader.end();) {
    auto& info = bucket->second;
    const auto entry = info.subs.find(key);
    if (entry == info.subs.end()) { ++bucket; continue; }
    stop_reader(bucket->first);
    for (auto& registration : entry->second) registration->active = false;
    info.subs.erase(entry);
    info.keyids.erase(key);
    found = true;
    if (info.subs.empty()) bucket = _reader.erase(bucket);
    else { start_reader(bucket->first); ++bucket; }
  }
  return found;
}

bool RedisAdapter::start_reader(uint32_t token)
{
  if (_readers_defer) return true;

  if (token == NO_TOKEN || _reader.count(token) == 0) return false;

  reader_info& info = _reader.at(token);

  if (info.thread.joinable()) return false;

  //  info.start_mx / info.start_cv live in reader_info (in the _reader map) for as long
  //  as the reader exists, which safely outlives this function whether or not the wait
  //  below times out - this avoids a dangling reference to locals that a late-scheduled
  //  thread might still touch after this function has already returned
  unique_lock<mutex> lk(info.start_mx);  //  must be locked before cv.wait_for()

  info.started = false;
  info.run = true;
  for (auto& subscriptions : info.subs) {
    bool needsTail = info.keyids.at(subscriptions.first) == "$";
    for (const auto& registration : subscriptions.second) {
      lock_guard<mutex> cursorLock(registration->mutex);
      needsTail = needsTail || registration->cursor == "$";
    }
    if (!needsTail) continue;
    ItemStream latest;
    if (!_redis.xrevrange(subscriptions.first, "+", "-", 1, back_inserter(latest))) continue;
    const auto tail = latest.empty() ? "0-0" : latest.front().first;
    if (info.keyids.at(subscriptions.first) == "$") info.keyids[subscriptions.first] = tail;
    for (const auto& registration : subscriptions.second) {
      lock_guard<mutex> cursorLock(registration->mutex);
      if (registration->cursor == "$") registration->cursor = tail;
    }
  }
  info.thread = thread([this, &info]() {
    {
      lock_guard<mutex> lock(info.start_mx);
      info.started = true;
    }
    info.start_cv.notify_all();
    for (Streams out; info.run; out.clear()) {
      if (!_redis.xreadMultiBlock(info.keyids.begin(), info.keyids.end(), _options.cxn.timeout,
                                  inserter(out, out.end()))) {
        // redis++ reconnects its socket on the next read. A transient error must
        // not permanently stop a reader while a separate health PING succeeds.
        if (info.run) this_thread::sleep_for(milliseconds(50));
        continue;
      }
      for (auto& item : out) {
        if (!item.second.empty()) info.keyids[item.first] = item.second.back().first;
        const auto subscriptions = info.subs.find(item.first);
        if (subscriptions == info.subs.end() || item.second.empty()) continue;
        const auto parts = split_key(item.first);
        const auto base = parts.first.empty() ? item.first : parts.first;
        const auto sub = parts.first.empty() ? item.first : parts.second;
        auto data = make_shared<const ItemStream>(std::move(item.second));
        for (const auto& registration : subscriptions->second) {
          _replier_pool.job(item.first, [registration, base, sub, data]() {
            if (!registration->active.load()) return;
            ItemStream fresh;
            {
              lock_guard<mutex> cursorLock(registration->mutex);
              for (const auto& entry : *data) {
                if (registration->cursor == "$" || compareStreamIds(entry.first, registration->cursor) > 0) {
                  fresh.push_back(entry);
                  registration->cursor = entry.first;
                }
              }
            }
            if (!fresh.empty() && registration->active.load())
              registration->callback(base, sub, fresh);
          });
        }
      }
    }
  });
  const bool nto = info.start_cv.wait_for(lk, THREAD_START_CONFIRM, [&]() { return info.started; });
  if (!nto) syslog(LOG_WARNING, "start_reader timeout waiting for thread start");
  return nto;
}

bool RedisAdapter::stop_reader(uint32_t token)
{
  if (token == NO_TOKEN || _reader.count(token) == 0) return false;

  reader_info& info = _reader.at(token);
  if ( ! info.thread.joinable()) return false;

  info.run = false;
  Attrs attrs = default_field_attrs("");
  //  poke the stop stream to unblock xreadMultiBlock - if it fails the reader
  //  will still exit after its timeout expires, do NOT call reconnect() here
  //  since stop_reader is called from within locked sections and spawning a
  //  reconnect thread could cause unnecessary blocking
  if (!info.stop.empty()) _redis.xaddTrim(info.stop, "*", attrs.begin(), attrs.end(), 1);
  info.thread.join();
  return true;
}

//  lazy reconnect - any _redis operation that passes zero into this function
//    triggers a reconnect thread to launch (unless thread is already active)
//    on failure thread lingers for 100ms to throttle network connection requests
int32_t RedisAdapter::reconnect(int32_t result)
{
  if (_shutdown) return result;

  if (result == 0 && _connecting.exchange(true) == false)
  {
    if (_reconnect_thd.joinable()) _reconnect_thd.join();

    _reconnect_thd = thread([this]()
      {
        if (_redis.connect(_options.cxn))
        {
          std::lock_guard<std::mutex> lk(_reader_mtx);

          //  stop any waiting readers
          for (const auto& rdr : _reader) { stop_reader(rdr.first); }
          //  if any NO_TOKEN readers exist move them to valid tokens
          if (_reader.count(NO_TOKEN))
          {
            //  move just the subs out (not the whole reader_info - it's non-movable
            //  now that it holds a mutex/condition_variable) and erase NO_TOKEN first,
            //  so that if reader_token() ever yields NO_TOKEN again below, _reader[token]
            //  creates a fresh entry instead of aliasing the map we're iterating over
            auto subs_map = std::move(_reader.at(NO_TOKEN).subs);
            auto cursors = std::move(_reader.at(NO_TOKEN).keyids);
            _reader.erase(NO_TOKEN);

            for (const auto& subs : subs_map)
            {
              string key = subs.first;
              uint32_t token = reader_token(key);
              reader_info& info = _reader[token];
              for (const auto& func : subs.second) { info.subs[key].push_back(func); }
              auto prior = info.keyids.find(key);
              const auto cursor = cursors.at(key);
              if (prior == info.keyids.end() || prior->second == "$" ||
                  (cursor != "$" && compareStreamIds(cursor, prior->second) < 0))
                info.keyids[key] = cursor;
              if (info.stop.empty())
              {
                info.stop = stopStreamKey(key);
                if (!info.stop.empty()) info.keyids[info.stop] = "$";
              }
            }
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
    );
  }
  return result;
}
