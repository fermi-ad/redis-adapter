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


//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  RedisAdapter : constructor
//
//    baseKey : base key of home device
//    options : struct of default values, override using per-field initializer list
//              e.g. { .user = "adinst", .password = "adinst" }
//    return  : RedisAdapter
//
RedisAdapter::RedisAdapter(const string& baseKey, const RA_Options& options) :
  _options(options), _redis(options.cxn, options.readers), _base_key(baseKey), _connecting(false),
  _watchdog_run(false), _readers_defer(false), _replier_pool(options.workers)
{
  _reader_owner->adapter = this;
  _watchdog_key = build_key("watchdog");

  if (_options.dogname.size())
  {
    _watchdog_run = true;
    _watchdog_thd = thread([&]()
      {
        mutex mx; unique_lock lk(mx);   //  dummies for _watchdog_cv

        addWatchdog(_options.dogname, 1);

        for (;      //  every 900ms set expire for 1000ms
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

  if (_watchdog_thd.joinable())
  {
    _watchdog_run = false;
    _watchdog_cv.notify_all();
    _watchdog_thd.join();
  }

  {
    lock_guard<mutex> reconnectLock(_reconnect_mtx);
    if (_reconnect_thd.joinable()) _reconnect_thd.join();
  }

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
RedisAdapter::ReaderStatus RedisAdapter::ReaderHandle::status() const {
  if (!registration_) return {};
  lock_guard<mutex> lock(registration_->mutex);
  auto result = registration_->status;
  result.active = registration_->active && !owner_.expired();
  result.cursor = registration_->cursor == "$" ? "" : registration_->cursor;
  result.observedCursor = registration_->observedCursor == "$" ? "" : registration_->observedCursor;
  if (!result.active) { result.connected = result.inspected = result.hasData = false; }
  return result;
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

RedisAdapter::StreamSnapshot RedisAdapter::getStreamSnapshot(const string& subKey, const string& baseKey) {
  StreamSnapshot result;
  ItemStream items;
  RedisConnection::ReadStatus status;
  result.connected = _redis.xrevrange(build_key(subKey, baseKey), "+", "-", 1, back_inserter(items), &status);
  result.rejected = status == RedisConnection::ReadStatus::Rejected;
  if (status == RedisConnection::ReadStatus::Unavailable) reconnect(false);
  if (result.connected) {
    result.id = items.empty() ? "0-0" : std::move(items.front().first);
    if (!items.empty()) result.fields = std::move(items.front().second);
  }
  return result;
}

RedisAdapter::ReaderHandle RedisAdapter::subscribeStream(const string& subKey, StreamCallback callback,
                                                         const string& afterId, const string& baseKey, uint32_t probeMs) {
  if (!callback) throw invalid_argument("empty stream callback");
  const auto base = baseKey.empty() ? _base_key : baseKey;
  return ReaderHandle(_reader_owner, register_reader(build_key(subKey, baseKey),
      [base, subKey, callback = std::move(callback)](const auto&, const auto&, const auto& batch) {
        callback(base, subKey, batch);
      }, afterId, true, probeMs));
}

RedisAdapter::ReaderHandle RedisAdapter::subscribeStreamWithEpoch(const string& subKey, EpochStreamCallback callback,
                                                                  const SubscriptionOptions& options) {
  if (!callback) throw invalid_argument("empty stream callback");
  const auto base = options.baseKey.empty() ? _base_key : options.baseKey;
  auto wrapped = [base, subKey, callback = std::move(callback)](const auto&, const auto&, const auto& batch, uint64_t epoch) {
    callback(base, subKey, batch, epoch);
  };
  return ReaderHandle(_reader_owner, register_reader(build_key(subKey, options.baseKey),
      [](const auto&, const auto&, const auto&) {}, options.afterId, true,
      options.probeMs.value_or(UINT32_MAX), std::move(wrapped)));
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

  const auto result = args.trim ? _redis.xaddTrimResult(key, args.time.id_or_now(), attrs.begin(), attrs.end(),
                                         args.trim, args.approximateTrim)
                               : _redis.xaddResult(key, args.time.id_or_now(), attrs.begin(), attrs.end());

  return finishWrite(result);
}

RA_Time RedisAdapter::finishWrite(const RedisConnection::WriteResult& result)
{
  switch (result.status) {
  case RedisConnection::CommandStatus::Accepted: return RA_Time(result.id);
  case RedisConnection::CommandStatus::Rejected:
    if (result.refreshConnection) reconnect(0);
    return RA_REJECTED;
  case RedisConnection::CommandStatus::Unavailable:
    reconnect(0);
    return RA_NOT_CONNECTED;
  }
  return RA_NOT_CONNECTED;
}

void RedisAdapter::finishBatch(const string& key, size_t accepted, uint32_t trim,
                               bool refreshNeeded, bool approximateTrim)
{
  if (trim && accepted) {
    const auto result = _redis.xtrimResult(key,
        std::max(trim, static_cast<uint32_t>(std::min<size_t>(accepted, UINT32_MAX))), approximateTrim);
    refreshNeeded |= result.status == RedisConnection::CommandStatus::Unavailable || result.refreshConnection;
    if (result.status == RedisConnection::CommandStatus::Rejected)
      syslog(LOG_WARNING, "accepted batch entries retained, but final trim rejected: %s", result.error.c_str());
  }
  // Accepted timestamps survive item/trim failures. Refresh prepares later
  // calls; no ambiguous write or trim is replayed on a standalone connection.
  if (refreshNeeded) reconnect(0);
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
  vector<vector<shared_ptr<ReaderRegistration>>> retired;
  lock_guard<mutex> lock(_reader_mtx);
  bool found = false;
  for (auto it = _reader.begin(); it != _reader.end();) {
    auto& info = it->second;
    auto entry = info.subs.find(key);
    if (entry == info.subs.end()) { ++it; continue; }
    stop_reader(it->first);
    for (auto& registration : entry->second) registration->active = false;
    retired.push_back(std::move(entry->second));
    info.subs.erase(entry);
    info.keyids.erase(key);
    info.boundaries.erase(key);
    info.quarantined.erase(key);
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
  const auto prefix = "{" + _base_key + "}";
  if (key.rfind(prefix, 0) != 0) return {};
  if (key.size() == prefix.size()) return {_base_key, ""};
  if (key[prefix.size()] != ':') return {};
  return {_base_key, key.substr(prefix.size() + 1)};
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
RedisAdapter::register_reader(const string& key, reader_sub_fn func, const string& afterId, bool resolveTail, uint32_t probeMs, EpochStreamCallback epochCallback)
{
  if (!func) throw invalid_argument("empty stream callback");
  if (afterId != "$") compareStreamIds(afterId, "0-0");
  auto registration = make_shared<ReaderRegistration>();
  registration->cursor = afterId;
  registration->callback = std::move(func);
  registration->epochCallback = std::move(epochCallback);
  registration->probeMs = resolveTail ? (probeMs == UINT32_MAX ? _options.readerProbeMs : probeMs) : 0;
  registration->status.epoch = 1;
  uint32_t token;
  {
    lock_guard<mutex> lock(_reader_mtx);
    if (_shutdown) throw runtime_error("adapter is shutting down");
    token = reader_token(key);
    auto& info = _reader[token];
    stop_reader(token);
    const auto old = info.keyids.find(key);
    const bool hadCursor = old != info.keyids.end();
    const string oldCursor = hadCursor ? old->second : string{};
    registration->id = ++_next_reader_id;
    try {
      // Legacy deferred readers resolve at start; owned readers resolve at
      // registration even when reads are deferred.
      if (resolveTail && registration->cursor == "$") {
        ItemStream latest;
        RedisConnection::ReadStatus status;
        bool wrongType = false;
        bool resolved = _redis.xrevrange(key, "+", "-", 1, back_inserter(latest), &status, &wrongType);
        if (!resolved && status == RedisConnection::ReadStatus::Rejected && !wrongType) {
          Streams tails;
          resolved = _redis.xreadTail(key, inserter(tails, tails.end()), &status, &wrongType);
          auto tail = tails.find(key);
          if (tail != tails.end()) latest = std::move(tail->second);
        }
        if (resolved || wrongType) registration->cursor = latest.empty() ? "0-0" : latest.back().first;
      }
      if (!hadCursor || oldCursor == "$" ||
          (registration->cursor != "$" && compareStreamIds(registration->cursor, oldCursor) < 0))
        info.keyids[key] = registration->cursor;
      registration->observedCursor = registration->cursor;
      registration->status.connected = info.readConnected;
      registration->everConnected = info.readConnected;
      info.subs[key].push_back(registration);
      if (info.stop.empty()) {
        info.stop = stopStreamKey(key);
        if (!info.stop.empty()) info.keyids[info.stop] = "0-0";
      }
      if (!start_reader(token) && token != NO_TOKEN && !_readers_defer && !_shutdown)
        throw runtime_error("reader thread did not start");
    } catch (...) {
      registration->active = false;
      stop_reader(token);
      const auto subscriptions = info.subs.find(key);
      if (subscriptions != info.subs.end()) {
        auto& registered = subscriptions->second;
        registered.erase(remove(registered.begin(), registered.end(), registration), registered.end());
        if (registered.empty()) info.subs.erase(subscriptions);
      }
      if (hadCursor) info.keyids[key] = oldCursor;
      else info.keyids.erase(key);
      if (info.subs.empty()) _reader.erase(token);
      else { try { start_reader(token); } catch (...) {} }
      throw;
    }
  }
  if (token == NO_TOKEN) reconnect(false);
  return registration;
}

void RedisAdapter::remove_registration(uint64_t id)
{
  shared_ptr<ReaderRegistration> retired;
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
      retired = std::move(*registration);
      registrations.erase(registration);
      if (registrations.empty()) {
        info.keyids.erase(key->first);
        info.boundaries.erase(key->first);
        info.quarantined.erase(key->first);
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
  vector<vector<shared_ptr<ReaderRegistration>>> retired;
  lock_guard<mutex> lock(_reader_mtx);
  bool found = false;
  for (auto bucket = _reader.begin(); bucket != _reader.end();) {
    auto& info = bucket->second;
    const auto entry = info.subs.find(key);
    if (entry == info.subs.end()) { ++bucket; continue; }
    stop_reader(bucket->first);
    for (auto& registration : entry->second) registration->active = false;
    retired.push_back(std::move(entry->second));
    info.subs.erase(entry);
    info.keyids.erase(key);
    info.boundaries.erase(key);
    info.quarantined.erase(key);
    found = true;
    if (info.subs.empty()) bucket = _reader.erase(bucket);
    else { start_reader(bucket->first); ++bucket; }
  }
  return found;
}

size_t RedisAdapter::resolve_reader_tails(reader_info& info)
{
  size_t unresolved = 0;
  for (auto& subscriptions : info.subs) {
    bool needsTail = info.keyids.at(subscriptions.first) == "$";
    for (const auto& registration : subscriptions.second) {
      lock_guard<mutex> cursorLock(registration->mutex);
      needsTail = needsTail || registration->cursor == "$";
    }
    if (!needsTail) continue;
    ItemStream latest;
    RedisConnection::ReadStatus status;
    bool wrongType = false;
    bool resolved = _redis.xrevrange(subscriptions.first, "+", "-", 1, back_inserter(latest), &status, &wrongType);
    if (!resolved && status == RedisConnection::ReadStatus::Rejected && !wrongType) {
      Streams tails;
      resolved = _redis.xreadTail(subscriptions.first, inserter(tails, tails.end()), &status, &wrongType);
      const auto tail = tails.find(subscriptions.first);
      if (tail != tails.end()) latest = std::move(tail->second);
    }
    if (!resolved && !wrongType) { ++unresolved; continue; }
    const auto tail = latest.empty() ? "0-0" : latest.back().first;
    if (info.keyids.at(subscriptions.first) == "$") info.keyids[subscriptions.first] = tail;
    for (const auto& registration : subscriptions.second) {
      lock_guard<mutex> cursorLock(registration->mutex);
      if (registration->cursor == "$") registration->cursor = registration->observedCursor = tail;
    }
  }
  return unresolved;
}

void RedisAdapter::reader_result(reader_info& info, RedisConnection::ReadStatus result, bool socketTimedOut)
{
  using Result = RedisConnection::ReadStatus;
  const bool connected = result == Result::Accepted || result == Result::TimedOut;
  if (connected && info.readConnected) return;
  if (result == Result::Rejected) return; // identify the rejected key on the read path
  for (const auto& key : info.subs) for (const auto& registration : key.second) {
    lock_guard<mutex> lock(registration->mutex);
    auto& status = registration->status;
    if (connected) {
      if (!status.connected && registration->everConnected) ++status.reconnects;
      status.connected = true; registration->everConnected = true;
    } else {
      ++status.readFailures;
      if (socketTimedOut) ++status.socketTimeouts;
      status.connected = false; registration->continuityCheck = true;
    }
  }
  info.readConnected = connected;
}

void RedisAdapter::reset_stream(reader_info& info, const string& key, StreamKind kind, bool allReaders)
{
  string minimum = "$";
  for (const auto& registration : info.subs.at(key)) {
    lock_guard<mutex> lock(registration->mutex);
    auto& status = registration->status;
    if (allReaders || registration->probeMs) {
      const bool hadCursor = registration->observedCursor != "$" && registration->observedCursor != "0-0";
      if (hadCursor || status.streamKind == StreamKind::Stream) {
        ++status.epoch; ++status.streamResets;
        if (kind == StreamKind::Missing) ++status.disappearances;
      }
      registration->cursor = registration->observedCursor = "0-0";
      registration->gapSignature.clear();
      status.hasData = false; status.lastReceived = {};
    }
    status.streamKind = kind;
    if (registration->cursor != "$" && (minimum == "$" || compareStreamIds(registration->cursor, minimum) < 0))
      minimum = registration->cursor;
  }
  info.keyids[key] = minimum;
}

void RedisAdapter::check_readable(reader_info& info, const vector<string>& keys)
{
  if (!info.run || keys.empty()) return;
  const auto checked = _redis.probeReadable(keys);
  if (checked.status == RedisConnection::ReadStatus::Unavailable) {
    reader_result(info, checked.status); return;
  }
  const auto now = steady_clock::now();
  if (checked.status == RedisConnection::ReadStatus::Accepted) {
    for (const auto& key : keys) {
      const auto bad = info.quarantined.find(key);
      if (bad != info.quarantined.end()) {
        if (bad->second.wrongType) reset_stream(info, key, StreamKind::Unknown, true);
        info.quarantined.erase(bad);
      }
      for (const auto& registration : info.subs.at(key)) {
        lock_guard<mutex> lock(registration->mutex);
        if (!registration->status.connected && registration->everConnected) ++registration->status.reconnects;
        registration->status.connected = true; registration->everConnected = true;
      }
    }
    return;
  }
  // A command-wide ACL refusal needs no per-key search. Otherwise split the
  // group to isolate bad keys without K serial round trips for one bad key.
  const bool commandDenied = checked.error.rfind("NOPERM", 0) == 0 &&
      checked.error.find("to run the 'xread' command") != string::npos;
  if (keys.size() > 1 && !commandDenied) {
    const auto middle = keys.begin() + keys.size() / 2;
    check_readable(info, vector<string>(keys.begin(), middle));
    check_readable(info, vector<string>(middle, keys.end()));
    return;
  }
  for (const auto& key : keys) {
    const auto inserted = info.quarantined.emplace(key, reader_info::Quarantine{});
    auto& bad = inserted.first->second;
    if (checked.wrongType && (inserted.second || !bad.wrongType)) reset_stream(info, key, StreamKind::Invalid, true);
    bad.wrongType |= checked.wrongType;
    bad.nextCheck = now + milliseconds(checked.wrongType ? 100 : 1000);
    if (inserted.second) syslog(LOG_WARNING, "stream key quarantined after read rejection: %s", key.c_str());
    for (const auto& registration : info.subs.at(key)) {
      lock_guard<mutex> lock(registration->mutex);
      ++registration->status.readRejections;
      registration->status.connected = true; registration->everConnected = true;
    }
  }
}

void RedisAdapter::prepare_probes(reader_info& info)
{
  info.probes = {};
  const auto now = steady_clock::now();
  for (const auto& key : info.subs) {
    uint32_t interval = UINT32_MAX;
    for (const auto& registration : key.second) if (registration->probeMs) interval = min(interval, registration->probeMs);
    auto& boundary = info.boundaries[key.first];
    if (interval == UINT32_MAX) { boundary.intervalMs = 0; continue; }
    if (!boundary.intervalMs || boundary.denied || boundary.nextProbe == steady_clock::time_point{}) {
      boundary.nextProbe = now; boundary.backoff = 1; boundary.denied = false;
    }
    boundary.intervalMs = interval;
    info.probes.push({boundary.nextProbe, ++info.probeOrder, key.first});
  }
}

void RedisAdapter::apply_bounds(reader_info& info, const string& key, const RedisConnection::StreamBounds& bounds)
{
  using Result = RedisConnection::CommandStatus;
  if (bounds.status != Result::Accepted) {
    for (const auto& registration : info.subs.at(key)) if (registration->probeMs) {
      lock_guard<mutex> lock(registration->mutex);
      registration->status.inspected = false;
      if (bounds.status == Result::Rejected) ++registration->status.inspectionRejections;
    }
    return;
  }
  bool reset = bounds.kind == StreamKind::Missing || bounds.kind == StreamKind::Invalid;
  if (bounds.kind == StreamKind::Stream) {
    for (const auto& registration : info.subs.at(key)) if (registration->probeMs) {
      lock_guard<mutex> lock(registration->mutex);
      if (registration->observedCursor != "$" && registration->observedCursor != "0-0" &&
          compareStreamIds(bounds.lastGeneratedId, registration->observedCursor) < 0) reset = true;
    }
  }
  if (reset) reset_stream(info, key, bounds.kind, bounds.kind == StreamKind::Invalid);
  if (bounds.kind == StreamKind::Invalid) info.quarantined[key] = {steady_clock::now() + milliseconds(100), true};
  for (const auto& registration : info.subs.at(key)) if (registration->probeMs) {
    lock_guard<mutex> lock(registration->mutex);
    auto& status = registration->status;
    status.inspected = true; status.streamKind = bounds.kind;
    status.hasData = bounds.kind == StreamKind::Stream && bounds.firstId != "0-0";
    const auto& cursor = registration->observedCursor;
    const bool hasCursor = cursor != "$" && cursor != "0-0";
    const bool gap = !reset && bounds.kind == StreamKind::Stream && hasCursor &&
        compareStreamIds(bounds.lastGeneratedId, cursor) > 0 &&
        (bounds.firstId == "0-0" || compareStreamIds(bounds.firstId, cursor) > 0);
    const auto signature = gap ? cursor + ":" + bounds.firstId + ":" + bounds.lastGeneratedId : string{};
    if (gap && signature != registration->gapSignature) ++status.retentionGaps;
    registration->gapSignature = signature;
    registration->continuityCheck = false;
  }
}

void RedisAdapter::inspect_readers(reader_info& info)
{
  constexpr size_t maximum = 16;
  vector<string> due;
  const auto now = steady_clock::now();
  while (!info.probes.empty() && info.probes.top().due <= now && due.size() < maximum && info.run) {
    auto ticket = info.probes.top(); info.probes.pop();
    auto& boundary = info.boundaries.at(ticket.key);
    if (!boundary.intervalMs) continue;
    bool continuity = false;
    for (const auto& registration : info.subs.at(ticket.key)) if (registration->probeMs) {
      lock_guard<mutex> lock(registration->mutex);
      continuity |= registration->continuityCheck;
    }
    if (!continuity && boundary.readVersion != boundary.lastProbeVersion) {
      boundary.lastProbeVersion = boundary.readVersion; boundary.backoff = 1;
      boundary.nextProbe = now + milliseconds(boundary.intervalMs);
      info.probes.push({boundary.nextProbe, ++info.probeOrder, ticket.key});
    } else due.push_back(std::move(ticket.key));
  }
  if (due.empty() || !info.run) return;
  const auto bounds = _redis.streamBoundsBatch(due);
  bool transportFailure = false;
  for (size_t index = 0; index < due.size(); ++index) {
    const auto& key = due[index];
    auto& boundary = info.boundaries.at(key);
    const auto& result = bounds[index];
    apply_bounds(info, key, result);
    boundary.lastProbeVersion = boundary.readVersion;
    if (result.status == RedisConnection::CommandStatus::Rejected) {
      boundary.denied = true;
      boundary.nextProbe = steady_clock::now() + seconds(60);
    } else {
      transportFailure |= result.status == RedisConnection::CommandStatus::Unavailable;
      boundary.backoff = min<uint32_t>(8, boundary.backoff * 2);
      boundary.nextProbe = steady_clock::now() + milliseconds(uint64_t(boundary.intervalMs) * boundary.backoff);
    }
    info.probes.push({boundary.nextProbe, ++info.probeOrder, key});
  }
  if (transportFailure) {
    // Inspection transport failure affects every registration in the bucket,
    // independently of which pipelined command first observed it.
    for (const auto& key : info.subs) for (const auto& registration : key.second) {
      lock_guard<mutex> lock(registration->mutex);
      ++registration->status.inspectionFailures;
      registration->status.inspected = false; registration->status.connected = false;
      registration->continuityCheck = true;
    }
    info.readConnected = false;
  }
}

uint32_t RedisAdapter::read_interval(const reader_info& info, bool& blocking) const
{
  uint32_t interval = _options.cxn.timeout ? min<uint32_t>(1000, _options.cxn.timeout) : 1000;
  const auto now = steady_clock::now();
  blocking = true;
  const auto shorten = [&](steady_clock::time_point due) {
    if (due <= now) { blocking = false; interval = 1; }
    else interval = min<uint32_t>(interval, max<int64_t>(1, duration_cast<milliseconds>(due - now).count()));
  };
  if (!info.probes.empty()) shorten(info.probes.top().due);
  for (const auto& bad : info.quarantined) shorten(bad.second.nextCheck);
  return interval;
}

bool RedisAdapter::start_reader(uint32_t token)
{
  if (_shutdown) return false;
  if (_readers_defer) return true;
  if (token == NO_TOKEN || _reader.count(token) == 0) return false;
  reader_info& info = _reader.at(token);
  if (info.thread.joinable()) return false;
  unique_lock<mutex> lk(info.start_mx);
  info.started = false;
  info.run = true;
  size_t unresolved = resolve_reader_tails(info);
  prepare_probes(info);
  try {
    info.thread = thread([this, &info, unresolved]() mutable {
      {
        lock_guard<mutex> lock(info.start_mx);
        info.started = true;
      }
      info.start_cv.notify_all();
      uint32_t delay = 50;
      bool reported = false;
      vector<string> allKeys;
      allKeys.reserve(info.subs.size());
      for (const auto& key : info.subs) allKeys.push_back(key.first);
      for (Streams out; info.run; out.clear()) {
        if (unresolved) unresolved = resolve_reader_tails(info);
        vector<string> recheck;
        const auto now = steady_clock::now();
        for (const auto& bad : info.quarantined) if (bad.second.nextCheck <= now) recheck.push_back(bad.first);
        if (!recheck.empty()) check_readable(info, recheck);
        inspect_readers(info);
        if (!info.run) break;
        // Never send '$' repeatedly. An unresolved tail is excluded until the
        // snapshot succeeds; healthy keys in the bucket continue to flow.
        const auto* cursors = &info.keyids;
        unordered_map<string, string> resolved;
        if (unresolved || !info.quarantined.empty() || !info.controlReadable) {
          for (const auto& key : info.keyids)
            if (key.second != "$" && !info.quarantined.count(key.first) &&
                (info.controlReadable || key.first != info.stop)) resolved.insert(key);
          cursors = &resolved;
        }
        RedisConnection::ReadStatus status = RedisConnection::ReadStatus::TimedOut;
        bool socketTimedOut = false, blocking;
        const auto interval = read_interval(info, blocking);
        const bool accepted = cursors->empty() || _redis.xreadMultiBlock(cursors->begin(), cursors->end(),
            interval, inserter(out, out.end()), &status, _options.readerBatchCount, &socketTimedOut, blocking);
        reader_result(info, status, socketTimedOut);
        if (!accepted) {
          if (status == RedisConnection::ReadStatus::Rejected) {
            const auto previous = info.quarantined.size();
            check_readable(info, allKeys);
            if (previous == info.quarantined.size() && !info.stop.empty() && info.controlReadable) {
              const auto control = _redis.probeReadable({info.stop});
              if (control.status == RedisConnection::ReadStatus::Rejected) info.controlReadable = false;
            }
          }
          if (!reported) { syslog(LOG_WARNING, "stream reader paused after read rejection or transport failure"); reported = true; }
          if (info.run) this_thread::sleep_for(milliseconds(delay));
          delay = min<uint32_t>(1000, delay * 2);
          continue;
        }
        if (cursors->empty()) this_thread::sleep_for(milliseconds(min<uint32_t>(interval, 50)));
        if (reported) syslog(LOG_INFO, "stream reader recovered");
        reported = false;
        delay = 50;
        for (auto& item : out) {
          if (!item.second.empty()) info.keyids[item.first] = item.second.back().first;
          const auto subscriptions = info.subs.find(item.first);
          if (subscriptions == info.subs.end() || item.second.empty()) continue;
          const auto parts = split_key(item.first);
          const auto base = parts.first.empty() ? item.first : parts.first;
          const auto sub = parts.first.empty() ? item.first : parts.second;
          auto data = make_shared<const ItemStream>(std::move(item.second));
          ++info.boundaries[item.first].readVersion;
          for (const auto& registration : subscriptions->second) {
            uint64_t epoch;
            {
              lock_guard<mutex> lock(registration->mutex);
              epoch = registration->status.epoch;
              if (registration->observedCursor == "$" || compareStreamIds(data->back().first, registration->observedCursor) > 0)
                registration->observedCursor = data->back().first;
              registration->status.streamKind = StreamKind::Stream;
              registration->status.hasData = true;
            }
            _replier_pool.job(item.first, [registration, base, sub, data, epoch]() {
              if (!registration->active.load()) return;
              size_t first = 0;
              {
                lock_guard<mutex> cursorLock(registration->mutex);
                if (registration->status.epoch != epoch) return;
                while (first < data->size() && registration->cursor != "$" &&
                       compareStreamIds((*data)[first].first, registration->cursor) <= 0) ++first;
                if (first == data->size()) return;
                registration->cursor = data->back().first;
              }
              if (!registration->active.load()) return;
              {
                lock_guard<mutex> lock(registration->mutex);
                if (registration->status.epoch != epoch) return;
                ++registration->status.callbacks;
                registration->status.entries += data->size() - first;
                registration->status.lastReceived = steady_clock::now();
              }
              const auto invoke = [&](const ItemStream& batch) {
                if (registration->epochCallback) registration->epochCallback(base, sub, batch, epoch);
                else registration->callback(base, sub, batch);
              };
              try {
                if (first == 0) invoke(*data);
                else { const ItemStream fresh(data->begin() + first, data->end()); invoke(fresh); }
              } catch (...) {
                lock_guard<mutex> lock(registration->mutex);
                ++registration->status.callbackErrors;
                throw;
              }
            });
          }
        }
      }
    });
  } catch (...) { info.run = false; throw; }
  const bool started = info.start_cv.wait_for(lk, THREAD_START_CONFIRM, [&]() { return info.started; });
  if (!started) syslog(LOG_WARNING, "start_reader timeout waiting for thread start");
  // A late-scheduled but successfully created thread is still a valid reader.
  return true;
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
  lock_guard<mutex> reconnectLock(_reconnect_mtx);
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
                if (!info.stop.empty()) info.keyids[info.stop] = "0-0";
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
