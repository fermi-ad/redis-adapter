//
//  RedisAdapterLite.cpp
//
//  Main implementation of RedisAdapterLite
//

#include "RedisAdapterLite.hpp"
#include <syslog.h>
#include <chrono>
#include <future>
#include <algorithm>

using namespace std::chrono;

static const uint32_t NO_TOKEN = static_cast<uint32_t>(-1);

//===========================================================================
//  Construction / Destruction
//===========================================================================

RedisAdapterLite::RedisAdapterLite(const std::string& baseKey, const RAL_Options& options)
  : _options(options), _redis(options), _base_key(baseKey), _worker_pool(options.workers)
{
  _watchdog_key = build_key("watchdog");

  if (_options.dogname.size())
  {
    // Set run flag BEFORE creating thread so the destructor can always
    // signal shutdown, even if it runs before the thread starts.
    _watchdog_run = true;

    _watchdog_thd = std::thread([this]()
    {
      std::unique_lock<std::mutex> lk(_watchdog_mutex);

      if (!addWatchdog(_options.dogname, 1))
        syslog(LOG_WARNING, "RedisAdapterLite: failed to register watchdog '%s'",
               _options.dogname.c_str());

      while (_watchdog_run)
      {
        if (_watchdog_cv.wait_for(lk, milliseconds(900)) == std::cv_status::timeout)
        {
          if (!petWatchdog(_options.dogname, 1))
            syslog(LOG_WARNING, "RedisAdapterLite: failed to pet watchdog '%s'",
                   _options.dogname.c_str());
        }
      }
    });
  }
}

RedisAdapterLite::~RedisAdapterLite()
{
  // Stop watchdog — hold mutex to synchronize with the watchdog thread's
  // condition variable wait, preventing missed wakeups
  {
    std::lock_guard<std::mutex> lk(_watchdog_mutex);
    _watchdog_run = false;
  }
  _watchdog_cv.notify_all();
  if (_watchdog_thd.joinable()) _watchdog_thd.join();

  // Stop all readers
  {
    std::lock_guard<std::mutex> lk(_reader_mutex);
    for (auto& [token, _] : _readers) stop_reader(token);
  }

  // Stop subscriber
  stop_subscriber();

  // Join reconnect thread
  if (_reconnect_thread.joinable()) _reconnect_thread.join();
}

//===========================================================================
//  Key building
//===========================================================================

std::string RedisAdapterLite::build_key(const std::string& subKey,
                                         const std::string& baseKey) const
{
  const std::string& base = baseKey.empty() ? _base_key : baseKey;
  return subKey.empty() ? base : base + ":" + subKey;
}

std::pair<std::string, std::string> RedisAdapterLite::split_key(const std::string& key) const
{
  size_t idx = key.find(_base_key);
  size_t len = _base_key.size();

  if (idx == std::string::npos) return {};

  return std::make_pair(
    key.substr(idx, len),
    key.size() > idx + len + 1 ? key.substr(idx + len + 1) : "");
}

//===========================================================================
//  Internal stream helpers
//===========================================================================

RAL_Time RedisAdapterLite::add_entry(const std::string& key, const Attrs& attrs,
                                      const RAL_AddArgs& args)
{
  std::string id = args.trim
    ? _redis.xadd_trim(key, args.time.id_or_now(), attrs, args.trim)
    : _redis.xadd(key, args.time.id_or_now(), attrs);

  if (check_reconnect(id.size()) == 0) return RAL_NOT_CONNECTED;
  return RAL_Time(id);
}

RAL_Time RedisAdapterLite::add_entry_single(const std::string& key,
                                             const char* data, size_t size,
                                             const RAL_AddArgs& args)
{
  static constexpr const char FIELD[] = "_";
  std::string id = args.trim
    ? _redis.xadd_trim_single(key, args.time.id_or_now(), FIELD, 1, data, size, args.trim)
    : _redis.xadd_single(key, args.time.id_or_now(), FIELD, 1, data, size);

  if (check_reconnect(id.size()) == 0) return RAL_NOT_CONNECTED;
  return RAL_Time(id);
}

TimeAttrsList RedisAdapterLite::get_forward(const std::string& key,
                                             RAL_Time minTime, RAL_Time maxTime,
                                             uint32_t count)
{
  std::string minID = minTime.id_or_min();
  std::string maxID = maxTime.id_or_max();

  TimeAttrsList raw;
  if (count)
    raw = _redis.xrange(key, minID, maxID, count);
  else
    raw = _redis.xrange(key, minID, maxID);

  // Only trigger reconnect if the connection is actually down.
  // An empty result is valid (no entries in range).
  check_reconnect(_redis.is_connected() ? 1 : 0);
  return raw;
}

TimeAttrsList RedisAdapterLite::get_reverse(const std::string& key,
                                             RAL_Time maxTime, uint32_t count)
{
  std::string maxID = maxTime.id_or_max();

  TimeAttrsList raw;
  if (count)
    raw = _redis.xrevrange(key, maxID, "-", count);
  else
    raw = _redis.xrevrange(key, maxID, "-");

  // Only trigger reconnect if the connection is actually down.
  // An empty result is valid (no entries in range).
  check_reconnect(_redis.is_connected() ? 1 : 0);
  return raw;
}

//===========================================================================
//  Add methods
//===========================================================================

RAL_Time RedisAdapterLite::addString(const std::string& subKey, const std::string& data,
                                      const RAL_AddArgs& args)
{
  return add_entry_single(build_key(subKey), data.data(), data.size(), args);
}

RAL_Time RedisAdapterLite::addDouble(const std::string& subKey, double data,
                                      const RAL_AddArgs& args)
{
  char buf[sizeof(double)];
  std::memcpy(buf, &data, sizeof(double));
  return add_entry_single(build_key(subKey), buf, sizeof(double), args);
}

RAL_Time RedisAdapterLite::addInt(const std::string& subKey, int64_t data,
                                   const RAL_AddArgs& args)
{
  char buf[sizeof(int64_t)];
  std::memcpy(buf, &data, sizeof(int64_t));
  return add_entry_single(build_key(subKey), buf, sizeof(int64_t), args);
}

RAL_Time RedisAdapterLite::addBlob(const std::string& subKey, const void* data, size_t size,
                                    const RAL_AddArgs& args)
{
  return add_entry_single(build_key(subKey),
                          static_cast<const char*>(data), size, args);
}

RAL_Time RedisAdapterLite::addAttrs(const std::string& subKey, const Attrs& data,
                                     const RAL_AddArgs& args)
{
  return add_entry(build_key(subKey), data, args);
}

//===========================================================================
//  Get single value (most recent at or before maxTime)
//===========================================================================

RAL_Time RedisAdapterLite::getString(const std::string& subKey, std::string& dest,
                                      const RAL_GetArgs& args)
{
  auto raw = get_reverse(build_key(subKey, args.baseKey), args.maxTime, 1);
  if (raw.empty()) return {};
  auto val = ral_to_string(raw.front().second);
  if (!val) return {};
  dest = std::move(*val);
  return raw.front().first;
}

RAL_Time RedisAdapterLite::getDouble(const std::string& subKey, double& dest,
                                      const RAL_GetArgs& args)
{
  auto raw = get_reverse(build_key(subKey, args.baseKey), args.maxTime, 1);
  if (raw.empty()) return {};
  auto val = ral_to_double(raw.front().second);
  if (!val) return {};
  dest = *val;
  return raw.front().first;
}

RAL_Time RedisAdapterLite::getInt(const std::string& subKey, int64_t& dest,
                                   const RAL_GetArgs& args)
{
  auto raw = get_reverse(build_key(subKey, args.baseKey), args.maxTime, 1);
  if (raw.empty()) return {};
  auto val = ral_to_int(raw.front().second);
  if (!val) return {};
  dest = *val;
  return raw.front().first;
}

RAL_Time RedisAdapterLite::getBlob(const std::string& subKey, std::vector<uint8_t>& dest,
                                    const RAL_GetArgs& args)
{
  auto raw = get_reverse(build_key(subKey, args.baseKey), args.maxTime, 1);
  if (raw.empty()) return {};
  auto val = ral_to_blob(raw.front().second);
  if (!val) return {};
  dest = std::move(*val);
  return raw.front().first;
}

RAL_Time RedisAdapterLite::getAttrs(const std::string& subKey, Attrs& dest,
                                     const RAL_GetArgs& args)
{
  auto raw = get_reverse(build_key(subKey, args.baseKey), args.maxTime, 1);
  if (raw.empty()) return {};
  dest = std::move(raw.front().second);
  return raw.front().first;
}

//===========================================================================
//  Get range
//===========================================================================

TimeStringList RedisAdapterLite::getStrings(const std::string& subKey, const RAL_GetArgs& args)
{
  auto raw = get_forward(build_key(subKey, args.baseKey), args.minTime, args.maxTime, args.count);
  TimeStringList result;
  for (auto& entry : raw)
  {
    auto val = ral_to_string(entry.second);
    if (val) result.emplace_back(entry.first, std::move(*val));
  }
  return result;
}

TimeDoubleList RedisAdapterLite::getDoubles(const std::string& subKey, const RAL_GetArgs& args)
{
  auto raw = get_forward(build_key(subKey, args.baseKey), args.minTime, args.maxTime, args.count);
  TimeDoubleList result;
  for (auto& entry : raw)
  {
    auto val = ral_to_double(entry.second);
    if (val) result.emplace_back(entry.first, *val);
  }
  return result;
}

TimeIntList RedisAdapterLite::getInts(const std::string& subKey, const RAL_GetArgs& args)
{
  auto raw = get_forward(build_key(subKey, args.baseKey), args.minTime, args.maxTime, args.count);
  TimeIntList result;
  for (auto& entry : raw)
  {
    auto val = ral_to_int(entry.second);
    if (val) result.emplace_back(entry.first, *val);
  }
  return result;
}

TimeBlobList RedisAdapterLite::getBlobs(const std::string& subKey, const RAL_GetArgs& args)
{
  auto raw = get_forward(build_key(subKey, args.baseKey), args.minTime, args.maxTime, args.count);
  TimeBlobList result;
  for (auto& entry : raw)
  {
    auto val = ral_to_blob(entry.second);
    if (val) result.emplace_back(entry.first, std::move(*val));
  }
  return result;
}

TimeAttrsList RedisAdapterLite::getAttrsRange(const std::string& subKey, const RAL_GetArgs& args)
{
  return get_forward(build_key(subKey, args.baseKey), args.minTime, args.maxTime, args.count);
}

//===========================================================================
//  Get range (reverse, before maxTime with count)
//  Results returned in chronological order (oldest first)
//===========================================================================

TimeStringList RedisAdapterLite::getStringsBefore(const std::string& subKey, const RAL_GetArgs& args)
{
  auto raw = get_reverse(build_key(subKey, args.baseKey), args.maxTime, args.count);
  TimeStringList result;
  for (auto it = raw.rbegin(); it != raw.rend(); ++it)
  {
    auto val = ral_to_string(it->second);
    if (val) result.emplace_back(it->first, std::move(*val));
  }
  return result;
}

TimeDoubleList RedisAdapterLite::getDoublesBefore(const std::string& subKey, const RAL_GetArgs& args)
{
  auto raw = get_reverse(build_key(subKey, args.baseKey), args.maxTime, args.count);
  TimeDoubleList result;
  for (auto it = raw.rbegin(); it != raw.rend(); ++it)
  {
    auto val = ral_to_double(it->second);
    if (val) result.emplace_back(it->first, *val);
  }
  return result;
}

TimeIntList RedisAdapterLite::getIntsBefore(const std::string& subKey, const RAL_GetArgs& args)
{
  auto raw = get_reverse(build_key(subKey, args.baseKey), args.maxTime, args.count);
  TimeIntList result;
  for (auto it = raw.rbegin(); it != raw.rend(); ++it)
  {
    auto val = ral_to_int(it->second);
    if (val) result.emplace_back(it->first, *val);
  }
  return result;
}

TimeBlobList RedisAdapterLite::getBlobsBefore(const std::string& subKey, const RAL_GetArgs& args)
{
  auto raw = get_reverse(build_key(subKey, args.baseKey), args.maxTime, args.count);
  TimeBlobList result;
  for (auto it = raw.rbegin(); it != raw.rend(); ++it)
  {
    auto val = ral_to_blob(it->second);
    if (val) result.emplace_back(it->first, std::move(*val));
  }
  return result;
}

TimeAttrsList RedisAdapterLite::getAttrsBefore(const std::string& subKey, const RAL_GetArgs& args)
{
  auto raw = get_reverse(build_key(subKey, args.baseKey), args.maxTime, args.count);
  // Reverse to chronological order
  std::reverse(raw.begin(), raw.end());
  return raw;
}

//===========================================================================
//  Bulk add (multiple timestamped items)
//===========================================================================

std::vector<RAL_Time> RedisAdapterLite::pipeline_add(const std::string& key,
                                                       const std::vector<std::pair<std::string, Attrs>>& entries,
                                                       uint32_t trim)
{
  // Ensure trim is at least as large as the batch to avoid
  // trimming away entries we just added
  uint32_t effective_trim = 0;
  if (trim) effective_trim = std::max(trim, static_cast<uint32_t>(entries.size()));

  auto ids = _redis.xadd_pipeline(key, entries, effective_trim);

  std::vector<RAL_Time> ret;
  ret.reserve(ids.size());
  for (auto& id : ids)
  {
    if (!id.empty()) ret.push_back(RAL_Time(id));
  }
  check_reconnect(ret.size());
  return ret;
}

std::vector<RAL_Time> RedisAdapterLite::addStrings(const std::string& subKey,
                                                     const TimeStringList& data, uint32_t trim)
{
  std::string key = build_key(subKey);
  std::vector<std::pair<std::string, Attrs>> entries;
  entries.reserve(data.size());
  for (auto& item : data)
    entries.emplace_back(item.first.id_or_now(), ral_from_string(item.second));
  return pipeline_add(key, entries, trim);
}

std::vector<RAL_Time> RedisAdapterLite::addDoubles(const std::string& subKey,
                                                     const TimeDoubleList& data, uint32_t trim)
{
  std::string key = build_key(subKey);
  std::vector<std::pair<std::string, Attrs>> entries;
  entries.reserve(data.size());
  for (auto& item : data)
    entries.emplace_back(item.first.id_or_now(), ral_from_double(item.second));
  return pipeline_add(key, entries, trim);
}

std::vector<RAL_Time> RedisAdapterLite::addInts(const std::string& subKey,
                                                  const TimeIntList& data, uint32_t trim)
{
  std::string key = build_key(subKey);
  std::vector<std::pair<std::string, Attrs>> entries;
  entries.reserve(data.size());
  for (auto& item : data)
    entries.emplace_back(item.first.id_or_now(), ral_from_int(item.second));
  return pipeline_add(key, entries, trim);
}

std::vector<RAL_Time> RedisAdapterLite::addBlobs(const std::string& subKey,
                                                   const TimeBlobList& data, uint32_t trim)
{
  std::string key = build_key(subKey);
  std::vector<std::pair<std::string, Attrs>> entries;
  entries.reserve(data.size());
  for (auto& item : data)
    entries.emplace_back(item.first.id_or_now(),
                         ral_from_blob(item.second.data(), item.second.size()));
  return pipeline_add(key, entries, trim);
}

std::vector<RAL_Time> RedisAdapterLite::addAttrsBatch(const std::string& subKey,
                                                        const TimeAttrsList& data, uint32_t trim)
{
  std::string key = build_key(subKey);
  std::vector<std::pair<std::string, Attrs>> entries;
  entries.reserve(data.size());
  for (auto& item : data)
    entries.emplace_back(item.first.id_or_now(), item.second);
  return pipeline_add(key, entries, trim);
}

//===========================================================================
//  WriteBatch
//===========================================================================

void RedisAdapterLite::WriteBatch::addString(const std::string& subKey,
                                              const std::string& data,
                                              const RAL_AddArgs& args)
{
  _entries.push_back({_adapter.build_key(subKey), args.time.id_or_now(),
                      ral_from_string(data), args.trim});
}

void RedisAdapterLite::WriteBatch::addDouble(const std::string& subKey, double data,
                                              const RAL_AddArgs& args)
{
  char buf[sizeof(double)];
  std::memcpy(buf, &data, sizeof(double));
  Attrs attrs = {{ DEFAULT_FIELD, std::string(buf, sizeof(double)) }};
  _entries.push_back({_adapter.build_key(subKey), args.time.id_or_now(),
                      std::move(attrs), args.trim});
}

void RedisAdapterLite::WriteBatch::addInt(const std::string& subKey, int64_t data,
                                           const RAL_AddArgs& args)
{
  char buf[sizeof(int64_t)];
  std::memcpy(buf, &data, sizeof(int64_t));
  Attrs attrs = {{ DEFAULT_FIELD, std::string(buf, sizeof(int64_t)) }};
  _entries.push_back({_adapter.build_key(subKey), args.time.id_or_now(),
                      std::move(attrs), args.trim});
}

void RedisAdapterLite::WriteBatch::addBlob(const std::string& subKey,
                                            const void* data, size_t size,
                                            const RAL_AddArgs& args)
{
  _entries.push_back({_adapter.build_key(subKey), args.time.id_or_now(),
                      ral_from_blob(data, size), args.trim});
}

void RedisAdapterLite::WriteBatch::addAttrs(const std::string& subKey,
                                             const Attrs& data,
                                             const RAL_AddArgs& args)
{
  _entries.push_back({_adapter.build_key(subKey), args.time.id_or_now(),
                      data, args.trim});
}

std::vector<RAL_Time> RedisAdapterLite::WriteBatch::execute()
{
  if (_entries.empty()) return {};

  auto ids = _adapter._redis.xadd_pipeline_multi(_entries);
  _entries.clear();

  std::vector<RAL_Time> ret;
  ret.reserve(ids.size());
  for (auto& id : ids)
  {
    if (!id.empty()) ret.push_back(RAL_Time(id));
    else ret.push_back(RAL_Time());
  }
  _adapter.check_reconnect(ret.empty() ? 0 : 1);
  return ret;
}

//===========================================================================
//  Connection
//===========================================================================

bool RedisAdapterLite::connected()
{
  return check_reconnect(_redis.ping());
}

//===========================================================================
//  Utility
//===========================================================================

bool RedisAdapterLite::del(const std::string& subKey)
{
  return check_reconnect(_redis.del(build_key(subKey)) >= 0);
}

bool RedisAdapterLite::rename(const std::string& srcSubKey, const std::string& dstSubKey)
{
  return check_reconnect(_redis.rename(build_key(srcSubKey), build_key(dstSubKey)));
}

bool RedisAdapterLite::copy(const std::string& srcSubKey, const std::string& dstSubKey,
                             const std::string& baseKey)
{
  std::string srcKey = build_key(srcSubKey, baseKey);
  std::string dstKey = build_key(dstSubKey);

  int64_t ret = _redis.copy(srcKey, dstKey);

  // If copy failed and dest doesn't exist, try manual copy via XRANGE + XADD
  if (ret <= 0 && _redis.exists(dstKey) == 0)
  {
    auto raw = _redis.xrange(srcKey, "-", "+");
    if (!raw.empty())
    {
      std::string id;
      for (auto& entry : raw)
      {
        id = _redis.xadd(dstKey, entry.first.id(), entry.second);
      }
      ret = id.size() > 0 ? 1 : 0;
    }
  }
  check_reconnect(ret != -1);
  return ret > 0;
}

//===========================================================================
//  Watchdog
//===========================================================================

bool RedisAdapterLite::addWatchdog(const std::string& dogname, uint32_t expiration)
{
  return _redis.hset(_watchdog_key, dogname, RAL_VERSION) && petWatchdog(dogname, expiration);
}

bool RedisAdapterLite::petWatchdog(const std::string& dogname, uint32_t expiration)
{
  return check_reconnect(_redis.hexpire(_watchdog_key, dogname, expiration) != -1);
}

std::vector<std::string> RedisAdapterLite::getWatchdogs()
{
  return _redis.hkeys(_watchdog_key);
}

//===========================================================================
//  Reconnect
//===========================================================================

int32_t RedisAdapterLite::check_reconnect(int32_t result)
{
  if (result == 0 && _connecting.exchange(true) == false)
  {
    // Join any previous reconnect thread
    if (_reconnect_thread.joinable()) _reconnect_thread.join();

    _reconnect_thread = std::thread([this]()
    {
      if (_redis.reconnect())
      {
        // Restart all readers
        std::lock_guard<std::mutex> lk(_reader_mutex);
        for (auto& [token, _] : _readers) stop_reader(token);
        for (auto& [token, _] : _readers) start_reader(token);
      }
      else
      {
        std::this_thread::sleep_for(milliseconds(100));  // throttle failures
      }
      _connecting = false;
    });
  }
  return result;
}

//===========================================================================
//  Stream Readers
//===========================================================================

uint32_t RedisAdapterLite::reader_token(const std::string& key)
{
  static std::hash<std::string> hasher;

  if (!_redis.ping()) return NO_TOKEN;

  if (_options.readers <= 1) return 0;
  return hasher(key) % _options.readers;
}

bool RedisAdapterLite::addReader(const std::string& subKey, ReaderCallback func,
                                  const std::string& baseKey)
{
  std::string key = build_key(subKey, baseKey);
  uint32_t token = reader_token(key);

  std::lock_guard<std::mutex> lk(_reader_mutex);
  auto& ptr = _readers[token];
  if (!ptr) ptr = std::make_unique<ReaderInfo>();
  ReaderInfo& info = *ptr;

  info.subs[key].push_back(func);
  info.key_ids[key] = "$";

  if (token == NO_TOKEN) return false;

  stop_reader(token);

  if (info.stop_key.empty())
  {
    info.stop_key = build_key(subKey + ":" + STOP_STUB, baseKey);
    info.key_ids[info.stop_key] = "$";
  }
  return start_reader(token);
}

bool RedisAdapterLite::removeReader(const std::string& subKey, const std::string& baseKey)
{
  std::string key = build_key(subKey, baseKey);
  uint32_t token = reader_token(key);

  std::lock_guard<std::mutex> lk(_reader_mutex);
  if (token == NO_TOKEN || _readers.count(token) == 0) return false;

  stop_reader(token);
  ReaderInfo& info = *_readers.at(token);
  info.subs.erase(key);
  info.key_ids.erase(key);

  if (info.subs.empty())
  {
    _readers.erase(token);
    return true;
  }
  return start_reader(token);
}

bool RedisAdapterLite::setDeferReaders(bool defer)
{
  std::lock_guard<std::mutex> lk(_reader_mutex);
  if (defer && !_readers_defer)
  {
    _readers_defer = true;
    for (auto& [token, _] : _readers) stop_reader(token);
  }
  else if (!defer && _readers_defer)
  {
    _readers_defer = false;
    for (auto& [token, _] : _readers) start_reader(token);
  }
  return true;
}

bool RedisAdapterLite::start_reader(uint32_t token)
{
  // Caller must hold _reader_mutex
  if (_readers_defer) return true;
  if (token == NO_TOKEN || _readers.count(token) == 0) return false;

  ReaderInfo& info = *_readers.at(token);
  if (info.thread.joinable()) return false;

  // Create a dedicated context for this reader thread
  info.ctx = _redis.create_context();
  if (!info.ctx) return false;

  // Use promise/future for thread start confirmation
  std::promise<void> started;
  auto started_future = started.get_future();

  // Capture raw pointer — stable across map rehashes since ReaderInfo
  // is heap-allocated via unique_ptr. stop_reader() joins before erase.
  ReaderInfo* info_ptr = &info;
  info.thread = std::thread([this, info_ptr, p = std::move(started)]() mutable
  {
    ReaderInfo& info = *info_ptr;
    bool check_for_dollars = true;
    info.run = true;
    p.set_value();  // signal thread started

    while (info.run)
    {
      // Build keys_ids for XREAD
      std::vector<std::pair<std::string, std::string>> keys_ids;
      for (auto& [k, id] : info.key_ids)
        keys_ids.emplace_back(k, id);

      if (keys_ids.empty()) break;

      // Build XREAD BLOCK command using the reader's own context
      std::string cmd_str = "XREAD";
      std::string block_kw = "BLOCK";
      std::string timeout_str = std::to_string(_options.timeout);
      std::string streams_kw = "STREAMS";

      std::vector<const char*> argv;
      std::vector<size_t> argvlen;
      argv.push_back(cmd_str.c_str());      argvlen.push_back(cmd_str.size());
      argv.push_back(block_kw.c_str());     argvlen.push_back(block_kw.size());
      argv.push_back(timeout_str.c_str());  argvlen.push_back(timeout_str.size());
      argv.push_back(streams_kw.c_str());   argvlen.push_back(streams_kw.size());

      for (auto& [k, _] : keys_ids)
        { argv.push_back(k.c_str()); argvlen.push_back(k.size()); }
      for (auto& [_, id] : keys_ids)
        { argv.push_back(id.c_str()); argvlen.push_back(id.size()); }

      redisReply* raw = static_cast<redisReply*>(
          redisCommandArgv(info.ctx, static_cast<int>(argv.size()),
                           argv.data(), argvlen.data()));

      if (!raw)
      {
        syslog(LOG_ERR, "RedisAdapterLite: reader lost connection");
        info.run = false;
        break;
      }

      if (raw->type == REDIS_REPLY_ERROR)
      {
        syslog(LOG_ERR, "RedisAdapterLite: reader error: %s", raw->str);
        freeReplyObject(raw);
        info.run = false;
        break;
      }

      if (raw->type == REDIS_REPLY_NIL)
      {
        freeReplyObject(raw);
        continue;  // timeout, retry
      }

      auto streams = parse_xread_reply(raw);
      freeReplyObject(raw);

      for (auto& [stream_key, entries] : streams)
      {
        if (!entries.empty())
        {
          info.key_ids[stream_key] = entries.back().first.id();

          // On first real result, replace all "$" IDs with this ID
          if (check_for_dollars)
          {
            const std::string& newid = entries.back().first.id();
            for (auto& [_, kid] : info.key_ids)
            {
              if (!kid.empty() && kid[0] == '$') kid = newid;
            }
            check_for_dollars = false;
          }
        }

        if (info.subs.count(stream_key))
        {
          auto split = split_key(stream_key);
          // Copy entries for each subscriber (fixes old std::move bug)
          for (auto& func : info.subs.at(stream_key))
          {
            TimeAttrsList entries_copy = entries;
            if (split.first.size())
            {
              _worker_pool.job(stream_key,
                [func, s = split, e = std::move(entries_copy)]()
                  { func(s.first, s.second, e); }
              );
            }
            else
            {
              _worker_pool.job(stream_key,
                [func, k = stream_key, e = std::move(entries_copy)]()
                  { func(k, k, e); }
              );
            }
          }
        }
      }
    }

    // Cleanup reader context
    if (info.ctx) { redisFree(info.ctx); info.ctx = nullptr; }
  });

  // Wait for thread to start (with timeout)
  if (started_future.wait_for(milliseconds(50)) == std::future_status::timeout)
  {
    syslog(LOG_WARNING, "RedisAdapterLite: start_reader timeout");
    return false;
  }
  return true;
}

bool RedisAdapterLite::stop_reader(uint32_t token)
{
  // Caller must hold _reader_mutex
  if (token == NO_TOKEN || _readers.count(token) == 0) return false;

  ReaderInfo& info = *_readers.at(token);
  if (!info.thread.joinable()) return false;

  info.run = false;

  // Send a stop signal via XADD to unblock XREAD
  Attrs attrs = ral_from_string("");
  _redis.xadd_trim(info.stop_key, "*", attrs, 1);

  info.thread.join();
  return true;
}

//===========================================================================
//  Pub/Sub
//===========================================================================

bool RedisAdapterLite::publish(const std::string& subKey, const std::string& message,
                                const std::string& baseKey)
{
  return check_reconnect(_redis.publish(build_key(subKey, baseKey), message) != -1);
}

bool RedisAdapterLite::subscribe(const std::string& subKey, SubCallback func,
                                  const std::string& baseKey)
{
  std::lock_guard<std::mutex> lk(_sub_mutex);
  std::string channel = build_key(subKey, baseKey);
  stop_subscriber();
  _sub.channels[channel] = func;
  return restart_subscriber();
}

bool RedisAdapterLite::unsubscribe(const std::string& subKey, const std::string& baseKey)
{
  std::lock_guard<std::mutex> lk(_sub_mutex);
  std::string channel = build_key(subKey, baseKey);
  stop_subscriber();
  _sub.channels.erase(channel);

  if (_sub.channels.empty()) return true;
  return restart_subscriber();
}

bool RedisAdapterLite::restart_subscriber()
{
  // Caller must hold _sub_mutex
  stop_subscriber();

  if (_sub.channels.empty()) return true;

  _sub.ctx = _redis.create_context();
  if (!_sub.ctx) return false;

  // Snapshot channels for the thread — the thread uses this copy
  // instead of accessing _sub.channels, eliminating the need for
  // _sub_mutex in the thread and preventing deadlock with join().
  auto channels_snapshot = _sub.channels;

  _sub.run = true;
  _sub.thread = std::thread([this, channels = std::move(channels_snapshot)]()
  {
    // Subscribe to all channels — use argv-based command to safely
    // handle channel names containing special characters
    for (auto& [channel, _] : channels)
    {
      const char* argv[2]    = { "SUBSCRIBE", channel.c_str() };
      size_t      argvlen[2] = { 9,           channel.size() };
      redisReply* r = static_cast<redisReply*>(
          redisCommandArgv(_sub.ctx, 2, argv, argvlen));
      if (r) freeReplyObject(r);
    }

    // Read messages
    while (_sub.run)
    {
      redisReply* reply = nullptr;
      if (redisGetReply(_sub.ctx, reinterpret_cast<void**>(&reply)) != REDIS_OK)
      {
        if (_sub.run)
          syslog(LOG_ERR, "RedisAdapterLite: subscriber connection lost");
        break;
      }

      if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3
          && reply->element[0] && reply->element[1] && reply->element[2]
          && reply->element[0]->str && reply->element[1]->str && reply->element[2]->str)
      {
        std::string type(reply->element[0]->str, reply->element[0]->len);
        if (type == "message")
        {
          std::string channel(reply->element[1]->str, reply->element[1]->len);
          std::string message(reply->element[2]->str, reply->element[2]->len);

          // Use the thread-local snapshot — no mutex needed
          auto it = channels.find(channel);
          if (it != channels.end())
          {
            auto split = split_key(channel);
            auto func = it->second;
            _worker_pool.job(channel,
              [func, s = std::move(split), m = std::move(message)]()
                { func(s.first, s.second, m); }
            );
          }
        }
      }
      if (reply) freeReplyObject(reply);
    }

    // Context cleanup is handled by stop_subscriber() after join
  });

  return true;
}

void RedisAdapterLite::stop_subscriber()
{
  // Caller must hold _sub_mutex
  if (!_sub.thread.joinable()) return;
  _sub.run = false;

  // Send PUBLISH via a separate context to unblock redisGetReply
  if (_sub.ctx)
  {
    redisContext* tmp = _redis.create_context();
    if (tmp)
    {
      for (auto& [channel, _] : _sub.channels)
      {
        const char* argv[3]    = { "PUBLISH", channel.c_str(), "" };
        size_t      argvlen[3] = { 7,         channel.size(),  0 };
        redisReply* r = static_cast<redisReply*>(
            redisCommandArgv(tmp, 3, argv, argvlen));
        if (r) freeReplyObject(r);
      }
      redisFree(tmp);
    }
  }

  _sub.thread.join();

  // Clean up context after thread has exited — no race possible
  if (_sub.ctx) { redisFree(_sub.ctx); _sub.ctx = nullptr; }
}
