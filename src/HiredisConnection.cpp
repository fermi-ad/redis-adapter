//
//  HiredisConnection.cpp
//
//  RAII hiredis connection implementation
//

#include "HiredisConnection.hpp"
#include <syslog.h>
#include <cstdarg>
#include <atomic>

//--- Helper: create and configure a redisContext ---
static redisContext* make_context(const RAL_Options& opts)
{
  struct timeval tv;
  tv.tv_sec  = opts.timeout / 1000;
  tv.tv_usec = (opts.timeout % 1000) * 1000;

  redisContext* ctx = nullptr;

  if (opts.path.size())
    ctx = redisConnectUnixWithTimeout(opts.path.c_str(), tv);
  else
    ctx = redisConnectWithTimeout(opts.host.c_str(), opts.port, tv);

  if (!ctx)
  {
    syslog(LOG_ERR, "HiredisConnection: allocation failed");
    return nullptr;
  }

  if (ctx->err)
  {
    syslog(LOG_ERR, "HiredisConnection: %s", ctx->errstr);
    redisFree(ctx);
    return nullptr;
  }

  // Set command timeout
  redisSetTimeout(ctx, tv);

  // Auth if password provided — use argv-based command to safely handle
  // passwords containing special characters (spaces, quotes, etc.)
  if (opts.password.size())
  {
    const char* argv[3]    = { "AUTH", opts.user.c_str(), opts.password.c_str() };
    size_t      argvlen[3] = { 4,     opts.user.size(),  opts.password.size() };

    redisReply* r = static_cast<redisReply*>(
        redisCommandArgv(ctx, 3, argv, argvlen));
    bool ok = r && r->type != REDIS_REPLY_ERROR;
    if (!ok) syslog(LOG_ERR, "HiredisConnection: AUTH failed: %s", r ? r->str : "null reply");
    if (r) freeReplyObject(r);
    if (!ok) { redisFree(ctx); return nullptr; }
  }

  return ctx;
}

HiredisConnection::HiredisConnection(const RAL_Options& opts)
  : _opts(opts)
{
  _ctx = make_context(opts);
  if (!_ctx) syslog(LOG_ERR, "HiredisConnection: failed to connect in constructor");
}

HiredisConnection::~HiredisConnection()
{
  std::lock_guard<std::mutex> lk(_mutex);
  if (_ctx) { redisFree(_ctx); _ctx = nullptr; }
}

bool HiredisConnection::connect(const RAL_Options& opts)
{
  std::lock_guard<std::mutex> lk(_mutex);
  if (_ctx) { redisFree(_ctx); _ctx = nullptr; }
  _opts = opts;
  _ctx = make_context(opts);
  return _ctx != nullptr;
}

bool HiredisConnection::reconnect()
{
  std::lock_guard<std::mutex> lk(_mutex);
  if (_ctx) { redisFree(_ctx); _ctx = nullptr; }
  _ctx = make_context(_opts);
  return _ctx != nullptr;
}

bool HiredisConnection::ping()
{
  std::lock_guard<std::mutex> lk(_mutex);
  if (!_ctx) return false;

  redisReply* r = static_cast<redisReply*>(redisCommand(_ctx, "PING"));
  bool ok = r && r->type == REDIS_REPLY_STATUS && std::string(r->str) == "PONG";
  if (r) freeReplyObject(r);
  return ok;
}

bool HiredisConnection::is_connected()
{
  std::lock_guard<std::mutex> lk(_mutex);
  return _ctx != nullptr && _ctx->err == 0;
}

redisContext* HiredisConnection::create_context()
{
  return make_context(_opts);
}

//--- Private: send a command ---
ReplyPtr HiredisConnection::cmd(const char* fmt, ...)
{
  // Note: caller must hold _mutex
  if (!_ctx) return nullptr;

  va_list ap;
  va_start(ap, fmt);
  redisReply* r = static_cast<redisReply*>(redisvCommand(_ctx, fmt, ap));
  va_end(ap);

  if (!r)
  {
    syslog(LOG_ERR, "HiredisConnection: null reply, connection lost");
    return nullptr;
  }
  if (r->type == REDIS_REPLY_ERROR)
  {
    syslog(LOG_ERR, "HiredisConnection: %s", r->str);
  }
  return ReplyPtr(r);
}

//--- Stream commands ---

std::string HiredisConnection::xadd(const std::string& key, const std::string& id,
                                     const Attrs& fields)
{
  std::lock_guard<std::mutex> lk(_mutex);
  if (!_ctx) return {};

  // Build argv: XADD key id field value field value ...
  std::vector<const char*> argv;
  std::vector<size_t> argvlen;
  argv.reserve(3 + fields.size() * 2);
  argvlen.reserve(3 + fields.size() * 2);

  std::string cmd_str = "XADD";
  argv.push_back(cmd_str.c_str());  argvlen.push_back(cmd_str.size());
  argv.push_back(key.c_str());      argvlen.push_back(key.size());
  argv.push_back(id.c_str());       argvlen.push_back(id.size());

  for (auto& [f, v] : fields)
  {
    argv.push_back(f.c_str());  argvlen.push_back(f.size());
    argv.push_back(v.c_str());  argvlen.push_back(v.size());
  }

  ReplyPtr r(static_cast<redisReply*>(
      redisCommandArgv(_ctx, static_cast<int>(argv.size()), argv.data(), argvlen.data())));

  if (r && r->type == REDIS_REPLY_STRING)
    return std::string(r->str, r->len);

  if (r && r->type == REDIS_REPLY_ERROR)
    syslog(LOG_ERR, "HiredisConnection::xadd %s", r->str);

  return {};
}

std::string HiredisConnection::xadd_trim(const std::string& key, const std::string& id,
                                          const Attrs& fields, uint32_t maxlen)
{
  std::lock_guard<std::mutex> lk(_mutex);
  if (!_ctx) return {};

  // XADD key MAXLEN ~ maxlen id field value ...
  std::string cmd_str = "XADD";
  std::string maxlen_kw = "MAXLEN";
  std::string approx = "~";
  std::string maxlen_str = std::to_string(maxlen);

  std::vector<const char*> argv;
  std::vector<size_t> argvlen;
  argv.reserve(6 + fields.size() * 2);
  argvlen.reserve(6 + fields.size() * 2);

  argv.push_back(cmd_str.c_str());     argvlen.push_back(cmd_str.size());
  argv.push_back(key.c_str());         argvlen.push_back(key.size());
  argv.push_back(maxlen_kw.c_str());   argvlen.push_back(maxlen_kw.size());
  argv.push_back(approx.c_str());      argvlen.push_back(approx.size());
  argv.push_back(maxlen_str.c_str());  argvlen.push_back(maxlen_str.size());
  argv.push_back(id.c_str());          argvlen.push_back(id.size());

  for (auto& [f, v] : fields)
  {
    argv.push_back(f.c_str());  argvlen.push_back(f.size());
    argv.push_back(v.c_str());  argvlen.push_back(v.size());
  }

  ReplyPtr r(static_cast<redisReply*>(
      redisCommandArgv(_ctx, static_cast<int>(argv.size()), argv.data(), argvlen.data())));

  if (r && r->type == REDIS_REPLY_STRING)
    return std::string(r->str, r->len);

  if (r && r->type == REDIS_REPLY_ERROR)
    syslog(LOG_ERR, "HiredisConnection::xadd_trim %s", r->str);

  return {};
}

std::string HiredisConnection::xadd_single(const std::string& key, const std::string& id,
                                             const char* field, size_t field_len,
                                             const char* value, size_t value_len)
{
  std::lock_guard<std::mutex> lk(_mutex);
  if (!_ctx) return {};

  const char* argv[5]    = { "XADD", key.c_str(), id.c_str(), field, value };
  size_t      argvlen[5] = { 4,      key.size(),  id.size(),  field_len, value_len };

  ReplyPtr r(static_cast<redisReply*>(
      redisCommandArgv(_ctx, 5, argv, argvlen)));

  if (r && r->type == REDIS_REPLY_STRING)
    return std::string(r->str, r->len);

  if (r && r->type == REDIS_REPLY_ERROR)
    syslog(LOG_ERR, "HiredisConnection::xadd_single %s", r->str);

  return {};
}

std::string HiredisConnection::xadd_trim_single(const std::string& key, const std::string& id,
                                                  const char* field, size_t field_len,
                                                  const char* value, size_t value_len,
                                                  uint32_t maxlen)
{
  std::lock_guard<std::mutex> lk(_mutex);
  if (!_ctx) return {};

  std::string maxlen_str = std::to_string(maxlen);

  const char* argv[8]    = { "XADD", key.c_str(), "MAXLEN", "~",
                              maxlen_str.c_str(), id.c_str(), field, value };
  size_t      argvlen[8] = { 4,      key.size(),  6,         1,
                              maxlen_str.size(),  id.size(),  field_len, value_len };

  ReplyPtr r(static_cast<redisReply*>(
      redisCommandArgv(_ctx, 8, argv, argvlen)));

  if (r && r->type == REDIS_REPLY_STRING)
    return std::string(r->str, r->len);

  if (r && r->type == REDIS_REPLY_ERROR)
    syslog(LOG_ERR, "HiredisConnection::xadd_trim_single %s", r->str);

  return {};
}

int64_t HiredisConnection::xtrim(const std::string& key, uint32_t maxlen)
{
  std::lock_guard<std::mutex> lk(_mutex);
  auto r = cmd("XTRIM %s MAXLEN ~ %u", key.c_str(), maxlen);
  if (r && r->type == REDIS_REPLY_INTEGER) return r->integer;
  return -1;
}

TimeAttrsList HiredisConnection::xrange(const std::string& key, const std::string& min,
                                         const std::string& max)
{
  std::lock_guard<std::mutex> lk(_mutex);
  auto r = cmd("XRANGE %s %s %s", key.c_str(), min.c_str(), max.c_str());
  return r ? parse_stream_reply(r.get()) : TimeAttrsList{};
}

TimeAttrsList HiredisConnection::xrange(const std::string& key, const std::string& min,
                                         const std::string& max, uint32_t count)
{
  std::lock_guard<std::mutex> lk(_mutex);
  auto r = cmd("XRANGE %s %s %s COUNT %u", key.c_str(), min.c_str(), max.c_str(), count);
  return r ? parse_stream_reply(r.get()) : TimeAttrsList{};
}

TimeAttrsList HiredisConnection::xrevrange(const std::string& key, const std::string& max,
                                            const std::string& min)
{
  std::lock_guard<std::mutex> lk(_mutex);
  auto r = cmd("XREVRANGE %s %s %s", key.c_str(), max.c_str(), min.c_str());
  return r ? parse_stream_reply(r.get()) : TimeAttrsList{};
}

TimeAttrsList HiredisConnection::xrevrange(const std::string& key, const std::string& max,
                                            const std::string& min, uint32_t count)
{
  std::lock_guard<std::mutex> lk(_mutex);
  auto r = cmd("XREVRANGE %s %s %s COUNT %u", key.c_str(), max.c_str(), min.c_str(), count);
  return r ? parse_stream_reply(r.get()) : TimeAttrsList{};
}

std::unordered_map<std::string, TimeAttrsList>
HiredisConnection::xread_block(const std::vector<std::pair<std::string, std::string>>& keys_ids,
                                uint32_t timeout_ms)
{
  // NOT using _mutex — this method is for reader threads with their own context.
  // Caller must provide their own redisContext* via create_context() and call
  // redisCommand directly. This method uses _ctx for non-reader usage.
  std::lock_guard<std::mutex> lk(_mutex);
  if (!_ctx || keys_ids.empty()) return {};

  // Build: XREAD BLOCK timeout STREAMS key1 key2 ... id1 id2 ...
  std::string cmd_str = "XREAD";
  std::string block_kw = "BLOCK";
  std::string timeout_str = std::to_string(timeout_ms);
  std::string streams_kw = "STREAMS";

  std::vector<const char*> argv;
  std::vector<size_t> argvlen;

  argv.push_back(cmd_str.c_str());      argvlen.push_back(cmd_str.size());
  argv.push_back(block_kw.c_str());     argvlen.push_back(block_kw.size());
  argv.push_back(timeout_str.c_str());  argvlen.push_back(timeout_str.size());
  argv.push_back(streams_kw.c_str());   argvlen.push_back(streams_kw.size());

  for (auto& [key, _] : keys_ids)
  {
    argv.push_back(key.c_str());  argvlen.push_back(key.size());
  }
  for (auto& [_, id] : keys_ids)
  {
    argv.push_back(id.c_str());  argvlen.push_back(id.size());
  }

  ReplyPtr r(static_cast<redisReply*>(
      redisCommandArgv(_ctx, static_cast<int>(argv.size()), argv.data(), argvlen.data())));

  if (!r || r->type == REDIS_REPLY_NIL) return {};   // timeout

  if (r->type == REDIS_REPLY_ERROR)
  {
    syslog(LOG_ERR, "HiredisConnection::xread_block %s", r->str);
    return {};
  }

  return parse_xread_reply(r.get());
}

//--- Key commands ---

int64_t HiredisConnection::del(const std::string& key)
{
  std::lock_guard<std::mutex> lk(_mutex);
  auto r = cmd("DEL %s", key.c_str());
  if (r && r->type == REDIS_REPLY_INTEGER) return r->integer;
  return -1;
}

bool HiredisConnection::rename(const std::string& src, const std::string& dst)
{
  std::lock_guard<std::mutex> lk(_mutex);
  auto r = cmd("RENAME %s %s", src.c_str(), dst.c_str());
  return r && r->type == REDIS_REPLY_STATUS;
}

int64_t HiredisConnection::copy(const std::string& src, const std::string& dst)
{
  std::lock_guard<std::mutex> lk(_mutex);
  auto r = cmd("COPY %s %s", src.c_str(), dst.c_str());
  if (r && r->type == REDIS_REPLY_INTEGER) return r->integer;
  if (r && r->type == REDIS_REPLY_ERROR) return -1;
  return -1;
}

int64_t HiredisConnection::exists(const std::string& key)
{
  std::lock_guard<std::mutex> lk(_mutex);
  auto r = cmd("EXISTS %s", key.c_str());
  if (r && r->type == REDIS_REPLY_INTEGER) return r->integer;
  return -1;
}

//--- Hash commands ---

bool HiredisConnection::hset(const std::string& key, const std::string& field,
                              const std::string& value)
{
  std::lock_guard<std::mutex> lk(_mutex);
  auto r = cmd("HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
  return r && r->type == REDIS_REPLY_INTEGER;
}

int32_t HiredisConnection::hexpire(const std::string& key, const std::string& field,
                                    uint32_t seconds)
{
  std::lock_guard<std::mutex> lk(_mutex);
  if (!_ctx) return -1;

  std::string sec_str = std::to_string(seconds);

  std::string cmd_s = "HEXPIRE";
  std::string fields_kw = "FIELDS";
  std::string one = "1";

  std::vector<const char*> argv = {
    cmd_s.c_str(), key.c_str(), sec_str.c_str(),
    fields_kw.c_str(), one.c_str(), field.c_str()
  };
  std::vector<size_t> argvlen = {
    cmd_s.size(), key.size(), sec_str.size(),
    fields_kw.size(), one.size(), field.size()
  };

  ReplyPtr r(static_cast<redisReply*>(
      redisCommandArgv(_ctx, static_cast<int>(argv.size()), argv.data(), argvlen.data())));

  if (!r) return -1;

  if (r->type == REDIS_REPLY_ERROR)
  {
    std::string err(r->str, r->len);
    if (err.find("unknown command") != std::string::npos)
    {
      static std::atomic<bool> squelch{false};
      if (!squelch.load(std::memory_order_relaxed))
      {
        syslog(LOG_NOTICE, "HiredisConnection: HEXPIRE requires redis-server 7.4+");
        squelch.store(true, std::memory_order_relaxed);
      }
      return -3;
    }
    syslog(LOG_ERR, "HiredisConnection::hexpire %s", r->str);
    return -1;
  }

  if (r->type == REDIS_REPLY_ARRAY && r->elements > 0 &&
      r->element[0]->type == REDIS_REPLY_INTEGER)
    return static_cast<int32_t>(r->element[0]->integer);

  return -1;
}

std::vector<std::string> HiredisConnection::hkeys(const std::string& key)
{
  std::lock_guard<std::mutex> lk(_mutex);
  std::vector<std::string> result;
  auto r = cmd("HKEYS %s", key.c_str());
  if (r && r->type == REDIS_REPLY_ARRAY)
  {
    for (size_t i = 0; i < r->elements; ++i)
    {
      if (r->element[i]->type == REDIS_REPLY_STRING)
        result.emplace_back(r->element[i]->str, r->element[i]->len);
    }
  }
  return result;
}

//--- Pub/Sub ---

int64_t HiredisConnection::publish(const std::string& channel, const std::string& message)
{
  std::lock_guard<std::mutex> lk(_mutex);
  if (!_ctx) return -1;

  // Use argv-based command to handle binary-safe message
  std::string cmd_s = "PUBLISH";
  std::vector<const char*> argv = { cmd_s.c_str(), channel.c_str(), message.c_str() };
  std::vector<size_t> argvlen = { cmd_s.size(), channel.size(), message.size() };

  ReplyPtr r(static_cast<redisReply*>(
      redisCommandArgv(_ctx, static_cast<int>(argv.size()), argv.data(), argvlen.data())));

  if (r && r->type == REDIS_REPLY_INTEGER) return r->integer;
  return -1;
}
