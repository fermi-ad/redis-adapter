#pragma once

#include "sw/redis++/redis++.h"
#include <syslog.h>
#include <mutex>
#include <algorithm>

namespace swr = sw::redis;
namespace chr = std::chrono;

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  class RedisConnection
//
//  Provides a common interface to a standalone Redis server
//  If an exception is thrown in a method, the exception is logged and failure is returned
//  If a method is called while not connected, failure is returned but not logged
//
class RedisConnection
{
public:
  enum class ReadStatus { Accepted, TimedOut, Rejected, Unavailable };
  enum class CommandStatus { Accepted, Rejected, Unavailable };
  enum class StreamKind { Unknown, Missing, Stream, Invalid };
  struct StreamBounds {
    CommandStatus status = CommandStatus::Unavailable;
    StreamKind kind = StreamKind::Unknown;
    std::string firstId = "0-0", lastGeneratedId = "0-0", error;
  };
  struct ReadProbe {
    ReadStatus status = ReadStatus::Unavailable;
    bool wrongType = false;
    std::string error;
  };
  struct WriteResult {
    CommandStatus status = CommandStatus::Unavailable;
    std::string id;
    std::string error;
    bool refreshConnection = false;
  };
  struct TrimResult {
    CommandStatus status = CommandStatus::Unavailable;
    int64_t count = -1;
    std::string error;
    bool refreshConnection = false;
  };
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  struct RedisConnection::Options
  //
  //    path     : path to unix domain socket file
  //    host     : IP address of server "w.x.y.z"
  //    user     : username for connection
  //    password : password for connection
  //    timeout  : connection and blocking read timeout
  //    port     : port server is listening on
  //    size     : connection pool size
  //
  struct Options
  {
    std::string path;
    std::string host = "127.0.0.1";
    std::string user = "default";
    std::string password;
    uint32_t timeout = 500;   //  milliseconds
    uint16_t port = 6379;
    uint16_t size = 5;
    uint32_t connectTimeout = 500;  // milliseconds; zero explicitly disables it
  };

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  RedisConnection : store connection options and attempt to connect
  //
  //    options : see RedisConnection::Options above
  //
  RedisConnection(const Options& opts, uint16_t readerPoolSize = 1)
    : _reader_pool_size(std::max<uint16_t>(1, readerPoolSize))
  {
    if ( ! connect(opts)) syslog(LOG_ERR, "RedisConnection failed to connect in constructor");
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  RedisConnection : deleted to prevent copy construction
  //
  RedisConnection(const RedisConnection& conn) = delete;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  operator= : deleted to prevent copy by assignment
  //
  RedisConnection& operator=(const RedisConnection&) = delete;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  RedisConnection : move construction not allowed
  //
  RedisConnection(RedisConnection&&) = delete;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  connect : attempt to make a standalone server connection
  //
  //    options : see RedisConnection::Options above
  //    return : true if live server connected
  //             false if not connected
  //
  bool connect(const Options& opts)
  {
    swr::ConnectionOptions co;
    swr::ConnectionPoolOptions cpo;

    bool is_unix_socket = opts.path.size();   // Check if the host is a Unix socket path

    if (is_unix_socket)
    {
      co.type = swr::ConnectionType::UNIX;  // Set the connection type to UNIX socket
      co.path = opts.path;                  // Set the Unix socket path
    }
    else
    {
      co.host = opts.host;
      co.port = opts.port;
    }
    co.user = opts.user;
    co.password = opts.password;
    co.socket_timeout = chr::milliseconds(opts.timeout);
    co.connect_timeout = chr::milliseconds(opts.connectTimeout);

    cpo.size = opts.size;

    // Build both clients before taking the lock. A failed replacement leaves
    // the established clients intact, and snapshots keep clients alive for
    // in-flight calls after a successful replacement.
    std::shared_ptr<swr::Redis> client;
    std::shared_ptr<swr::Redis> reader;

    try
    {
      client = std::make_shared<swr::Redis>(co, cpo);
      client->ping();
    }
    catch (...) { client.reset(); }

    if (client) {
      if (!acceptStandalone(client)) return false;
      auto readerOptions = co;
      // A finite XREAD cycle needs socket-deadline slack so an idle NIL reply
      // arrives before the client times out. Reader cancellation remains bounded
      // even when command callers deliberately select timeout == 0.
      readerOptions.socket_timeout = chr::milliseconds(std::max<uint64_t>(1000, opts.timeout) + 250);
      auto readerPool = cpo;
      readerPool.size = _reader_pool_size;
      try {
        reader = std::make_shared<swr::Redis>(readerOptions, readerPool);
      } catch (const swr::Error& error) {
        syslog(LOG_WARNING, "cannot prepare blocking reader connections: %s", error.what());
        return false;
      }
      std::lock_guard<std::mutex> lk(_mtx);
      _client = std::move(client);
      _reader = std::move(reader);
      return true;
    }

    // No server connected; log the failure and retain established clients.
    if (is_unix_socket)
      syslog(LOG_ERR, "RedisConnection can't connnect to %s", co.path.c_str());
    else
      syslog(LOG_ERR, "RedisConnection can't connnect to %s:%i", co.host.c_str(), co.port);

    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  ping : test whether or not a live server is connected
  //
  //    return : true if live server connected
  //             false if not connected
  //
  bool ping(const std::string& key = "ping")
  {
    (void)key;  // Retained for source compatibility.
    auto client = snapshot();
    try
    {
      if (client) return client->ping().compare("PONG") == 0;
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  del : delete the specified key
  //
  //    key    : key (any type) to delete
  //    return : 1 if key deleted
  //             0 if key not found
  //            -1 if not connected
  //
  int32_t del(const std::string& key)
  {
    auto client = snapshot();
    try
    {
      if (client) return client->del(key);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xrange : read a forward-id-ordered (newest last) list of elements from a stream
  //
  //    key    : the stream to read
  //    beg    : the lowest id to read (subject to cnt)
  //    end    : the highest id to read
  //    cnt    : the max number of elements to read (regardless of beg)
  //    out    : the elements read, typically ItemStream
  //    return : true if connected
  //             false if not connected
  //
  template<typename Output>
  bool xrange(const std::string& key, const std::string& beg,
              const std::string& end, uint32_t cnt, Output out)
  {
    auto client = snapshot();
    try
    {
      if (client) { client->xrange(key, beg, end, cnt, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xrange : read a forward-id-ordered (newest last) list of elements from a stream
  //
  //    key    : the stream to read
  //    beg    : the lowest id to read
  //    end    : the highest id to read
  //    out    : the elements read, typically ItemStream
  //    return : true if connected
  //             false if not connected
  //
  template<typename Output>
  bool xrange(const std::string& key, const std::string& beg,
              const std::string& end, Output out)
  {
    auto client = snapshot();
    try
    {
      if (client) { client->xrange(key, beg, end, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xrevrange : read a reverse-id-ordered (newest first) list of elements from a stream
  //
  //    key    : the stream to read
  //    end    : the highest id to read
  //    beg    : the lowest id to read (subject to cnt)
  //    cnt    : the max number of elements to read (regardless of beg)
  //    out    : the elements read, typically ItemStream
  //    return : true if connected
  //             false if not connected
  //
  template<typename Output>
  bool xrevrange(const std::string& key, const std::string& end,
                 const std::string& beg, uint32_t cnt, Output out,
                 ReadStatus* status = nullptr, bool* wrongType = nullptr)
  {
    if (status) *status = ReadStatus::Unavailable;
    if (wrongType) *wrongType = false;
    auto client = snapshot();
    try {
      if (client) client->xrevrange(key, end, beg, cnt, out);
      else return false;
      if (status) *status = ReadStatus::Accepted;
      return true;
    } catch (const swr::ReplyError& error) {
      if (status) *status = ReadStatus::Rejected;
      if (wrongType) *wrongType = std::string(error.what()).rfind("WRONGTYPE", 0) == 0;
    } catch (const swr::Error&) {}
    return false;
  }

  // Redis 7.4 supports '+' as an XREAD tail ID. This fallback uses only XREAD
  // permission when a consumer is deliberately denied XREVRANGE.
  template<typename Output>
  bool xreadTail(const std::string& key, Output out, ReadStatus* status = nullptr,
                 bool* wrongType = nullptr) {
    if (status) *status = ReadStatus::Unavailable;
    if (wrongType) *wrongType = false;
    auto client = snapshot();
    const std::vector<std::pair<std::string, std::string>> keys{{key, "+"}};
    try {
      if (client) client->xread(keys.begin(), keys.end(), 1, out);
      else return false;
      if (status) *status = ReadStatus::Accepted;
      return true;
    } catch (const swr::ReplyError& error) {
      if (status) *status = ReadStatus::Rejected;
      if (wrongType) *wrongType = std::string(error.what()).rfind("WRONGTYPE", 0) == 0;
    } catch (const swr::Error&) {}
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xrevrange : read a reverse-id-ordered (newest first) list of elements from a stream
  //
  //    key    : the stream to read
  //    end    : the highest id to read
  //    beg    : the lowest id to read
  //    out    : the elements read, typically ItemStream
  //    return : true if connected
  //             false if not connected
  //
  template<typename Output>
  bool xrevrange(const std::string& key, const std::string& end,
                 const std::string& beg, Output out)
  {
    auto client = snapshot();
    try
    {
      if (client) { client->xrevrange(key, end, beg, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xreadMultiBlock : read from multiple streams, block until new data on one or more streams
  //
  //    fst    : the first element of map<string, string> of: stream key -> most recent element id read
  //    lst    : the last element of map<string, string> of: stream key -> most recent element id read
  //    tmo    : logical read interval; physical cycles are at most one second
  //    out    : the elements read as Streams per https://github.com/sewenew/redis-plus-plus#examples-4
  //    return : true if connected
  //             false if not connected
  //
  template<typename Input, typename Output>
  bool xreadMultiBlock(Input fst, Input lst, uint32_t tmo, Output out,
                       ReadStatus* status = nullptr, uint32_t count = 64,
                       bool* socketTimedOut = nullptr, bool blocking = true)
  {
    if (status) *status = ReadStatus::Unavailable;
    if (socketTimedOut) *socketTimedOut = false;
    auto client = snapshot(true);
    const auto block = chr::milliseconds(tmo == 0 ? 1000 : std::min<uint32_t>(tmo, 1000));
    using Fields = std::unordered_map<std::string, std::string>;
    using Entries = std::vector<std::pair<std::string, Fields>>;
    std::unordered_map<std::string, Entries> result;
    try {
      const auto destination = std::inserter(result, result.end());
      if (client) {
        if (blocking) client->xread(fst, lst, block, std::max<uint32_t>(1, count), destination);
        else client->xread(fst, lst, std::max<uint32_t>(1, count), destination);
      }
      else return false;
      if (status) *status = result.empty() ? ReadStatus::TimedOut : ReadStatus::Accepted;
      for (auto& item : result) *out++ = std::move(item);
      return true;
    } catch (const swr::ReplyError& error) {
      const std::string message = error.what();
      if (status) *status = message.rfind("WRONGPASS", 0) == 0 || message.rfind("NOAUTH", 0) == 0
          ? ReadStatus::Unavailable : ReadStatus::Rejected;
    } catch (const swr::TimeoutError&) {
      if (socketTimedOut) *socketTimedOut = true;
    } catch (const swr::Error&) {}
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xadd : add an element to the specified stream
  //
  //    key    : the stream to add an element to
  //    id     : the id for the element, must exceed current highest id, "*" means server will generate
  //    fst    : the first element of map<string, string> of: field -> value
  //    fst    : the last element of map<string, string> of: field -> value
  //    return : the id of the new element if successful
  //             empty string if unsuccsessful or not connected
  //
  // A nonblocking '$' XREAD returns no payload and tests exactly the permission
  // and type contract that a shared blocking read needs.
  ReadProbe probeReadable(const std::vector<std::string>& keys) {
    if (keys.empty()) return {ReadStatus::Accepted};
    auto client = snapshot();
    std::vector<std::pair<std::string, std::string>> cursors;
    cursors.reserve(keys.size());
    for (const auto& key : keys) cursors.emplace_back(key, "$" );
    using Fields = std::unordered_map<std::string, std::string>;
    using Entries = std::vector<std::pair<std::string, Fields>>;
    std::unordered_map<std::string, Entries> ignored;
    try {
      auto out = std::inserter(ignored, ignored.end());
      if (client) client->xread(cursors.begin(), cursors.end(), 1, out);
      else return {};
      return {ReadStatus::Accepted};
    } catch (const swr::ReplyError& error) {
      const std::string message = error.what();
      const bool auth = message.rfind("WRONGPASS", 0) == 0 || message.rfind("NOAUTH", 0) == 0;
      return {auth ? ReadStatus::Unavailable : ReadStatus::Rejected,
              message.rfind("WRONGTYPE", 0) == 0, message};
    } catch (const swr::Error& error) { return {ReadStatus::Unavailable, false, error.what()}; }
  }

  // Bounded pipeline depth is chosen by the scheduler. FULL COUNT 1 transfers
  // one retained payload rather than both first/last payloads.
  std::vector<StreamBounds> streamBoundsBatch(const std::vector<std::string>& keys) {
    std::vector<StreamBounds> result(keys.size());
    auto client = snapshot();
    if (!client) return result;
    try {
      auto pipeline = client->pipeline(false);
      for (const auto& key : keys) pipeline.command("XINFO", "STREAM", key, "FULL", "COUNT", 1);
      auto replies = pipeline.exec();
      for (size_t index = 0; index < keys.size(); ++index) {
        try { result[index] = parseBounds(replies.get(index)); }
        catch (const swr::ReplyError& error) { result[index] = rejectedBounds(error.what()); }
      }
    } catch (const swr::Error& error) {
      for (auto& item : result) { item.status = CommandStatus::Unavailable; item.error = error.what(); }
    }
    return result;
  }

  StreamBounds streamBounds(const std::string& key) {
    return streamBoundsBatch({key}).front();
  }

  template<typename Input>
  std::string xadd(const std::string& key, const std::string& id, Input fst, Input lst) {
    return xaddResult(key, id, fst, lst).id;
  }

  // Writes are never automatically replayed here after a lost reply.
  template<typename Input>
  WriteResult xaddResult(const std::string& key, const std::string& id, Input fst, Input lst) {
    auto client = snapshot();
    try {
      if (client) return {CommandStatus::Accepted, client->xadd(key, id, fst, lst)};
    } catch (const swr::ReplyError& error) { return replyFailure(error); }
      catch (const swr::Error& error) { return {CommandStatus::Unavailable, {}, error.what()}; }
    return {};
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xtrim : trim older elements from a stream
  //
  //    key    : the stream to add an element to
  //    thr    : the threshold (number of elements) to trim stream to (zero works but is silly)
  //    apx    : true if thr can be approximate (>=), false if thr should be exact
  //    return : the number of trimmed elements if successful
  //             -1 if unsuccsessful or not connected
  //
  TrimResult xtrimResult(const std::string& key, uint32_t threshold, bool approximate = true) {
    auto client = snapshot();
    try {
      if (client) return {CommandStatus::Accepted, client->xtrim(key, threshold, approximate)};
    } catch (const swr::ReplyError& error) {
      const auto rejected = replyFailure(error);
      return {rejected.status, -1, rejected.error, rejected.refreshConnection};
    } catch (const swr::Error& error) { return {CommandStatus::Unavailable, -1, error.what()}; }
    return {};
  }

  int32_t xtrim(const std::string& key, uint32_t threshold, bool approximate = true,
                CommandStatus* status = nullptr) {
    const auto result = xtrimResult(key, threshold, approximate);
    if (status) *status = result.status;
    return static_cast<int32_t>(result.count);
  }
  // Exact pointer overload prevents a status pointer converting to bool.
  int32_t xtrim(const std::string& key, uint32_t threshold, CommandStatus* status) {
    return xtrim(key, threshold, true, status);
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xaddTrim : add an element to the specified stream and trim older elements
  //
  //    key    : the stream to add an element to
  //    id     : the id for the element, must exceed current highest id, "*" means server will generate
  //    fst    : the first element of map<string, string> of: field -> value
  //    fst    : the last element of map<string, string> of: field -> value
  //    thr    : the threshold (number of elements) to trim stream to (zero works but is silly)
  //    apx    : true if thr can be approximate (>=), false if thr should be exact
  //    return : the id of the new element if successful
  //             empty string if unsuccsessful or not connected
  //
  template<typename Input>
  std::string xaddTrim(const std::string& key, const std::string& id,
                       Input fst, Input lst, uint32_t threshold, bool approximate = true) {
    return xaddTrimResult(key, id, fst, lst, threshold, approximate).id;
  }

  template<typename Input>
  WriteResult xaddTrimResult(const std::string& key, const std::string& id,
                            Input fst, Input lst, uint32_t threshold, bool approximate = true) {
    auto client = snapshot();
    try {
      if (client) return {CommandStatus::Accepted, client->xadd(key, id, fst, lst, threshold, approximate)};
    } catch (const swr::ReplyError& error) { return replyFailure(error); }
      catch (const swr::Error& error) { return {CommandStatus::Unavailable, {}, error.what()}; }
    return {};
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  exists : test if a key exists
  //
  //    key    : the key to look for
  //    return : 1 if key exists
  //             0 if key does not exist
  //            -1 if unsuccsessful or not connected
  //
  int32_t exists(const std::string& key)
  {
    auto client = snapshot();
    try
    {
      if (client) return client->exists(key);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  copy : copy a key to another key
  //
  //    src    : the key to copy from
  //    dst    : the key to copy to
  //    return : 1 if key copied
  //             0 if key not copied
  //            -1 if error or not connected
  //
  int32_t copy(const std::string& src, const std::string& dst)
  {
    auto client = snapshot();
    try
    {
      if (client) return client->command<long long>("copy", src, dst);
    }
    catch (const swr::Error& e)
    {
      syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what());
    }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  rename : rename a key to another key
  //
  //    src    : the key to rename from
  //    dst    : the key to rename to
  //    return : true if connected
  //             false if not connected
  //
  bool rename(const std::string& src, const std::string& dst)
  {
    auto client = snapshot();
    try
    {
      if (client) { client->rename(src, dst); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  time : gets current server time
  //
  //    return : vector<string> = { seconds, microseconds }
  //
  std::vector<std::string> time(const std::string& key = "time")
  {
    (void)key;  // Retained for source compatibility.
    auto client = snapshot();
    std::vector<std::string> ret;
    try
    {
      if (client) client->command("time", std::back_inserter(ret));
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return ret;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  hexists : test if a hashmap field exists
  //
  //    key    : the hashmap to look in
  //    fld    : the field to look for
  //    return : 1 if field exists
  //             0 if field does not exist
  //            -1 if unsuccsessful or not connected
  //
  int32_t hexists(const std::string& key, const std::string& fld)
  {
    auto client = snapshot();
    try
    {
      if (client) return client->hexists(key, fld);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  hset : sets a field,value pair in the hashmap
  //
  //    key    : the hashmap to add a field,value pair to
  //    fld    : the field for the pair to be added
  //    val    : the value for the pair to be added
  //    return : true if field added
  //             false if unsuccsessful or not connected
  //
  bool hset(const std::string& key, const std::string& fld, const std::string& val)
  {
    auto client = snapshot();
    try
    {
      if (client) return client->hset(key, fld, val) >= 0;
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  hexpire : set the expiration of a field in a hashmap
  //
  //    key    : the hashmap to set field expiration in
  //    fld    : the field to set expiration on
  //    sec    : the expiration interval in seconds
  //    return : 2 if expiration time set (expired)
  //             1 if expiration time set (unexpired)
  //             0 if option not met (N/A)
  //            -1 if unsuccsessful or not connected
  //            -2 if key or field does not exist
  //            -3 if hexpire not supported
  //
  int32_t hexpire(const std::string& key, const std::string& fld, uint32_t sec)
  {
    auto client = snapshot();
    std::vector<long long> ret;
    try
    {
      if (client) client->command("hexpire", key, std::to_string(sec), "fields", "1", fld, std::back_inserter(ret));
    }
    catch (const swr::Error& e)
    {
      if (std::string(e.what()).find("unknown command") != std::string::npos)
      {
        static bool squelch = false;
        if ( ! squelch)
        {
          syslog(LOG_NOTICE, "RedisConnection::%s %s", __func__,
            "HEXPIRE requires redis-server 7.4.0 or higher - upgrade to support redis-adapter watchdog");
          squelch = true;
        }
        return -3;
      }
      else { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    }
    return ret.size() ? ret.front() : -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  hkeys : get all the field names in a hashmap
  //
  //    key    : the hashmap key
  //    return : list of field names for the hashmap
  //
   std::vector<std::string> hkeys(const std::string& key)
  {
    auto client = snapshot();
    std::vector<std::string> ret;
    try
    {
      if (client) client->hkeys(key, std::back_inserter(ret));
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return ret;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  subscriber : get a Subscriber object for pub/sub
  //
  //    return : pointer to a Subscriber if successful
  //             0 if unsuccessful or not connected
  //
  //    note - previously this function returned std::optional<swr::Subscriber> but
  //           std::optional has been replaced with swr::Optional to support c++14
  //           and unfortunately swr::Optional cannot create an empty optional with
  //           swr::Subscriber - now client must delete new swr::Subscriber when done
  //
  swr::Subscriber* subscriber()
  {
    auto client = snapshot();
    try
    {
      if (client) { return new swr::Subscriber(client->subscriber()); }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return 0;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  publish : publish a message to a pub/sub channel
  //
  //    chn    : the channel to publish to
  //    msg    : the message to publish
  //    return : >= 0 the number of subscribers notified
  //             -1 if not connected
  //
  int32_t publish(const std::string& chn, const std::string& msg)
  {
    auto client = snapshot();
    try
    {
      if (client) return client->publish(chn, msg);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

private:
  enum class InitialServer { Unchecked, Accepted, Unsupported };

  // Probe only the first reachable server. This separate lock serializes
  // concurrent initial connects without holding the client snapshot lock
  // during network I/O. Transport failures leave the probe pending.
  bool acceptStandalone(const std::shared_ptr<swr::Redis>& client) {
    std::lock_guard<std::mutex> lock(_probe_mtx);
    if (_initial_server != InitialServer::Unchecked)
      return _initial_server == InitialServer::Accepted;
    try {
      client->command<std::string>("CLUSTER", "INFO");
      _initial_server = InitialServer::Unsupported;
      syslog(LOG_ERR, "RedisConnection does not support Redis Cluster; use a standalone Redis server");
      return false;
    } catch (const swr::ReplyError& error) {
      const std::string message = error.what();
      // Standalone Redis rejects CLUSTER INFO. Restricted users may receive
      // NOPERM instead; that is inconclusive and must not reject the server.
      if (message.find("cluster support disabled") == std::string::npos)
        syslog(LOG_WARNING, "RedisConnection cannot determine server mode; proceeding with standalone commands: %s", error.what());
      _initial_server = InitialServer::Accepted;
      return true;
    } catch (const swr::Error& error) {
      syslog(LOG_WARNING, "RedisConnection initial server mode probe failed: %s", error.what());
      return false;
    }
  }

  static StreamBounds rejectedBounds(const std::string& message) {
    if (message.rfind("ERR no such key", 0) == 0) return {CommandStatus::Accepted, StreamKind::Missing};
    if (message.rfind("WRONGTYPE", 0) == 0) return {CommandStatus::Accepted, StreamKind::Invalid};
    return {CommandStatus::Rejected, StreamKind::Unknown, "0-0", "0-0", message};
  }
  static StreamBounds parseBounds(const redisReply& reply) {
    StreamBounds result;
    result.status = CommandStatus::Rejected;
    if (reply.type != REDIS_REPLY_ARRAY || reply.elements % 2) return result;
    const auto text = [](const redisReply* value) {
      return value && value->type == REDIS_REPLY_STRING ? std::string(value->str, value->len) : std::string{};
    };
    bool hasLast = false;
    for (size_t index = 0; index < reply.elements; index += 2) {
      const auto field = text(reply.element[index]);
      const auto* value = reply.element[index + 1];
      if (field == "last-generated-id") { result.lastGeneratedId = text(value); hasLast = !result.lastGeneratedId.empty(); }
      else if (field == "entries" && value && value->type == REDIS_REPLY_ARRAY && value->elements) {
        const auto* first = value->element[0];
        if (first && first->type == REDIS_REPLY_ARRAY && first->elements) result.firstId = text(first->element[0]);
      }
    }
    if (hasLast) { result.status = CommandStatus::Accepted; result.kind = StreamKind::Stream; }
    return result;
  }
  static WriteResult replyFailure(const swr::ReplyError& error) {
    const std::string message = error.what();
    const bool refresh = message == "READONLY" || message.rfind("READONLY ", 0) == 0;
    syslog(LOG_WARNING, "Redis stream command rejected: %s", message.c_str());
    // READONLY is a known refusal, with topology refresh for later operations.
    return {CommandStatus::Rejected, {}, message, refresh};
  }
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  snapshot : copy the selected client shared_ptr under a brief lock
  //
  //  The lock only protects the refcount copy, never a Redis call. This lets
  //  connect() replace clients without starving callers and keeps the selected
  //  client alive for the full call even during concurrent replacement.
  //
  std::shared_ptr<swr::Redis> snapshot(bool blocking = false)
  {
    std::lock_guard<std::mutex> lk(_mtx);
    return blocking ? _reader : _client;
  }

  std::mutex _mtx;
  std::mutex _probe_mtx;
  InitialServer _initial_server = InitialServer::Unchecked;
  std::shared_ptr<swr::Redis> _client;
  std::shared_ptr<swr::Redis> _reader;
  const uint16_t _reader_pool_size;
};
